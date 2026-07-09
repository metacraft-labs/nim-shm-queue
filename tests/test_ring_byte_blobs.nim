## Layer-1 (`shm_queue/ring`) coordination corner cases (spec §3.1).
##
## The ring is exercised as a pure byte-blob MPSC coordination device: blob
## roundtrip at every boundary size, single-producer FIFO, wraparound past
## 2*cap, ring-full drop signalling (never overwriting an unread slot),
## multi-PROCESS producers (fork), multi-THREAD producers, boot-guard rejection,
## version-mismatch attach, torn-write skipping, capacity edges, and detach /
## re-attach mid-stream.

import std/[os, posix, sets, unittest]
import shm_queue/[ring, segment]

proc cExit(code: cint) {.importc: "_exit", header: "<unistd.h>", noreturn.}
proc quitChild(code: cint) {.noreturn.} =
  ## Terminate a forked child WITHOUT running Nim/atexit teardown (which could
  ## flush shared state or run destructors the parent still relies on).
  cExit(code)

# --- helpers ---------------------------------------------------------------

var tmpCounter = 0
proc freshPath(tag: string): string =
  inc tmpCounter
  getTempDir() / ("shmq-test-" & tag & "-" & $getpid() & "-" & $tmpCounter & ".seg")

proc cleanup(path: string) =
  try: removeFile(path)
  except CatchableError: discard

proc blobOf(n: int; seed: byte = 0): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte((i + int(seed)) and 0xFF)

proc drainInto(r: var ShmRing; buf: var seq[byte]): (DrainResult, int) =
  var outLen = 0
  let dr = r.tryDrainOne(buf, outLen)
  (dr, outLen)

const TestBoot = 0xABCDEF01'u64

# --- blob roundtrip at boundary sizes --------------------------------------

suite "blob roundtrip at boundary sizes":
  test "sizes 0, 1, maxBlobLen, maxBlobLen+1 (oversize)":
    let path = freshPath("roundtrip")
    defer: cleanup(path)
    const maxBlob = 64
    var r = createRing(path, 8, maxBlob, TestBoot)
    check r.isValid
    check r.capacity == 8
    check r.maxBlobLen == maxBlob

    var outBuf = newSeq[byte](maxBlob)

    for sz in [0, 1, maxBlob]:
      let b = blobOf(sz, byte(sz))
      check r.tryPush(b) == prPushed
      let (dr, ol) = drainInto(r, outBuf)
      check dr == drGot
      check ol == sz
      check outBuf[0 ..< sz] == b

    # maxBlobLen+1 -> prOversize, ring unchanged.
    let big = blobOf(maxBlob + 1)
    check r.tryPush(big) == prOversize
    check r.pendingCount() == 0
    let (dr2, _) = drainInto(r, outBuf)
    check dr2 == drEmpty
    r.detach()

  test "drain into an undersized buffer -> drOverflowBuf, slot intact":
    let path = freshPath("overflowbuf")
    defer: cleanup(path)
    var r = createRing(path, 4, 32, TestBoot)
    let b = blobOf(20, 7)
    check r.tryPush(b) == prPushed
    var small = newSeq[byte](10)
    var outLen = 0
    check r.tryDrainOne(small, outLen) == drOverflowBuf
    check r.pendingCount() == 1 # slot untouched
    # A big-enough buffer still retrieves it.
    var big = newSeq[byte](32)
    let (dr, ol) = drainInto(r, big)
    check dr == drGot
    check ol == 20
    check big[0 ..< 20] == b
    r.detach()

# --- single-producer FIFO + wraparound -------------------------------------

suite "single-producer FIFO + wraparound":
  test "FIFO order preserved for a single producer":
    let path = freshPath("fifo")
    defer: cleanup(path)
    var r = createRing(path, 8, 16, TestBoot)
    var outBuf = newSeq[byte](16)
    # push 5, drain 5, verify order
    for i in 0 ..< 5:
      check r.tryPush(@[byte(i), byte(i*2)]) == prPushed
    for i in 0 ..< 5:
      let (dr, ol) = drainInto(r, outBuf)
      check dr == drGot
      check ol == 2
      check outBuf[0] == byte(i)
      check outBuf[1] == byte(i*2)
    r.detach()

  test "wraparound past 2*cap (interleaved push/drain)":
    let path = freshPath("wrap")
    defer: cleanup(path)
    const cap = 8
    var r = createRing(path, cap, 16, TestBoot)
    var outBuf = newSeq[byte](16)
    let total = 2 * cap + 5 # 21 items, forces >2 full wraps of the slot array
    var pushed = 0
    var drained = 0
    while drained < total:
      # keep at most cap-1 outstanding so we never hit full here
      if pushed < total and (pushed - drained) < cap - 1:
        var blob = newSeq[byte](4)
        for k in 0 ..< 4: blob[k] = byte((pushed + k) and 0xFF)
        check r.tryPush(blob) == prPushed
        inc pushed
      else:
        let (dr, ol) = drainInto(r, outBuf)
        check dr == drGot
        check ol == 4
        for k in 0 ..< 4:
          check outBuf[k] == byte((drained + k) and 0xFF)
        inc drained
    check r.droppedCount() == 0
    r.detach()

# --- ring-full -> dropped, unread slots intact -----------------------------

suite "ring-full drop signalling":
  test "full ring bumps dropped, returns prDropped, unread slots intact":
    let path = freshPath("full")
    defer: cleanup(path)
    const cap = 4
    var r = createRing(path, cap, 16, TestBoot)
    # Fill exactly cap slots without draining.
    for i in 0 ..< cap:
      check r.tryPush(@[byte(i)]) == prPushed
    check r.pendingCount() == uint64(cap)
    # Next pushes must drop, never overwrite.
    for j in 0 ..< 3:
      check r.tryPush(@[byte(200 + j)]) == prDropped
    check r.droppedCount() == 3
    check r.pendingCount() == uint64(cap)
    # The cap unread slots are the ORIGINAL blobs, in order (not overwritten).
    var outBuf = newSeq[byte](16)
    for i in 0 ..< cap:
      let (dr, ol) = drainInto(r, outBuf)
      check dr == drGot
      check ol == 1
      check outBuf[0] == byte(i)
    check r.pendingCount() == 0
    # Dropped count is sticky.
    check r.droppedCount() == 3
    r.detach()

  test "cap=1 works":
    let path = freshPath("cap1")
    defer: cleanup(path)
    var r = createRing(path, 1, 8, TestBoot)
    check r.isValid
    check r.tryPush(@[byte 42]) == prPushed
    check r.tryPush(@[byte 43]) == prDropped # full at 1
    check r.droppedCount() == 1
    var outBuf = newSeq[byte](8)
    let (dr, ol) = drainInto(r, outBuf)
    check dr == drGot
    check ol == 1
    check outBuf[0] == 42
    # now empty again, next push fits
    check r.tryPush(@[byte 44]) == prPushed
    let (dr2, _) = drainInto(r, outBuf)
    check dr2 == drGot
    check outBuf[0] == 44
    r.detach()

# --- capacity edges: non-power-of-two rejected -----------------------------

suite "capacity edges":
  test "non-power-of-two capacity rejected at create":
    for badCap in [0, 3, 5, 6, 7, 9, 100, 1000]:
      let path = freshPath("badcap-" & $badCap)
      defer: cleanup(path)
      var r = createRing(path, badCap, 16, TestBoot)
      check (not r.isValid)
      check (not fileExists(path)) # nothing was created

  test "power-of-two capacities accepted":
    for goodCap in [1, 2, 4, 8, 16, 1024]:
      let path = freshPath("goodcap-" & $goodCap)
      defer: cleanup(path)
      var r = createRing(path, goodCap, 16, TestBoot)
      check r.isValid
      check r.capacity == goodCap
      r.detach()

# --- boot-guard ------------------------------------------------------------

suite "boot-guard":
  test "a segment stamped with a different bootId is rejected on attach":
    let path = freshPath("bootguard")
    defer: cleanup(path)
    # Create stamped with an OLD boot id.
    var creator = createRing(path, 8, 16, 0x1111_1111'u64)
    check creator.isValid
    check creator.tryPush(@[byte 1, 2, 3]) == prPushed
    creator.detach() # segment file remains on disk

    # attachRing uses the CURRENT bootId(); a mismatch must be rejected (the
    # real bootId() will not equal our synthetic old stamp).
    var attached = attachRing(path)
    check (not attached.isValid) # stale-boot segment not trusted

    # But attaching a segment stamped with the current boot succeeds.
    let path2 = freshPath("bootguard-cur")
    defer: cleanup(path2)
    var cur = createRing(path2, 8, 16, bootId())
    check cur.isValid
    check cur.tryPush(@[byte 9]) == prPushed
    cur.detach()
    var att2 = attachRing(path2)
    check att2.isValid
    var outBuf = newSeq[byte](16)
    let (dr, ol) = drainInto(att2, outBuf)
    check dr == drGot
    check ol == 1
    check outBuf[0] == 9
    att2.detach()

# --- version-mismatch attach fails loudly ----------------------------------

suite "version mismatch":
  test "a segment with a wrong formatVersion fails attach":
    let path = freshPath("vermismatch")
    defer: cleanup(path)
    var r = createRing(path, 8, 16, bootId())
    check r.isValid
    r.detach()
    # Corrupt the on-disk formatVersion field, then attach must reject it.
    let fd = open(path.cstring, O_RDWR)
    check fd >= 0
    # HdrOffFormatVersion is the u32 right after the u64 magic.
    var bad: uint32 = ShmRingFormatVersion + 99
    check lseek(fd, Off(HdrOffFormatVersion), SEEK_SET) == Off(HdrOffFormatVersion)
    check write(fd, addr bad, 4) == 4
    check close(fd) == 0
    var attached = attachRing(path)
    check (not attached.isValid)

  test "a segment with a wrong magic fails attach":
    let path = freshPath("magicmismatch")
    defer: cleanup(path)
    var r = createRing(path, 8, 16, bootId())
    check r.isValid
    r.detach()
    let fd = open(path.cstring, O_RDWR)
    check fd >= 0
    var bad: uint64 = 0xDEADBEEF'u64
    check lseek(fd, Off(HdrOffMagic), SEEK_SET) == Off(HdrOffMagic)
    check write(fd, addr bad, 8) == 8
    check close(fd) == 0
    check (not attachRing(path).isValid)

# --- torn-write tolerance --------------------------------------------------

suite "torn-write tolerance":
  test "a reserved-but-unpublished slot is skipped, not read as garbage":
    let path = freshPath("torn")
    defer: cleanup(path)
    const cap = 8
    var r = createRing(path, cap, 16, TestBoot)
    # Simulate a producer that reserved ticket 0 (bumped tail) but has NOT yet
    # release-stored ready. We do this by directly manipulating the header: push
    # nothing, but bump tail so head(0) < tail(1) while slot 0 stays ready==0.
    let base = r.seg.base
    # Manually reserve one ticket (as a stalled producer would after its CAS).
    storeU64Release(base, HdrOffTail, 1)
    # Consumer: head=0 < tail=1, but slot0.ready==0 (!=1) -> must return drEmpty.
    var outBuf = newSeq[byte](16)
    var outLen = 0
    check r.tryDrainOne(outBuf, outLen) == drEmpty
    check outLen == 0
    check r.pendingCount() == 1 # the reservation is still outstanding

    # Now the "producer" completes: write the blob then publish ready=1.
    let so = SegHeaderSize + 0 * slotStrideFor(r.maxBlobLen)
    let payload = @[byte 5, 6, 7]
    copyMem(addr base[so + SlotOffBlob], unsafeAddr payload[0], payload.len)
    storeU32Release(base, so + SlotOffBlobLen, uint32(payload.len))
    storeU64Release(base, so + SlotOffReady, 1)
    let (dr, ol) = drainInto(r, outBuf)
    check dr == drGot
    check ol == 3
    check outBuf[0] == 5 and outBuf[1] == 6 and outBuf[2] == 7
    r.detach()

# --- detach + re-attach mid-stream -----------------------------------------

suite "detach + re-attach mid-stream":
  test "unread records survive a detach/re-attach":
    let path = freshPath("reattach")
    defer: cleanup(path)
    var r = createRing(path, 8, 16, bootId())
    for i in 0 ..< 5:
      check r.tryPush(@[byte(i), byte(100 + i)]) == prPushed
    var outBuf = newSeq[byte](16)
    # Drain 2, leaving 3 unread.
    for i in 0 ..< 2:
      let (dr, ol) = drainInto(r, outBuf)
      check dr == drGot
      check outBuf[0] == byte(i)
    check r.pendingCount() == 3
    r.detach()

    # Re-attach: the 3 unread records must still be there, in order.
    var r2 = attachRing(path)
    check r2.isValid
    check r2.pendingCount() == 3
    for i in 2 ..< 5:
      let (dr, ol) = drainInto(r2, outBuf)
      check dr == drGot
      check ol == 2
      check outBuf[0] == byte(i)
      check outBuf[1] == byte(100 + i)
    check r2.pendingCount() == 0
    r2.detach()

# --- multi-THREAD producers ------------------------------------------------

suite "multi-thread producers":
  test "N threads each push M distinct blobs, consumer drains all":
    let path = freshPath("threads")
    defer: cleanup(path)
    const nThreads = 4
    const perThread = 2000
    const cap = 1024
    # Big enough ring + active draining so we never force a full-ring drop.
    var r = createRing(path, cap, 16, bootId())
    check r.isValid

    type ProdArg = object
      path: string
      tid: int
    var threads: array[nThreads, Thread[ProdArg]]

    proc producer(arg: ProdArg) {.thread.} =
      var pr = attachRing(arg.path)
      doAssert pr.isValid
      var i = 0
      while i < perThread:
        # encode (tid, i) as an 8-byte blob
        var blob = newSeq[byte](8)
        blob[0] = byte(arg.tid)
        blob[1] = byte(i and 0xFF)
        blob[2] = byte((i shr 8) and 0xFF)
        let res = pr.tryPush(blob)
        if res == prPushed:
          inc i
        # on prDropped, retry the same i (consumer will catch up)
      pr.detach()

    for t in 0 ..< nThreads:
      createThread(threads[t], producer, ProdArg(path: path, tid: t))

    # Consumer drains until it has collected nThreads*perThread unique items.
    var seen = initHashSet[(int, int)]()
    var outBuf = newSeq[byte](16)
    let target = nThreads * perThread
    while seen.len < target:
      var outLen = 0
      if r.tryDrainOne(outBuf, outLen) == drGot:
        check outLen == 8
        let tid = int(outBuf[0])
        let idx = int(outBuf[1]) or (int(outBuf[2]) shl 8)
        check (not seen.contains((tid, idx))) # no duplicates
        seen.incl((tid, idx))
    for t in 0 ..< nThreads:
      joinThread(threads[t])
    # NO LOSS: every distinct (tid, idx) was delivered exactly once. Producers
    # retry on a transient full ring, so a nonzero dropped counter is fine — the
    # invariant that matters is that all target items arrive with no duplicates.
    check seen.len == target
    r.detach()

# --- multi-PROCESS producers (fork) ----------------------------------------

suite "multi-process producers (fork)":
  test "fork N children each pushing M blobs; parent drains exactly N*M":
    let path = freshPath("procs")
    defer: cleanup(path)
    const nProc = 4
    const perProc = 3000
    const cap = 1024
    var r = createRing(path, cap, 16, bootId())
    check r.isValid

    var pids: seq[Pid]
    for c in 0 ..< nProc:
      let pid = fork()
      if pid == 0:
        # CHILD: attach the shared segment and push perProc distinct blobs.
        var cr = attachRing(path)
        if not cr.isValid:
          quitChild(2)
        var i = 0
        while i < perProc:
          var blob = newSeq[byte](8)
          blob[0] = byte(c)
          blob[1] = byte(i and 0xFF)
          blob[2] = byte((i shr 8) and 0xFF)
          blob[3] = byte((i shr 16) and 0xFF)
          if cr.tryPush(blob) == prPushed:
            inc i
        cr.detach()
        quitChild(0)
      else:
        check pid > 0
        pids.add(pid)

    # PARENT consumer: drain exactly nProc*perProc unique records.
    var seen = initHashSet[(int, int)]()
    var outBuf = newSeq[byte](16)
    let target = nProc * perProc
    while seen.len < target:
      var outLen = 0
      if r.tryDrainOne(outBuf, outLen) == drGot:
        check outLen == 8
        let c = int(outBuf[0])
        let idx = int(outBuf[1]) or (int(outBuf[2]) shl 8) or (int(outBuf[3]) shl 16)
        check (not seen.contains((c, idx)))
        seen.incl((c, idx))

    for pid in pids:
      var status: cint
      discard waitpid(pid, status, 0)
    # NO LOSS across process boundaries: exactly nProc*perProc distinct records
    # crossed the shared mmap segment, each seen once. Children retry on a
    # transient full ring, so every attempted record is eventually delivered.
    check seen.len == target
    r.detach()

  test "forced-full: dropped-count is exact (no draining until all children exit)":
    let path = freshPath("procs-full")
    defer: cleanup(path)
    const nProc = 3
    const perProc = 500
    const cap = 8 # tiny ring; children will overflow it
    var r = createRing(path, cap, 16, bootId())
    check r.isValid

    var pids: seq[Pid]
    for c in 0 ..< nProc:
      let pid = fork()
      if pid == 0:
        var cr = attachRing(path)
        if not cr.isValid:
          quitChild(2)
        # Each child attempts EXACTLY perProc pushes (no retry). Some are
        # pushed, some dropped; the ring must account for every attempt.
        for i in 0 ..< perProc:
          discard cr.tryPush(@[byte(c), byte(i and 0xFF)])
        cr.detach()
        quitChild(0)
      else:
        check pid > 0
        pids.add(pid)

    for pid in pids:
      var status: cint
      discard waitpid(pid, status, 0)

    # All children have exited. Now drain everything the ring holds.
    var drainedCount = 0
    var outBuf = newSeq[byte](16)
    while true:
      var outLen = 0
      if r.tryDrainOne(outBuf, outLen) == drGot:
        inc drainedCount
      else:
        break
    let totalAttempts = nProc * perProc
    # INVARIANT: every attempt is either pushed(+drained) or dropped, exactly.
    check drainedCount.uint64 + r.droppedCount() == uint64(totalAttempts)
    # And the ring is bounded: we can never have drained more than attempts.
    check drainedCount <= totalAttempts
    r.detach()
