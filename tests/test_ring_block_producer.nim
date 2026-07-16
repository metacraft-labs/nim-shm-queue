## Layer-1 `opBlockProducer` overflow policy (io-mon-Lossless-Event-Capture §4.1).
##
## The block-on-full ring must:
##   * behave identically to the drop ring on the uncontended path;
##   * WAIT for a slot rather than drop when full while the consumer is alive
##     (LF-1: zero loss even under a tiny ring with many concurrent producers);
##   * fail fast with `prConsumerGone` — never hang — when the consumer is
##     dead/absent (LF-4).
##
## The default `ShmRing` (bare, no policy) stays `opDropSignalled`; that is
## covered by `test_ring_byte_blobs`. Here we prove the new policy.

import std/[os, posix, sets, times, unittest]
import shm_queue/[ring, segment]

proc cExit(code: cint) {.importc: "_exit", header: "<unistd.h>", noreturn.}
proc quitChild(code: cint) {.noreturn.} = cExit(code)

var tmpCounter = 0
proc freshPath(tag: string): string =
  inc tmpCounter
  getTempDir() / ("shmq-block-" & tag & "-" & $getpid() & "-" & $tmpCounter & ".seg")

proc cleanup(path: string) =
  try: removeFile(path)
  except CatchableError: discard

const TestBoot = 0xABCDEF01'u64

# --- uncontended parity ----------------------------------------------------

suite "opBlockProducer: uncontended parity with the drop ring":
  test "push/drain roundtrip + FIFO, never drops on a non-full ring":
    let path = freshPath("parity")
    defer: cleanup(path)
    var r = createBlockingRing(path, 8, 32, TestBoot)
    check r.isValid
    r.registerConsumer()
    var outBuf = newSeq[byte](32)
    for i in 0 ..< 5:
      check r.tryPush(@[byte(i), byte(100 + i)]) == prPushed
    for i in 0 ..< 5:
      var outLen = 0
      check r.tryDrainOne(outBuf, outLen) == drGot
      check outLen == 2
      check outBuf[0] == byte(i)
      check outBuf[1] == byte(100 + i)
    check r.droppedCount() == 0
    r.detach()

# --- LF-4: dead consumer -> prConsumerGone, never a hang -------------------

suite "opBlockProducer: dead consumer fails fast (LF-4)":
  test "a full ring with a dead consumer returns prConsumerGone":
    let path = freshPath("gone")
    defer: cleanup(path)
    const cap = 2
    var r = createBlockingRing(path, cap, 16, TestBoot)
    check r.isValid

    # Forge a consumer-liveness token that points at a definitely-dead pid on
    # the current boot: fork a child, reap it, then claim its pid as consumer.
    let child = fork()
    if child == 0:
      quitChild(0)
    var status: cint
    discard waitpid(child, status, 0)
    let base = r.seg.base
    storeU64Relaxed(base, HdrOffConsumerBoot, bootId())
    storeU64Relaxed(base, HdrOffConsumerPid, uint64(child))
    storeU64Release(base, HdrOffConsumerAlive, 1)

    # Fill the ring, then the overflowing push must NOT hang — it detects the
    # dead consumer and returns prConsumerGone within a bounded time.
    for i in 0 ..< cap:
      check r.tryPush(@[byte(i)]) == prPushed
    let t0 = epochTime()
    check r.tryPush(@[byte 99]) == prConsumerGone
    check (epochTime() - t0) < 5.0 # bounded: no unbounded hang
    check r.droppedCount() == 0    # never a drop
    r.detach()

# --- LF-1: zero loss under backpressure, many processes, tiny ring ---------

suite "opBlockProducer: zero loss under backpressure (LF-1)":
  test "N fork children each push M distinct blobs into a TINY ring; all arrive":
    let path = freshPath("noloss")
    defer: cleanup(path)
    const
      nProc = 4
      perProc = 4000
      cap = 8 # deliberately tiny: producers WILL block and wait for the drain
    var r = createBlockingRing(path, cap, 16, bootId())
    check r.isValid
    r.registerConsumer() # publish liveness so children never see prConsumerGone

    var pids: seq[Pid]
    for c in 0 ..< nProc:
      let pid = fork()
      if pid == 0:
        # CHILD producer: NO retry loop — the block policy itself guarantees the
        # push lands (or the consumer is gone, which is a hard error here).
        var cr = attachBlockingRing(path)
        if not cr.isValid:
          quitChild(2)
        for i in 0 ..< perProc:
          var blob = newSeq[byte](8)
          blob[0] = byte(c)
          blob[1] = byte(i and 0xFF)
          blob[2] = byte((i shr 8) and 0xFF)
          blob[3] = byte((i shr 16) and 0xFF)
          if cr.tryPush(blob) != prPushed:
            cr.detach(); quitChild(3) # any drop / consumerGone is a failure
        cr.detach()
        quitChild(0)
      else:
        check pid > 0
        pids.add(pid)

    # PARENT consumer: drain exactly nProc*perProc distinct records. Its draining
    # is what frees slots and wakes the blocked producers.
    var seen = initHashSet[(int, int)]()
    var outBuf = newSeq[byte](16)
    let target = nProc * perProc
    while seen.len < target:
      var outLen = 0
      if r.tryDrainOne(outBuf, outLen) == drGot:
        check outLen == 8
        let c = int(outBuf[0])
        let idx = int(outBuf[1]) or (int(outBuf[2]) shl 8) or (int(outBuf[3]) shl 16)
        check (not seen.contains((c, idx))) # no duplicates / no phantoms
        seen.incl((c, idx))

    for pid in pids:
      var st: cint
      let w = waitpid(pid, st, 0)
      check w == pid
      check WIFEXITED(st)
      check WEXITSTATUS(st) == 0 # every child pushed all records with no drop

    # LF-1: exactly target distinct records crossed the tiny ring, none lost,
    # none phantom, and the SIGNALLED drop counter is zero (nothing was dropped).
    check seen.len == target
    check r.droppedCount() == 0
    r.markConsumerGone()
    r.detach()
