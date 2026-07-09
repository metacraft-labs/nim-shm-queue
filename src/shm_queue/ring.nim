## Layer 1 — the lock-free multi-producer / single-consumer ring as a
## coordination device over fixed-size BYTE BLOBS. No types, no serialization,
## no domain knowledge: this is the reusable substrate both io-mon's dependency
## queue and reprobuild's action-cache submission ring sit on.
##
## The coordination protocol is ported VERBATIM IN SHAPE from the two proven
## copies:
##   * `reprobuild/libs/repro_shm_index/src/repro_shm_index/ring.nim`
##     (the AC-2 MPSC submission ring) + its `segment.nim` (versioned +
##     boot-guarded mmap segment).
##   * `io-mon/src/io_mon/shm/dep_queue.nim` (io-mon-DEP-SHM). This module is
##     exactly that ring MINUS the MonitorRecord codec — the codec is Layer 2/3.
##
## Reservation protocol (bounded, drop-on-full SIGNALLED):
##   * `tail` is a monotonically increasing u64 ticket counter, CAS-bumped by
##     producers to RESERVE a ticket. `head` is the consumer-owned ticket of the
##     next slot to drain. A ticket maps to a slot via `ticket mod capacity`.
##   * A producer reserves ticket T via CAS(tail: T -> T+1) only if the ring is
##     not full (`T - head < capacity`); on full it bumps the atomic `dropped`
##     counter and returns a drop signal (NEVER silent, NEVER overwriting an
##     unread slot).
##   * The producer writes the blob (u32 length prefix + bytes) into slot
##     `T mod capacity`, then PUBLISHES via a release-store of the slot's
##     `ready` field = T+1. `ready == 0` means never-written; `ready == T+1`
##     means "the blob for ticket T is complete".
##   * The single consumer drains ticket H: it reads slot(H).ready (acquire);
##     if `ready != H+1` the slot is empty or a producer is mid-write, so it
##     returns `drEmpty` (torn/unpublished slots are SKIPPED, never read as
##     garbage). Otherwise it copies the blob out, clears `ready` to 0, and
##     advances `head` to H+1 (release) as its linearization point.
##
## No hot-path heap allocation: `tryPush` copies from a caller-owned
## `openArray[byte]`; `tryDrainOne` copies into a caller-owned buffer. This
## keeps the ring fork/orc-safe on a monitored hot path.

import ./segment
export segment.ShmSegment, segment.isValid, segment.bootId,
  segment.shmSegmentSupported

type
  ShmRing* = object
    ## A view over a byte-blob ring backed by an `ShmSegment`. Copy semantics
    ## follow the segment (an owned mapping); use `detach` to release.
    seg*: ShmSegment

  PushResult* = enum
    prPushed   ## the blob was reserved + published
    prDropped  ## ring full: SIGNALLED drop (the `dropped` counter bumped)
    prOversize ## blob longer than the segment's `maxBlobLen`: ring unchanged

  DrainResult* = enum
    drEmpty       ## nothing to drain (empty ring, or head slot not yet published)
    drGot         ## a blob was drained into the caller's buffer
    drOverflowBuf ## the caller's buffer is smaller than the stored blob

func capacity*(r: ShmRing): int {.inline.} = r.seg.capacity
func maxBlobLen*(r: ShmRing): int {.inline.} = r.seg.maxBlobLen
func isValid*(r: ShmRing): bool {.inline.} = r.seg.isValid

proc createRing*(path: string; cap, maxBlobLen: int; boot: uint64): ShmRing =
  ## Create a fresh, zero-filled ring segment of `cap` slots, each holding a
  ## blob of up to `maxBlobLen` bytes, stamped with `boot`. `cap` MUST be a
  ## power of two (so `ticket mod cap` is a mask) and positive; `maxBlobLen`
  ## MUST be non-negative. Returns an invalid ring on a bad geometry or on any
  ## create failure.
  if cap <= 0 or (cap and (cap - 1)) != 0 or maxBlobLen < 0:
    return ShmRing() # invalid: caller checks isValid
  ShmRing(seg: createSegment(path, cap, maxBlobLen, boot))

proc attachRing*(path: string): ShmRing =
  ## Attach to an existing ring segment. Returns an invalid ring when the file
  ## is missing / wrong-sized / stale (wrong magic / formatVersion /
  ## creatorBootId — the boot guard). The geometry is read from the header.
  ShmRing(seg: attachSegment(path))

proc detach*(r: var ShmRing) =
  detach(r.seg)

when shmSegmentSupported:

  func slotOff(r: ShmRing; ticket: uint64): int {.inline.} =
    SegHeaderSize + int(ticket mod uint64(r.seg.capacity)) *
      slotStrideFor(r.seg.maxBlobLen)

  # --- multi-producer push (CAS tail) --------------------------------------

  proc tryPush*(r: var ShmRing; blob: openArray[byte]): PushResult =
    ## Lock-free multi-producer push. Reserves a ticket by CAS-bumping `tail`,
    ## writes `blob` (u32 length prefix + bytes) into the reserved slot, and
    ## publishes via a release-store of `ready = ticket+1`. Bounded: a full ring
    ## bumps the atomic `dropped` counter and returns `prDropped` (SIGNALLED,
    ## never silent, never overwriting an unread slot). A blob longer than the
    ## segment's `maxBlobLen` returns `prOversize` with the ring unchanged.
    if not r.seg.isValid:
      return prOversize
    if blob.len > r.seg.maxBlobLen:
      return prOversize
    let base = r.seg.base
    let cap = uint64(r.seg.capacity)
    var tail = loadU64Acquire(base, HdrOffTail)
    while true:
      let head = loadU64Acquire(base, HdrOffHead)
      if tail - head >= cap:
        # Ring full. Re-check tail once (it may have advanced under us) before
        # committing to a drop, then signal the drop via the atomic counter.
        let tailNow = loadU64Acquire(base, HdrOffTail)
        if tailNow != tail:
          tail = tailNow
          continue
        discard fetchAddU64(base, HdrOffDropped, 1)
        return prDropped
      # Reserve `tail` by advancing it to tail+1. On CAS failure `tail` is
      # refreshed with the observed value and we retry.
      if casU64(base, HdrOffTail, tail, tail + 1):
        break
    # We own ticket `tail`. Its slot is free: the consumer cleared `ready` for
    # the previous occupant (ticket tail-cap) before advancing head past it.
    let so = slotOff(r, tail)
    if blob.len > 0:
      copyMem(addr base[so + SlotOffBlob], unsafeAddr blob[0], blob.len)
    storeU32Release(base, so + SlotOffBlobLen, uint32(blob.len))
    # Publish: ready = ticket + 1 (never 0). Release-ordered so a consumer that
    # observes `ready` also observes the length + blob writes above.
    storeU64Release(base, so + SlotOffReady, tail + 1)
    prPushed

  # --- single-consumer drain -----------------------------------------------

  proc tryDrainOne*(r: var ShmRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    ## SINGLE-consumer non-blocking drain of the next ready ticket. Returns
    ## `drEmpty` when the head slot is not yet published (empty ring or a
    ## producer mid-write — a torn/unpublished slot is SKIPPED, never read).
    ## Returns `drOverflowBuf` (ring unchanged) when `outBuf` is smaller than
    ## the stored blob. On `drGot`, `outLen` bytes were copied into `outBuf`.
    outLen = 0
    if not r.seg.isValid:
      return drEmpty
    let base = r.seg.base
    let head = loadU64Relaxed(base, HdrOffHead) # consumer-owned
    let tail = loadU64Acquire(base, HdrOffTail)
    if head >= tail:
      return drEmpty # nothing reserved past head
    let so = slotOff(r, head)
    let ready = loadU64Acquire(base, so + SlotOffReady)
    if ready != head + 1:
      return drEmpty # producer still publishing
    # Acquire on `ready` above ordered these reads after the producer's writes.
    let blobLen = int(loadU32Acquire(base, so + SlotOffBlobLen))
    if blobLen > outBuf.len:
      # Consumer buffer too small. Leave the slot intact so the caller can
      # retry with a bigger buffer (the ring is unchanged).
      return drOverflowBuf
    if blobLen > 0:
      copyMem(addr outBuf[0], addr base[so + SlotOffBlob], blobLen)
    outLen = blobLen
    # Retire the slot: clear ready (so ticket head+cap can reuse it) and advance
    # head (release) as the consumer's linearization point.
    storeU64Release(base, so + SlotOffReady, 0)
    storeU64Release(base, HdrOffHead, head + 1)
    drGot

  proc droppedCount*(r: ShmRing): uint64 {.inline.} =
    ## Number of SIGNALLED ring-full drops.
    if not r.seg.isValid: return 0
    loadU64Acquire(r.seg.base, HdrOffDropped)

  proc pendingCount*(r: ShmRing): uint64 {.inline.} =
    ## Reserved-but-not-yet-drained tickets (tail - head). Bounded by capacity.
    if not r.seg.isValid: return 0
    loadU64Acquire(r.seg.base, HdrOffTail) - loadU64Acquire(r.seg.base, HdrOffHead)

else:
  # Non-POSIX: compiles but every op reports empty/unavailable.
  proc tryPush*(r: var ShmRing; blob: openArray[byte]): PushResult = prOversize
  proc tryDrainOne*(r: var ShmRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    outLen = 0
    drEmpty
  proc droppedCount*(r: ShmRing): uint64 = 0
  proc pendingCount*(r: ShmRing): uint64 = 0
