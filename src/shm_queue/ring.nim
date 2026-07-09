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
  segment.shmSegmentSupported, segment.embeddedRingSize,
  segment.embeddedRingHeaderSize

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

  # --- core coordination over (base, ringBase, cap, maxBlobLen) -------------
  #
  # The ticket-CAS reservation / release-store publish / single-consumer drain
  # protocol, parameterised by the mapped `base`, the byte offset of the ring
  # header (`ringBase`), and the ring geometry. Both the segment-owning
  # `ShmRing` (ringBase = 0, geometry from the segment header) and the
  # `EmbeddedRing` (ring header embedded at `ringBase` inside a caller-owned
  # region — e.g. reprobuild's action-cache control region) delegate here, so
  # exactly ONE copy of the coordination code exists.

  func slotOffAt(ringBase, cap, maxBlobLen: int; ticket: uint64): int {.inline.} =
    ringBase + ringSlotsBaseOffset() +
      int(ticket mod uint64(cap)) * slotStrideFor(maxBlobLen)

  proc pushBlob(base: ShmBase; ringBase, cap, maxBlobLen: int;
      blob: openArray[byte]): PushResult =
    if blob.len > maxBlobLen:
      return prOversize
    var tail = loadU64Acquire(base, ringBase + RingOffTail)
    while true:
      let head = loadU64Acquire(base, ringBase + RingOffHead)
      if tail - head >= uint64(cap):
        # Ring full. Re-check tail once (it may have advanced under us) before
        # committing to a drop, then signal the drop via the atomic counter.
        let tailNow = loadU64Acquire(base, ringBase + RingOffTail)
        if tailNow != tail:
          tail = tailNow
          continue
        discard fetchAddU64(base, ringBase + RingOffDropped, 1)
        return prDropped
      # Reserve `tail` by advancing it to tail+1. On CAS failure `tail` is
      # refreshed with the observed value and we retry.
      if casU64(base, ringBase + RingOffTail, tail, tail + 1):
        break
    # We own ticket `tail`. Its slot is free: the consumer cleared `ready` for
    # the previous occupant (ticket tail-cap) before advancing head past it.
    let so = slotOffAt(ringBase, cap, maxBlobLen, tail)
    if blob.len > 0:
      copyMem(addr base[so + SlotOffBlob], unsafeAddr blob[0], blob.len)
    storeU32Release(base, so + SlotOffBlobLen, uint32(blob.len))
    # Publish: ready = ticket + 1 (never 0). Release-ordered so a consumer that
    # observes `ready` also observes the length + blob writes above.
    storeU64Release(base, so + SlotOffReady, tail + 1)
    prPushed

  proc drainBlob(base: ShmBase; ringBase, cap, maxBlobLen: int;
      outBuf: var openArray[byte]; outLen: var int): DrainResult =
    outLen = 0
    let head = loadU64Relaxed(base, ringBase + RingOffHead) # consumer-owned
    let tail = loadU64Acquire(base, ringBase + RingOffTail)
    if head >= tail:
      return drEmpty # nothing reserved past head
    let so = slotOffAt(ringBase, cap, maxBlobLen, head)
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
    storeU64Release(base, ringBase + RingOffHead, head + 1)
    drGot

  # --- segment-owning ShmRing (ringBase = 0) -------------------------------

  proc tryPush*(r: var ShmRing; blob: openArray[byte]): PushResult =
    ## Lock-free multi-producer push. Reserves a ticket by CAS-bumping `tail`,
    ## writes `blob` (u32 length prefix + bytes) into the reserved slot, and
    ## publishes via a release-store of `ready = ticket+1`. Bounded: a full ring
    ## bumps the atomic `dropped` counter and returns `prDropped` (SIGNALLED,
    ## never silent, never overwriting an unread slot). A blob longer than the
    ## segment's `maxBlobLen` returns `prOversize` with the ring unchanged.
    if not r.seg.isValid:
      return prOversize
    pushBlob(r.seg.base, HdrOffHead, r.seg.capacity, r.seg.maxBlobLen, blob)

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
    drainBlob(r.seg.base, HdrOffHead, r.seg.capacity, r.seg.maxBlobLen, outBuf, outLen)

  proc droppedCount*(r: ShmRing): uint64 {.inline.} =
    ## Number of SIGNALLED ring-full drops.
    if not r.seg.isValid: return 0
    loadU64Acquire(r.seg.base, HdrOffDropped)

  proc pendingCount*(r: ShmRing): uint64 {.inline.} =
    ## Reserved-but-not-yet-drained tickets (tail - head). Bounded by capacity.
    if not r.seg.isValid: return 0
    loadU64Acquire(r.seg.base, HdrOffTail) - loadU64Acquire(r.seg.base, HdrOffHead)

  # --- EmbeddedRing: a ring living inside a caller-owned mmap region --------
  #
  # For a consumer that already owns a shared region (its own header, other
  # fields) and wants the SAME MPSC ring as a SUB-STRUCTURE at a fixed byte
  # offset — e.g. reprobuild's action-cache control region, which carries a
  # boot-guarded header, a reader-epoch table AND the submission ring in one
  # `.ctl` mapping. The caller owns the mapping lifecycle + header/boot guard;
  # this view supplies only the ring coordination, using the SAME ring header
  # (head/tail/dropped) + slot layout as the segment-owning ring, so there is
  # one and only one MPSC implementation.

  type
    EmbeddedRing* = object
      base*: ShmBase   ## caller-owned mapped base (nil ⇒ unavailable)
      ringBase*: int   ## byte offset of the ring header within the region
      cap*: int        ## ring capacity (power of two)
      maxBlobLen*: int ## per-slot blob capacity

  proc initEmbeddedRing*(base: ShmBase; ringBase, cap, maxBlobLen: int):
      EmbeddedRing {.inline.} =
    ## Build a view over the ring embedded at `ringBase` in the caller-owned
    ## region `base`. The caller supplies the geometry (there is no segment
    ## header to read it from) and owns the region's create/attach/boot guard.
    EmbeddedRing(base: base, ringBase: ringBase, cap: cap, maxBlobLen: maxBlobLen)

  proc resetEmbeddedRing*(r: EmbeddedRing) =
    ## Zero the ring header (head/tail/dropped) on fresh-region init. Slot
    ## `ready` fields are already zero in a freshly zero-filled region.
    if r.base.isNil: return
    storeU64Relaxed(r.base, r.ringBase + RingOffHead, 0)
    storeU64Relaxed(r.base, r.ringBase + RingOffTail, 0)
    storeU64Relaxed(r.base, r.ringBase + RingOffDropped, 0)

  func isValid*(r: EmbeddedRing): bool {.inline.} = not r.base.isNil
  func capacity*(r: EmbeddedRing): int {.inline.} = r.cap
  func maxBlobLen*(r: EmbeddedRing): int {.inline.} = r.maxBlobLen

  proc tryPush*(r: EmbeddedRing; blob: openArray[byte]): PushResult =
    ## Same multi-producer push as `ShmRing.tryPush`, over the embedded ring.
    if r.base.isNil:
      return prOversize
    pushBlob(r.base, r.ringBase, r.cap, r.maxBlobLen, blob)

  proc tryDrainOne*(r: EmbeddedRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    ## Same single-consumer drain as `ShmRing.tryDrainOne`, over the embedded ring.
    outLen = 0
    if r.base.isNil:
      return drEmpty
    drainBlob(r.base, r.ringBase, r.cap, r.maxBlobLen, outBuf, outLen)

  proc droppedCount*(r: EmbeddedRing): uint64 {.inline.} =
    if r.base.isNil: return 0
    loadU64Acquire(r.base, r.ringBase + RingOffDropped)

  proc pendingCount*(r: EmbeddedRing): uint64 {.inline.} =
    if r.base.isNil: return 0
    loadU64Acquire(r.base, r.ringBase + RingOffTail) -
      loadU64Acquire(r.base, r.ringBase + RingOffHead)

else:
  # Non-POSIX: compiles but every op reports empty/unavailable.
  type
    EmbeddedRing* = object
      base*: ShmBase
      ringBase*: int
      cap*: int
      maxBlobLen*: int

  proc tryPush*(r: var ShmRing; blob: openArray[byte]): PushResult = prOversize
  proc tryDrainOne*(r: var ShmRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    outLen = 0
    drEmpty
  proc droppedCount*(r: ShmRing): uint64 = 0
  proc pendingCount*(r: ShmRing): uint64 = 0

  proc initEmbeddedRing*(base: ShmBase; ringBase, cap, maxBlobLen: int):
      EmbeddedRing =
    EmbeddedRing(base: base, ringBase: ringBase, cap: cap, maxBlobLen: maxBlobLen)
  proc resetEmbeddedRing*(r: EmbeddedRing) = discard
  func isValid*(r: EmbeddedRing): bool = not r.base.isNil
  func capacity*(r: EmbeddedRing): int = r.cap
  func maxBlobLen*(r: EmbeddedRing): int = r.maxBlobLen
  proc tryPush*(r: EmbeddedRing; blob: openArray[byte]): PushResult = prOversize
  proc tryDrainOne*(r: EmbeddedRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    outLen = 0
    drEmpty
  proc droppedCount*(r: EmbeddedRing): uint64 = 0
  proc pendingCount*(r: EmbeddedRing): uint64 = 0
