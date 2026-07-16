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
  OverflowPolicy* = enum
    ## Compile-time overflow discipline. Selected per instantiation so each ring
    ## is monomorphized to branch-free code (no runtime policy check on the hot
    ## path). See `Shm-Queue-Library.md` / `io-mon-Lossless-Event-Capture.md` §4.1.
    opDropSignalled ## action cache: on full bump `dropped`, return `prDropped`.
    opBlockProducer ## io-mon deps: on full WAIT for a slot; never drop.

  ShmRing*[policy: static OverflowPolicy = opDropSignalled] = object
    ## A view over a byte-blob ring backed by an `ShmSegment`, parameterised by
    ## its overflow `policy` (default `opDropSignalled`, so a bare `ShmRing` is
    ## the historical drop-on-full ring). Copy semantics follow the segment (an
    ## owned mapping); use `detach` to release.
    seg*: ShmSegment

  PushResult* = enum
    prPushed       ## the blob was reserved + published
    prDropped      ## ring full: SIGNALLED drop (`opDropSignalled` only)
    prOversize     ## blob longer than the segment's `maxBlobLen`: ring unchanged
    prConsumerGone ## `opBlockProducer` only: waited, but the consumer is dead/
                   ## absent (liveness token) — fail fast, never hang (LF-4)

  DrainResult* = enum
    drEmpty       ## nothing to drain (empty ring, or head slot not yet published)
    drGot         ## a blob was drained into the caller's buffer
    drOverflowBuf ## the caller's buffer is smaller than the stored blob

func capacity*(r: ShmRing): int {.inline.} = r.seg.capacity
func maxBlobLen*(r: ShmRing): int {.inline.} = r.seg.maxBlobLen
func isValid*(r: ShmRing): bool {.inline.} = r.seg.isValid

proc createRing*(path: string; cap, maxBlobLen: int;
    boot: uint64): ShmRing[opDropSignalled] =
  ## Create a fresh, zero-filled DROP-ON-FULL ring segment of `cap` slots, each
  ## holding a blob of up to `maxBlobLen` bytes, stamped with `boot`. `cap` MUST
  ## be a power of two (so `ticket mod cap` is a mask) and positive; `maxBlobLen`
  ## MUST be non-negative. Returns an invalid ring on a bad geometry or on any
  ## create failure.
  if cap <= 0 or (cap and (cap - 1)) != 0 or maxBlobLen < 0:
    return ShmRing[opDropSignalled]() # invalid: caller checks isValid
  ShmRing[opDropSignalled](seg: createSegment(path, cap, maxBlobLen, boot))

proc attachRing*(path: string): ShmRing[opDropSignalled] =
  ## Attach to an existing DROP-ON-FULL ring segment. Returns an invalid ring
  ## when the file is missing / wrong-sized / stale (wrong magic / formatVersion
  ## / creatorBootId — the boot guard). The geometry is read from the header.
  ShmRing[opDropSignalled](seg: attachSegment(path))

proc createBlockingRing*(path: string; cap, maxBlobLen: int;
    boot: uint64): ShmRing[opBlockProducer] =
  ## Create a fresh LOSSLESS (block-on-full) ring segment. Same on-segment format
  ## as `createRing`; only the producer's overflow discipline differs (it waits
  ## for a slot rather than dropping). The consumer should call
  ## `registerConsumer` before spawning producers so a waiting producer can
  ## detect a dead consumer (LF-4).
  if cap <= 0 or (cap and (cap - 1)) != 0 or maxBlobLen < 0:
    return ShmRing[opBlockProducer]()
  ShmRing[opBlockProducer](seg: createSegment(path, cap, maxBlobLen, boot))

proc attachBlockingRing*(path: string): ShmRing[opBlockProducer] =
  ## Attach to an existing block-on-full ring segment (producer side).
  ShmRing[opBlockProducer](seg: attachSegment(path))

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

  # --- block-producer (opBlockProducer) push -------------------------------
  #
  # Bounded-spin → futex-wait on `head` with a consumer-liveness re-check, so a
  # full ring makes the producer WAIT for a slot (never drop) while the consumer
  # is alive, and fail fast (`prConsumerGone`) when it is not. Only used by the
  # segment-owning `ShmRing[opBlockProducer]` (embedded rings stay drop-only).

  const
    BlockSpinLimit* = 256
      ## CAS-reservation spin attempts before parking on the futex. The common
      ## case (consumer keeps up) resolves here with no syscall.
    BlockFutexTimeoutNs* = 20_000_000'i64
      ## Futex wait slice (20 ms). A killed consumer that can never wake us is
      ## bounded by this: on timeout the producer re-probes liveness and, if the
      ## consumer is dead, returns `prConsumerGone`.

  proc pushBlobBlocking(base: ShmBase; cap, maxBlobLen: int;
      blob: openArray[byte]): PushResult =
    if blob.len > maxBlobLen:
      return prOversize
    var tail = loadU64Acquire(base, HdrOffTail)
    var spins = 0
    while true:
      let head = loadU64Acquire(base, HdrOffHead)
      if tail - head >= uint64(cap):
        # Ring full. Never drop: wait for the consumer to free a slot, but only
        # while it is alive.
        if not consumerAlive(base):
          return prConsumerGone
        if spins < BlockSpinLimit:
          inc spins
          tail = loadU64Acquire(base, HdrOffTail)
          continue
        # Park. Register as a waiter (so the consumer knows to wake us), then
        # re-check under the registration to avoid a lost wakeup.
        discard fetchAddU64(base, HdrOffWaiters, 1)
        let head2 = loadU64Acquire(base, HdrOffHead)
        let tail2 = loadU64Acquire(base, HdrOffTail)
        if tail2 - head2 < uint64(cap):
          discard fetchSubU64(base, HdrOffWaiters, 1)
          tail = tail2
          spins = 0
          continue
        if not consumerAlive(base):
          discard fetchSubU64(base, HdrOffWaiters, 1)
          return prConsumerGone
        futexWaitHead(base, uint32(head2 and 0xFFFF_FFFF'u64), BlockFutexTimeoutNs)
        discard fetchSubU64(base, HdrOffWaiters, 1)
        spins = 0
        tail = loadU64Acquire(base, HdrOffTail)
        continue
      # Not full: reserve ticket `tail`.
      if casU64(base, HdrOffTail, tail, tail + 1):
        break
    # We own ticket `tail`; write the blob then publish via release-store.
    let so = slotOffAt(HdrOffHead, cap, maxBlobLen, tail)
    if blob.len > 0:
      copyMem(addr base[so + SlotOffBlob], unsafeAddr blob[0], blob.len)
    storeU32Release(base, so + SlotOffBlobLen, uint32(blob.len))
    storeU64Release(base, so + SlotOffReady, tail + 1)
    prPushed

  # --- segment-owning ShmRing (ringBase = HdrOffHead) ----------------------

  proc tryPush*[p: static OverflowPolicy](r: var ShmRing[p];
      blob: openArray[byte]): PushResult =
    ## Lock-free multi-producer push. Reserves a ticket by CAS-bumping `tail`,
    ## writes `blob` (u32 length prefix + bytes) into the reserved slot, and
    ## publishes via a release-store of `ready = ticket+1`. A blob longer than the
    ## segment's `maxBlobLen` returns `prOversize` with the ring unchanged.
    ##
    ## `opDropSignalled`: a full ring bumps the atomic `dropped` counter and
    ## returns `prDropped` (SIGNALLED, never silent, never overwriting an unread
    ## slot). `opBlockProducer`: a full ring makes the producer WAIT for a slot
    ## (never drop) while the consumer is alive, returning `prConsumerGone` if
    ## the consumer is dead/absent (never a hang — LF-4).
    if not r.seg.isValid:
      return prOversize
    when p == opDropSignalled:
      pushBlob(r.seg.base, HdrOffHead, r.seg.capacity, r.seg.maxBlobLen, blob)
    else:
      pushBlobBlocking(r.seg.base, r.seg.capacity, r.seg.maxBlobLen, blob)

  proc tryDrainOne*[p: static OverflowPolicy](r: var ShmRing[p];
      outBuf: var openArray[byte]; outLen: var int): DrainResult =
    ## SINGLE-consumer non-blocking drain of the next ready ticket. Returns
    ## `drEmpty` when the head slot is not yet published (empty ring or a
    ## producer mid-write — a torn/unpublished slot is SKIPPED, never read).
    ## Returns `drOverflowBuf` (ring unchanged) when `outBuf` is smaller than
    ## the stored blob. On `drGot`, `outLen` bytes were copied into `outBuf`.
    ##
    ## For `opBlockProducer`, after freeing a slot the consumer wakes any waiting
    ## producers (gated by the `waiters` atomic, so the uncontended path is one
    ## relaxed load with no syscall).
    outLen = 0
    if not r.seg.isValid:
      return drEmpty
    let dr = drainBlob(r.seg.base, HdrOffHead, r.seg.capacity,
      r.seg.maxBlobLen, outBuf, outLen)
    when p == opBlockProducer:
      if dr == drGot and loadU64Relaxed(r.seg.base, HdrOffWaiters) > 0:
        futexWakeHead(r.seg.base)
    dr

  proc registerConsumer*(r: var ShmRing[opBlockProducer]) =
    ## Consumer side: publish this process as the live drainer (LF-4). Call once
    ## before producers may start waiting.
    if r.seg.isValid: registerConsumer(r.seg.base)

  proc markConsumerGone*(r: var ShmRing[opBlockProducer]) =
    ## Consumer side: announce a clean shutdown and wake any waiters so they see
    ## `prConsumerGone` immediately instead of after the futex timeout.
    if r.seg.isValid: deregisterConsumer(r.seg.base)

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

  const
    BlockSpinLimit* = 256
    BlockFutexTimeoutNs* = 20_000_000'i64
  proc tryPush*(r: var ShmRing; blob: openArray[byte]): PushResult = prOversize
  proc tryDrainOne*(r: var ShmRing; outBuf: var openArray[byte];
      outLen: var int): DrainResult =
    outLen = 0
    drEmpty
  proc droppedCount*(r: ShmRing): uint64 = 0
  proc pendingCount*(r: ShmRing): uint64 = 0
  proc registerConsumer*(r: var ShmRing[opBlockProducer]) = discard
  proc markConsumerGone*(r: var ShmRing[opBlockProducer]) = discard

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
