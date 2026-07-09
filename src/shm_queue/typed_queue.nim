## Layer 2 — `TypedShmQueue[T, Format]`: a typed view over the Layer-1 byte-blob
## ring, parametric over a payload type `T` and a nim-serialization `Format`
## (a `serialization` flavor). The `(T, Format)` pair IS the encode/decode:
##
##   * `push(q, item: T)` serializes `item` via `Format` into a per-queue
##     scratch stream, then `ring.tryPush`es the encoded bytes. A serialized
##     value longer than the ring's `maxBlobLen` returns `prOversize` with the
##     ring UNCHANGED, so the caller can fall back (SIGNALLED, never silent).
##   * `tryDrainOne(q, item: var T)` drains a blob via `ring.tryDrainOne`, then
##     deserializes it via `Format` into `item`.
##
## The `Format` axis is open: the library ships a compact binary flavor
## (`binary_flavor.ShmBinary`) so a consumer gets a working `(T, ShmBinary)` out
## of the box, but any other `serialization` `Format` (e.g. json-serialization's
## `Json`, or an io-mon RMDF flavor) may be supplied for the same `T`.
##
## No hot-path heap allocation: the encode side serializes into a REUSED per-queue
## `memoryOutput` stream whose pages are recycled after every push (steady-state:
## no allocation once the pages are warm — same discipline as io-mon's fixed
## `encodeDepRecord` scratch, but format-agnostic). Its bytes are copied straight
## into the reserved ring slot; oversize is detected by the encoded length and
## leaves the ring untouched (no partial write). The drain side decodes from a
## preallocated per-queue read buffer. Both scratch structures are owned by the
## `TypedShmQueue` value, keeping the typed layer fork/orc-safe on a monitored
## hot path.
##
## The encoded bytes are produced with `serialization`'s stream `writeValue` /
## `decode`, so the on-blob bytes for a fixed `(T, Format)` are stable across runs
## and processes.

import ./ring
export ring

import serialization
import faststreams/outputs

type
  TypedShmQueue*[T; Format] = object
    ## A typed view over an `ShmRing`. `T` is the payload type; `Format` is a
    ## `serialization` format/flavor. Copy semantics follow the ring (an owned
    ## mapping); use `detach` to release. The scratch structures are per-queue and
    ## MUST NOT be shared across threads — each producer/consumer holds its own
    ## `TypedShmQueue` (as it holds its own `ShmRing` view), same as Layer 1.
    ring*: ShmRing
    encStream: OutputStream   ## reused producer-side encode stream (pages recycled)
    decBuf: seq[byte]         ## consumer-side scratch for the drained blob

func isValid*(q: TypedShmQueue): bool {.inline.} = q.ring.isValid
func capacity*(q: TypedShmQueue): int {.inline.} = q.ring.capacity
func maxBlobLen*(q: TypedShmQueue): int {.inline.} = q.ring.maxBlobLen

proc droppedCount*(q: TypedShmQueue): uint64 {.inline.} = q.ring.droppedCount
proc pendingCount*(q: TypedShmQueue): uint64 {.inline.} = q.ring.pendingCount

# --- constructors (mirror Layer 1) -----------------------------------------

proc createTypedQueue*[T, Format](path: string; cap, maxBlobLen: int;
    boot: uint64): TypedShmQueue[T, Format] =
  ## Create a fresh typed queue backing an `ShmRing` of `cap` slots each holding
  ## up to `maxBlobLen` serialized bytes, stamped with `boot`. `cap` MUST be a
  ## power of two; on a bad geometry / create failure the queue is invalid
  ## (`isValid` is false). Preallocates the per-queue scratch structures.
  result.ring = createRing(path, cap, maxBlobLen, boot)
  result.encStream = memoryOutput().s
  result.decBuf = newSeq[byte](max(maxBlobLen, 0))

proc attachTypedQueue*[T, Format](path: string): TypedShmQueue[T, Format] =
  ## Attach to an existing typed queue segment. Invalid (so the caller can fall
  ## back / recreate) when the file is missing / wrong-sized / stale. Geometry is
  ## read from the header; the read buffer is sized to it.
  result.ring = attachRing(path)
  result.encStream = memoryOutput().s
  result.decBuf = newSeq[byte](max(result.ring.maxBlobLen, 0))

proc detach*(q: var TypedShmQueue) =
  ## Unmap + close the underlying ring. Does NOT unlink the backing file.
  q.ring.detach()

# --- typed push / drain -----------------------------------------------------

proc push*[T, Format](q: var TypedShmQueue[T, Format]; item: T): PushResult =
  ## Serialize `item` via `Format` into the reused per-queue encode stream, then
  ## push the encoded bytes onto the ring. Returns `prOversize` (ring UNCHANGED)
  ## when the serialized value is longer than `maxBlobLen`, so the caller can fall
  ## back; `prDropped` when the ring is full (SIGNALLED via the drop counter);
  ## `prPushed` on success.
  ##
  ## No hot-path heap allocation in steady state: `item` is serialized with
  ## `serialization`'s stream `writeValue` into a REUSED `memoryOutput` whose
  ## pages are recycled by `consumeContiguousOutput` on every call. The encoded
  ## bytes are copied directly into the reserved ring slot by `ring.tryPush`.
  when not shmSegmentSupported:
    return prOversize
  else:
    if not q.ring.isValid:
      return prOversize
    # Serialize into the reused stream. `consumeContiguousOutput` hands us the
    # contiguous encoded bytes AND recycles the stream's pages for the next push.
    # Bind the ring into an addressable local so the consumer body does not
    # capture the whole `var TypedShmQueue` (which cannot be captured safely).
    writeValue(q.encStream, Format, item)
    let maxLen = q.ring.maxBlobLen
    let ringPtr = addr q.ring
    var res = prPushed
    consumeContiguousOutput(q.encStream, encoded):
      if encoded.len > maxLen:
        res = prOversize                 # queue unchanged; caller falls back
      else:
        res = ringPtr[].tryPush(encoded)
    res

proc tryDrainOne*[T, Format](q: var TypedShmQueue[T, Format];
    item: var T): DrainResult =
  ## Drain one blob from the ring and deserialize it via `Format` into `item`.
  ## Returns `drEmpty` when nothing is ready; `drGot` when `item` was decoded.
  ## Decodes from the per-queue read buffer (no hot-path allocation for the
  ## drain itself; `T`'s own indirect fields allocate as the type requires).
  when not shmSegmentSupported:
    return drEmpty
  else:
    if not q.ring.isValid:
      return drEmpty
    if q.decBuf.len < q.ring.maxBlobLen:
      q.decBuf.setLen(q.ring.maxBlobLen)
    var outLen = 0
    let dr = q.ring.tryDrainOne(q.decBuf, outLen)
    if dr != drGot:
      return dr
    item = Format.decode(q.decBuf.toOpenArray(0, outLen - 1), T)
    drGot
