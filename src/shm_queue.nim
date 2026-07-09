## shm_queue — layered shared-memory MPSC queue.
##
## Layer 1: `shm_queue/ring` — a lock-free multi-producer / single-consumer
## ring as a coordination device over fixed-size BYTE BLOBS (no types, no
## serialization). Ticket-CAS reservation, release-store publish, single-
## consumer drain, atomic SIGNALLED drop counter, versioned + boot-guarded
## mmap segment.
##
## Layer 2: `shm_queue/typed_queue` — `TypedShmQueue[T, Format]`, parametric
## over a payload type `T` and a nim-serialization `Format`; the `(T, Format)`
## pair defines encode/decode over Layer 1's blobs.
##
## The library ships a compact binary flavor (`shm_queue/binary_flavor.ShmBinary`)
## so a consumer gets a working `(T, ShmBinary)` out of the box; the `Format` axis
## stays open for a consumer to supply its own `serialization` format.
##
## Layer 3 (consumers) live in their own repos (io-mon dep queue,
## reprobuild action-cache submission ring) and sit on Layer 2.
##
## Note: Layer 1 (`ring` + `segment`) has NO `serialization` dependency — a
## consumer that only needs raw byte blobs can `import shm_queue/ring` alone.
## Layer 2 (`typed_queue` + `binary_flavor`) pulls in `serialization`.
import shm_queue/[ring, segment, typed_queue, binary_flavor]
export ring, segment, typed_queue, binary_flavor
