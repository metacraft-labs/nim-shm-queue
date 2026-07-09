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
## Layer 3 (consumers) live in their own repos (io-mon dep queue,
## reprobuild action-cache submission ring) and sit on Layer 2.
import shm_queue/[ring, segment]
export ring, segment
