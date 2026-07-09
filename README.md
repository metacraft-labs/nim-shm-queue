# shm_queue

A layered shared-memory **multi-producer / single-consumer** queue for Nim.

- **Layer 1 — `shm_queue/ring`:** the ring as a coordination device over
  fixed-size byte blobs. Lock-free ticket-CAS; release-store publish;
  single-consumer drain; atomic *signalled* drop counter; versioned +
  boot-guarded mmap segment. No types, no serialization.
- **Layer 2 — `shm_queue/typed_queue`:** `TypedShmQueue[T, Format]`,
  parametric over a payload type and a nim-serialization `Format` — the
  `(T, Format)` pair is the encode/decode.
- **Layer 3 — consumers:** e.g. io-mon's discovered-dependency queue and
  reprobuild's action-cache submission ring, each on Layer 2.

Every layer has its own agnostic tests + benchmarks. See
`reprobuild-specs/Shm-Queue-Library.md`.
