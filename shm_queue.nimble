import std/[strutils]
version       = readFile("version.txt").strip()
author        = "Metacraft Labs"
description   = "Layered shared-memory MPSC queue: byte-blob ring, typed (T, Format) queue, and consumers."
license       = "Apache-2.0"
srcDir        = "src"
skipDirs      = @["tests", "benchmarks"]

requires "nim >= 2.0.0"
# Layer 2 (typed_shm_queue[T, Format]) is parametric over a nim-serialization Format.
requires "serialization"

task test, "Run the layer-agnostic test suite":
  exec "nim c -r --hints:off tests/test_ring_byte_blobs.nim"
  exec "nim c -r --hints:off tests/test_typed_queue_nim_spectrum.nim"
