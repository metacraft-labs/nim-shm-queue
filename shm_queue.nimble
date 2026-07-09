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

import std/os

task test, "Run the layer-agnostic test suite":
  # Layer 1 (SHM-QUEUE-L1): the byte-blob ring coordination corner cases.
  exec "nim c -r --hints:off --threads:on --warning:BareExcept:off " &
    "tests/test_ring_byte_blobs.nim"
  # Layer 2 (SHM-QUEUE-L2): the typed (T, Format) spectrum suite — run only once
  # that milestone has landed the test file.
  if fileExists("tests/test_typed_queue_nim_spectrum.nim"):
    exec "nim c -r --hints:off --threads:on " &
      "tests/test_typed_queue_nim_spectrum.nim"
