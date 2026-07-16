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

proc srzPaths(): string =
  ## Layer 2 builds against nim-serialization + faststreams + stew. When the
  ## reprobuild sibling checkout is present (workspace layout), thread its vendored
  ## lib paths so the suite builds without a separate nimble install; otherwise
  ## rely on the nimble-resolved `serialization` dependency.
  const libs = [
    "../reprobuild/libs/nim-serialization/src",
    "../reprobuild/libs/nim-faststreams/src",
    "../reprobuild/libs/nim-stew/src",
  ]
  result = ""
  for p in libs:
    if dirExists(p):
      result.add " --path:" & p

task test, "Run the layer-agnostic test suite":
  # Layer 1 (SHM-QUEUE-L1): the byte-blob ring coordination corner cases.
  exec "nim c -r --hints:off --threads:on --warning:BareExcept:off " &
    "tests/test_ring_byte_blobs.nim"
  # Layer 1 (SHM-QUEUE-L1): the opBlockProducer lossless overflow policy.
  exec "nim c -r --hints:off --threads:on --warning:BareExcept:off " &
    "tests/test_ring_block_producer.nim"
  # Layer 2 (SHM-QUEUE-L2): the typed (T, Format) spectrum suite.
  if fileExists("tests/test_typed_queue_nim_spectrum.nim"):
    exec "nim c -r --hints:off --threads:on --warning:BareExcept:off" &
      srzPaths() & " tests/test_typed_queue_nim_spectrum.nim"
