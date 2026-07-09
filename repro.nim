## Reprobuild project file for nim-shm-queue.
##
## **Follow-up rollout (FUP-A): give the new library the same treatment as
## the other workspace repos.** This is a Nim leaf library — a layered
## shared-memory MPSC queue: a byte-blob ring (L1) + a typed ``(T, Format)``
## queue (L2) over a compact binary flavor. It has NO in-scope sibling
## build dependency of its own: every importable module lives under this
## repo's ``src/`` tree. The L2 suite compiles against the status-im
## ``serialization`` / ``faststreams`` / ``stew`` trees, but those are
## THIRD-PARTY (rollout-excluded, no ``repro.nim`` / ``library`` producer),
## so they are threaded via the L2 edge's ``paths:`` slot (resolved by
## ``--path``, exactly as ``config.nims``'s ``srzPaths`` does) rather than a
## ``uses: "<sibling>"`` edge. So the ``uses:`` block is just the toolchain
## floor and there is no ``uses: "<sibling>"`` edge — the lock is self-only.
##
## A Mode 1 / Mode 3 hybrid (per
## ``reprobuild-specs/Three-Mode-Convention-System.md``) modelled on the
## canonical ``nim-libvterm/repro.nim`` / ``reprobuild-test-adapters/repro.nim``
## / ``nim-stackable-hooks/repro.nim`` leaf recipes:
##
## * Declares the upstream tool floor via ``uses:`` so consumers that
##   depend on this repo (via ``uses: "shm_queue"``) pick up the same
##   toolchain the nimble file's ``requires "nim >= 2.0.0"`` implies.
## * Declares ``library shm_queue`` so future consumers can express a
##   workspace dependency on this repo. The importable umbrella is
##   ``src/shm_queue.nim``; consumers may also import the submodules under
##   ``src/shm_queue/`` directly (``ring``, ``segment``, ``typed_queue``,
##   ``binary_flavor``). The ``src`` tree is the exported path
##   ``config.nims`` puts on ``--path`` (``switch("path", "src")``).
## * Emits, per test file under ``tests/``, a BUILD edge
##   (``buildNimUnittest.build``) that compiles ``build/test-bin/<stem>``
##   and an EXECUTE edge (``edge.testBinary.run``) that runs it — the
##   two-edge test template from ``reprobuild-specs/Package-Model.md``
##   §"The test template", exactly as reprobuild's own ``repro.nim`` does
##   it. The BUILD halves collect into ``test-builds`` and the EXECUTE
##   halves into ``test`` so ``repro build test`` / ``repro test``
##   materialise the runnable closure (each execute edge transitively
##   depends on its build edge).
## * Emits, per benchmark file under ``benchmarks/``, a BUILD-ONLY edge
##   (compile-check, no execute) that collects into ``test-builds`` so the
##   benchmarks never bit-rot — the perf run itself is not gated on CI.
##
## **Compile flags.**
##   * ``--path:src`` (`config.nims`'s ``switch("path", "src")``) is threaded
##     via the edge's ``paths:`` slot on every edge; ``tests`` is added so
##     the L2 test's helper types resolve.
##   * ``--threads:on`` (``config.nims``'s ``switch("threads", "on")``): the
##     multi-thread producer test + benchmarks spawn producer threads. This
##     is the ``buildNimUnittest.build`` wrapper's default (``threadsOn``),
##     so nothing extra is needed.
##   * The L1 edge (``test_ring_byte_blobs``) needs only ``nim`` + the Nim
##     stdlib — it imports ``std/[os, posix, sets, unittest]`` +
##     ``shm_queue/[ring, segment]``, no serialization.
##   * The L2 edge (``test_typed_queue_nim_spectrum``) additionally imports
##     ``serialization`` + ``faststreams/[inputs, outputs]``, so it threads
##     the three status-im lib ``src`` roots via ``paths:`` (matching
##     ``config.nims``'s ``srzPaths()``). The typed benchmark
##     (``bench_typed_queue``) does likewise.
##   * ``-d:release`` on the benchmark compile-check edges (the perf build
##     mode); the test edges use the default (debug) build, matching a bare
##     ``nim c -r`` on the file / ``nimble test``.
##
## **Per-test platform gating.** Both test files compile + run to exit 0 on
## this Linux host. The fork-based multi-PROCESS suites and the
## ``EmbeddedRing`` suite self-gate at runtime via ``shmSegmentSupported`` /
## ``when not shmSegmentSupported: skip()`` inside the file — a POSITIVE
## capability arm, not a compile gate — so the whole corpus is portable and
## every edge is unconditionally in the graph (no ``when defined(...)``
## extraction gate is needed).
##
## **Tool provisioning.** ``defaultToolProvisioning "path"`` matches the
## canonical leaf recipes: the nix dev shell puts ``nim`` + ``gcc`` on
## ``PATH``, so the weak-local PATH resolver is the right default. Without
## it ``repro build`` refuses to run with "typed tool provisioning is
## required for uses declarations".

import repro_project_dsl

# ``ct_test_nim_unittest`` supplies the ``buildNimUnittest.build(...)``
# typed-tool used by every BUILD edge below, and the
# ``edge.testBinary.run(...)`` UFCS dispatch for the EXECUTE edges. It
# re-exports ``repro_project_dsl`` so the import order is unimportant.
#
# Note: like the other leaf recipes this file does NOT import
# ``ct_test_runner_install`` / call ``installCtTestRunner`` — that module is
# engine-coupled and lives at reprobuild's repo root, importable only from
# reprobuild's own project extraction. Without it the execute edges route
# through the engine's default direct-binary runner (run the binary, key on
# exit status), which is exactly the exit-0 verification this corpus needs;
# the Nim ``unittest`` harness already prints per-suite results and exits
# non-zero on failure.
import ct_test_nim_unittest

type
  ShmQueueTestSpec = object
    ## One entry per test file. ``source`` is the repo-relative ``.nim``
    ## path; ``binary`` is the ``build/test-bin/<stem>`` output. ``needsSrz``
    ## flags the L2 files that additionally need the status-im
    ## serialization/faststreams/stew ``src`` roots on ``--path``.
    source: string
    binary: string
    needsSrz: bool

# The three status-im lib ``src`` roots the L2 corpus compiles against, in
# the workspace layout (vendored under the sibling reprobuild checkout). These
# are THIRD-PARTY (rollout-excluded, no ``repro.nim`` / ``library`` producer),
# so they ride in via ``paths:`` — NOT a ``uses:`` edge. Mirrors
# ``config.nims``'s ``srzPaths()`` (the same ``../reprobuild/libs/nim-*/src``
# triple).
const srzPaths = @[
  "../reprobuild/libs/nim-serialization/src",
  "../reprobuild/libs/nim-faststreams/src",
  "../reprobuild/libs/nim-stew/src",
]

const shmQueueTestSpecs: seq[ShmQueueTestSpec] = @[
  # L1 — byte-blob MPSC ring coordination corner cases. Only nim + stdlib.
  ShmQueueTestSpec(source: "tests/test_ring_byte_blobs.nim",
    binary: "build/test-bin/test_ring_byte_blobs", needsSrz: false),
  # L2 — typed `(T, Format)` spectrum + cross-process roundtrip. Needs the
  # status-im serialization/faststreams/stew src roots via `paths:`.
  ShmQueueTestSpec(source: "tests/test_typed_queue_nim_spectrum.nim",
    binary: "build/test-bin/test_typed_queue_nim_spectrum", needsSrz: true),
]

const shmQueueBenchSpecs: seq[ShmQueueTestSpec] = @[
  # Benchmarks are compile-checked (BUILD-only) so they never bit-rot; the
  # perf run itself is not gated on CI.
  ShmQueueTestSpec(source: "benchmarks/bench_ring_throughput.nim",
    binary: "build/bench-bin/bench_ring_throughput", needsSrz: false),
  ShmQueueTestSpec(source: "benchmarks/bench_typed_queue.nim",
    binary: "build/bench-bin/bench_typed_queue", needsSrz: true),
]

package shm_queue:
  defaultToolProvisioning "path"

  uses:
    # Toolchain floor — the PATH-resolvable binaries the build needs.
    # ``nim`` compiles every binary (the ``buildNimUnittest.build`` edges
    # below); ``gcc`` is the C back-end ``nim c`` shells out to. The lower
    # bound mirrors the nimble file's ``requires "nim >= 2.0.0"``;
    # ``gcc >=12`` matches the workspace toolchain floor. Sufficient for the
    # path-mode resolver under ``nix develop``.
    "nim >=2.0"
    "gcc >=12"

  # Library declaration — the ``src/`` tree ``config.nims`` puts on
  # ``--path`` (``switch("path", "src")``) is importable when this package
  # is consumed via ``uses: "shm_queue"``. The umbrella is
  # ``src/shm_queue.nim``; consumers may also import the submodules under
  # ``src/shm_queue/`` directly.
  library shm_queue

  devEnv:
    task "bump-version", command = "just bump-version", description = "Bump version number (version.txt)"

  build:
    # Two-edge test template (Package-Model.md §"The test template"): one
    # compile BUILD edge + one EXECUTE edge per test file. BUILD halves
    # collect into ``test-builds`` (compile-only verification); EXECUTE
    # halves collect into ``test`` so ``repro test`` / ``repro build test``
    # materialise the runnable closure (each execute edge transitively
    # depends on its build edge). Benchmark files get a BUILD-only edge
    # (compile-check, no execute) into ``test-builds``.
    #
    # ``paths`` supplies ``--path:src --path:tests`` (config.nims's
    # ``switch("path","src")`` — the engine compile doesn't read
    # config.nims), plus the three status-im lib ``src`` roots for the L2
    # files. ``src`` is a declared ``extraInput`` so the monitor tracks the
    # transitively imported ``src/shm_queue/**`` module tree.
    var testBuildActions: seq[BuildActionDef] = @[]
    var testExecuteActions: seq[BuildActionDef] = @[]

    proc stemOf(binary: string): string =
      var lastSlash = -1
      for i in 0 ..< binary.len:
        if binary[i] == '/' or binary[i] == '\\':
          lastSlash = i
      if lastSlash >= 0: binary[lastSlash + 1 .. ^1] else: binary

    proc pathsFor(needsSrz: bool): seq[string] =
      result = @["src", "tests"]
      if needsSrz:
        for p in srzPaths: result.add(p)

    proc emitTestPair(spec: ShmQueueTestSpec;
                      buildActions, executeActions: var seq[BuildActionDef]) =
      let stem = stemOf(spec.binary)
      let edge = buildNimUnittest.build(
        source = spec.source,
        binary = spec.binary,
        paths = pathsFor(spec.needsSrz),
        extraInputs = @["src"],
        actionId = "shm_queue.test_build." & stem)
      buildActions.add(edge.action)
      # ``registerImplicitName = false`` because the BUILD edge already owns
      # the binary basename as the implicit target name; the explicit
      # ``actionId`` is the execute edge's selector (two-edge shape).
      let executeEdge = edge.testBinary.run(
        actionId = "shm_queue.test_execute." & stem,
        registerImplicitName = false)
      executeActions.add(executeEdge)

    proc emitBenchBuild(spec: ShmQueueTestSpec;
                        buildActions: var seq[BuildActionDef]) =
      let stem = stemOf(spec.binary)
      # BUILD-only: compile-check the benchmark at ``-d:release`` (the perf
      # build mode). No EXECUTE edge — the benchmark run is not CI-gated.
      let edge = buildNimUnittest.build(
        source = spec.source,
        binary = spec.binary,
        defines = @["release"],
        paths = pathsFor(spec.needsSrz),
        extraInputs = @["src"],
        actionId = "shm_queue.bench_build." & stem)
      buildActions.add(edge.action)

    for spec in shmQueueTestSpecs:
      emitTestPair(spec, testBuildActions, testExecuteActions)

    for spec in shmQueueBenchSpecs:
      emitBenchBuild(spec, testBuildActions)

    discard collect("test", testExecuteActions)
    discard collect("test-builds", testBuildActions)
