## Justfile — nim-shm-queue.
##
## Recipe taxonomy (per `codetracer-specs/Repo-Requirements.md`):
##   * Top-level aggregates: `build`, `test`, `lint`, `format` / `fmt`.
##   * `test` runs the L1 (byte-blob ring) + L2 (typed `(T, Format)`
##     spectrum) suites — the same two files `nimble test` runs, wired
##     with the sibling `serialization`/`faststreams`/`stew` src paths L2
##     needs. `test-unit` is the L1 corner-case suite; `test-integration`
##     the L2 cross-process typed-roundtrip suite.
##   * `lint` is `nim check` over the umbrella + submodules + both test
##     files (no `flake.nix` in this repo → no `nixfmt` cross-check).
##   * `format` runs `nimpretty` when available.
##   * Hermetic flags (`--skipParentCfg --skipUserCfg`) are baked into
##     `nim-flags` so every invocation gets the same isolation. `config.nims`
##     already supplies `--path:src --threads:on`; the recipes re-state
##     `--path:src` explicitly for the hermetic (`--skipParentCfg`) case.

alias t := test
alias fmt := format

# Path lookups — keep the source layout discoverable even under
# `--skipParentCfg` (which suppresses `config.nims`'s `switch("path","src")`).
src-paths := "--path:src --path:tests"

# Hermetic + threading flags applied to every nim invocation in this file.
# `--threads:on` mirrors `config.nims` (the multi-thread producer test +
# benchmarks spawn producer threads).
nim-flags := "--skipParentCfg --skipUserCfg --threads:on --warning:BareExcept:off"

# Layer-2 builds against the status-im nim-serialization + faststreams + stew
# trees. In the workspace layout they are vendored under the sibling
# reprobuild checkout (matching `config.nims`'s `srzPaths`); thread whichever
# of the three exist so the L2 suite + typed benchmark compile.
srz-paths := ```
    p=""
    for d in ../reprobuild/libs/nim-serialization/src \
             ../reprobuild/libs/nim-faststreams/src \
             ../reprobuild/libs/nim-stew/src; do
      [ -d "$d" ] && p="$p --path:$d"
    done
    echo "$p"
    ```

# --- Default targets (per Repo-Requirements.md) ---

# Build: compile (no run) both test files as a sanity check.
build:
    @mkdir -p test-logs
    nim c {{nim-flags}} {{src-paths}} -d:release \
        -o:test-logs/test_ring_byte_blobs \
        tests/test_ring_byte_blobs.nim 2>&1 | tee test-logs/build.log
    nim c {{nim-flags}} {{src-paths}} {{srz-paths}} -d:release \
        -o:test-logs/test_typed_queue_nim_spectrum \
        tests/test_typed_queue_nim_spectrum.nim 2>&1 | tee -a test-logs/build.log

# Test: run the L1 + L2 suites (the same two files `nimble test` runs).
test: test-unit test-integration

# L1 (SHM-QUEUE-L1): byte-blob MPSC ring coordination corner cases.
test-unit:
    @mkdir -p test-logs
    nim c -r {{nim-flags}} {{src-paths}} \
        tests/test_ring_byte_blobs.nim 2>&1 | tee test-logs/test-unit.log

# L2 (SHM-QUEUE-L2): typed `(T, Format)` spectrum + cross-process roundtrip.
test-integration:
    @mkdir -p test-logs
    nim c -r {{nim-flags}} {{src-paths}} {{srz-paths}} \
        tests/test_typed_queue_nim_spectrum.nim 2>&1 | tee test-logs/test-integration.log

# Compile-check the benchmarks (no run) so they never bit-rot.
bench-check:
    @mkdir -p test-logs
    nim c {{nim-flags}} {{src-paths}} -d:release \
        -o:test-logs/bench_ring_throughput \
        benchmarks/bench_ring_throughput.nim 2>&1 | tee test-logs/bench-check.log
    nim c {{nim-flags}} {{src-paths}} {{srz-paths}} -d:release \
        -o:test-logs/bench_typed_queue \
        benchmarks/bench_typed_queue.nim 2>&1 | tee -a test-logs/bench-check.log

# Run the benchmarks (release + threads).
bench: bench-check
    @mkdir -p test-logs
    ./test-logs/bench_ring_throughput
    ./test-logs/bench_typed_queue

# Lint: nim check over the umbrella + submodules + both test files.
lint: lint-nim

lint-nim:
    #!/usr/bin/env bash
    # `pipefail` so a non-zero `nim check` exit propagates through the
    # `| tee` pipeline into `just lint`'s exit code (otherwise `tee`'s RC=0
    # masks a failing check — a false green). `-e` so ANY of the three
    # checks failing fails the whole recipe.
    set -euo pipefail
    mkdir -p test-logs
    # Umbrella `src/shm_queue.nim` transitively imports `faststreams`, so it
    # needs the same status-im src roots (`srz-paths`) the L2 checks thread.
    nim check {{nim-flags}} {{src-paths}} {{srz-paths}} src/shm_queue.nim 2>&1 | tee test-logs/lint-nim.log
    nim check {{nim-flags}} {{src-paths}} \
        tests/test_ring_byte_blobs.nim 2>&1 | tee -a test-logs/lint-nim.log
    nim check {{nim-flags}} {{src-paths}} {{srz-paths}} \
        tests/test_typed_queue_nim_spectrum.nim 2>&1 | tee -a test-logs/lint-nim.log

# Format: nimpretty when available.
format: format-nim

format-nim:
    @if command -v nimpretty >/dev/null 2>&1; then \
      nimpretty src/shm_queue.nim src/shm_queue/*.nim tests/*.nim benchmarks/*.nim; \
    else \
      echo "nimpretty not available; skipping Nim formatting"; \
    fi

# Single-source-of-truth version bump (version.txt is read by shm_queue.nimble).
bump-version version:
    printf '%s\n' "{{version}}" > version.txt

# Clean test-logs + built binaries.
clean:
    rm -rf test-logs
    find tests benchmarks -maxdepth 1 -type f -executable -not -name "*.nim" -delete
