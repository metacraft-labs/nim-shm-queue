## Layer-2 encode+push+drain+decode throughput benchmark (spec §4).
##
## Measures ops/s + p50/p99 latency for a representative record `T` under the
## library's compact binary flavor (`ShmBinary`), and reports the overhead versus
## the raw Layer-1 push/drain baseline (the same tight interleaved loop that
## `bench_ring_throughput` measures). The typed layer must stay comfortably above
## the ~946K ops/s RMDF baseline for a realistic small record so it does not
## regress the io-mon monitor hot path it is destined to carry.
##
## Run: nim c -r -d:release --threads:on
##        --path:<serialization src> --path:<faststreams src> --path:<stew src>
##        --path:src benchmarks/bench_typed_queue.nim

import std/[monotimes, os, algorithm, strformat]
import shm_queue/[ring, typed_queue, binary_flavor]

type
  # A realistic small io-mon-ish record: a syscall/dep event.
  DepEvent = object
    kind: uint8
    pid: uint32
    ts: uint64
    path: string
    flags: uint32

const
  Cap = 1024
  Slot = 256   ## serialized DepEvent fits comfortably

proc nowNs(): int64 = getMonoTime().ticks

proc pctl(sorted: seq[int64]; p: float): int64 =
  if sorted.len == 0: return 0
  let idx = int(p * float(sorted.len - 1) + 0.5)
  sorted[min(idx, sorted.len - 1)]

proc reportLatency(label: string; lats: var seq[int64]) =
  lats.sort()
  echo &"  {label}: p50={pctl(lats, 0.50)}ns p99={pctl(lats, 0.99)}ns (samples={lats.len})"

proc sampleEvent(i: int): DepEvent =
  DepEvent(kind: uint8(i and 0x7), pid: uint32(1000 + (i and 0xFFFF)),
    ts: uint64(i) * 131, path: "/usr/lib/x/file" & $(i and 0xFF) & ".so",
    flags: uint32(i and 0xF))

# --- Layer-2: encode+push+drain+decode, interleaved single thread -----------

proc benchTyped(nOps: int): float =
  let path = getTempDir() / ("shmq-tqbench-" & $getCurrentProcessId() & ".seg")
  defer:
    try: removeFile(path)
    except CatchableError: discard
  var q = createTypedQueue[DepEvent, ShmBinary](path, Cap, Slot, bootId())
  doAssert q.isValid
  var got: DepEvent

  for i in 0 ..< 100_000:            # warmup
    discard q.push(sampleEvent(i))
    discard q.tryDrainOne(got)

  var lats = newSeqOfCap[int64](min(nOps, 1_000_000))
  let sampleEvery = max(1, nOps div 1_000_000)
  let t0 = nowNs()
  for i in 0 ..< nOps:
    let ev = sampleEvent(i)
    if (i mod sampleEvery) == 0:
      let s = nowNs()
      doAssert q.push(ev) == prPushed
      doAssert q.tryDrainOne(got) == drGot
      lats.add(nowNs() - s)
    else:
      discard q.push(ev)
      discard q.tryDrainOne(got)
  let elapsed = float(nowNs() - t0) / 1e9
  result = float(nOps) / elapsed
  q.detach()
  echo &"typed (encode+push+drain+decode): {result:.0f} ops/s over {nOps} ops in {elapsed:.3f}s"
  reportLatency("typed push+drain", lats)

# --- Layer-1 raw baseline: push+drain a precomputed blob of the same size ----

proc benchRawL1(nOps: int; blobLen: int): float =
  let path = getTempDir() / ("shmq-l1bench-" & $getCurrentProcessId() & ".seg")
  defer:
    try: removeFile(path)
    except CatchableError: discard
  var r = createRing(path, Cap, Slot, bootId())
  doAssert r.isValid
  var blob = newSeq[byte](blobLen)
  for i in 0 ..< blobLen: blob[i] = byte(i)
  var outBuf = newSeq[byte](Slot)
  var outLen = 0

  for _ in 0 ..< 100_000:
    discard r.tryPush(blob)
    discard r.tryDrainOne(outBuf, outLen)

  let t0 = nowNs()
  for _ in 0 ..< nOps:
    discard r.tryPush(blob)
    discard r.tryDrainOne(outBuf, outLen)
  let elapsed = float(nowNs() - t0) / 1e9
  result = float(nOps) / elapsed
  r.detach()
  echo &"raw L1 (push+drain, {blobLen}B blob): {result:.0f} ops/s over {nOps} ops in {elapsed:.3f}s"

when isMainModule:
  const Goal = 946_000.0
  echo "shm_queue Layer-2 typed throughput (goal >= ", int(Goal), " ops/s)"
  echo "----------------------------------------------------------------"
  # size of the encoded representative record, for the raw L1 comparison
  let encodedLen = ShmBinary.encode(sampleEvent(1234)).len
  echo &"representative DepEvent encoded size = {encodedLen} bytes"
  echo ""

  const N = 10_000_000
  let typed = benchTyped(N)
  echo ""
  let raw = benchRawL1(N, encodedLen)
  echo ""

  let overheadPct = (raw - typed) / raw * 100.0
  echo &"typed ops/s   = {typed:.0f}"
  echo &"raw L1 ops/s  = {raw:.0f}"
  echo &"serialization overhead vs raw L1 = {overheadPct:.1f}% " &
    &"({raw / typed:.2f}x slower)"
  echo ""
  if typed >= Goal:
    echo &"GOAL MET (typed {typed:.0f} ops/s >= {Goal:.0f} ops/s RMDF baseline)"
  else:
    echo &"BELOW GOAL by {(Goal - typed) / Goal * 100:.1f}%"
    quit(1)
