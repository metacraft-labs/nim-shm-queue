## Layer-1 push+drain throughput benchmark (spec §4).
##
## Measures single-producer and N-producer push+drain ops/s for the byte-blob
## ring, and prints ops/s + p50/p99 per-op latency. The goal is >= the ~946K
## emits/s RMDF baseline noted in io-mon `writer.nim` (the ring must not regress
## the monitor hot path it replaces).
##
## Run: nim c -r -d:release --threads:on benchmarks/bench_ring_throughput.nim

import std/[monotimes, os, algorithm, strformat]
import shm_queue/[ring, segment]

const
  BlobLen = 48 ## representative RMDF-record-ish blob size
  Cap = 1024

proc nowNs(): int64 = getMonoTime().ticks

proc pctl(sorted: seq[int64]; p: float): int64 =
  if sorted.len == 0: return 0
  let idx = int(p * float(sorted.len - 1) + 0.5)
  sorted[min(idx, sorted.len - 1)]

proc reportLatency(label: string; lats: var seq[int64]) =
  lats.sort()
  let p50 = pctl(lats, 0.50)
  let p99 = pctl(lats, 0.99)
  echo &"  {label}: p50={p50}ns p99={p99}ns (samples={lats.len})"

# --- single-producer / single-consumer, same thread ------------------------

proc benchSingle(nOps: int): float =
  ## Interleaved push+drain in one thread: each op = one push followed by one
  ## drain. ops/s = nOps / elapsed. This is the tightest possible loop and the
  ## direct analogue of the RMDF emit hot path.
  let path = getTempDir() / ("shmq-bench-single-" & $getCurrentProcessId() & ".seg")
  defer:
    try: removeFile(path)
    except CatchableError: discard
  var r = createRing(path, Cap, BlobLen, bootId())
  doAssert r.isValid
  var blob = newSeq[byte](BlobLen)
  for i in 0 ..< BlobLen: blob[i] = byte(i)
  var outBuf = newSeq[byte](BlobLen)
  var outLen = 0

  # warmup
  for _ in 0 ..< 100_000:
    discard r.tryPush(blob)
    discard r.tryDrainOne(outBuf, outLen)

  var lats = newSeqOfCap[int64](min(nOps, 1_000_000))
  let sampleEvery = max(1, nOps div 1_000_000)
  let t0 = nowNs()
  for i in 0 ..< nOps:
    if (i mod sampleEvery) == 0:
      let s = nowNs()
      doAssert r.tryPush(blob) == prPushed
      doAssert r.tryDrainOne(outBuf, outLen) == drGot
      lats.add(nowNs() - s)
    else:
      discard r.tryPush(blob)
      discard r.tryDrainOne(outBuf, outLen)
  let elapsed = float(nowNs() - t0) / 1e9
  result = float(nOps) / elapsed
  r.detach()
  echo &"single-producer: {result:.0f} ops/s over {nOps} ops in {elapsed:.3f}s"
  reportLatency("single push+drain", lats)

# --- N producers (threads) + single consumer -------------------------------

type ProdArg = object
  path: string
  tid: int
  perThread: int

proc producerThread(arg: ProdArg) {.thread.} =
  var pr = attachRing(arg.path)
  doAssert pr.isValid
  var blob = newSeq[byte](BlobLen)
  blob[0] = byte(arg.tid)
  var i = 0
  while i < arg.perThread:
    blob[1] = byte(i and 0xFF)
    blob[2] = byte((i shr 8) and 0xFF)
    blob[3] = byte((i shr 16) and 0xFF)
    if pr.tryPush(blob) == prPushed:
      inc i
  pr.detach()

proc benchNProducers(nThreads, perThread: int): float =
  let path = getTempDir() / ("shmq-bench-multi-" & $getCurrentProcessId() & ".seg")
  defer:
    try: removeFile(path)
    except CatchableError: discard
  var r = createRing(path, Cap, BlobLen, bootId())
  doAssert r.isValid
  let total = nThreads * perThread

  var threads = newSeq[Thread[ProdArg]](nThreads)
  var outBuf = newSeq[byte](BlobLen)

  let t0 = nowNs()
  for t in 0 ..< nThreads:
    createThread(threads[t], producerThread,
      ProdArg(path: path, tid: t, perThread: perThread))

  var drained = 0
  var outLen = 0
  while drained < total:
    if r.tryDrainOne(outBuf, outLen) == drGot:
      inc drained
  for t in 0 ..< nThreads:
    joinThread(threads[t])
  let elapsed = float(nowNs() - t0) / 1e9
  result = float(total) / elapsed
  echo &"{nThreads}-producer: {result:.0f} ops/s over {total} ops in {elapsed:.3f}s (dropped={r.droppedCount()})"
  r.detach()

when isMainModule:
  const Goal = 946_000.0
  echo "shm_queue Layer-1 ring throughput (goal >= ", int(Goal), " ops/s)"
  echo "----------------------------------------------------------------"
  let single = benchSingle(20_000_000)
  echo ""
  let multi2 = benchNProducers(2, 5_000_000)
  let multi4 = benchNProducers(4, 3_000_000)
  echo ""
  echo &"single-producer ops/s = {single:.0f}  (goal {Goal:.0f})"
  if single >= Goal:
    echo "GOAL MET (single-producer >= 946K ops/s)"
  else:
    echo &"BELOW GOAL by {(Goal - single) / Goal * 100:.1f}%"
    quit(1)
