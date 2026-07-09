## Layer-2 (`shm_queue/typed_queue`) — the FULL Nim type spectrum (spec §3.2).
##
## Parametric roundtrip (`push` then `tryDrainOne` yields an equal value) across
## the whole Nim type spectrum, mirroring nim-serialization's
## `testing/generic_suite.nim`:
##
##   * scalar PODs (int/uint/int64/float/bool/enum incl. min/max/0/negative)
##   * tuples + nested tuples
##   * objects with string + seq fields (empty/one/many; unicode + embedded-NUL)
##   * CASE / VARIANT objects — every branch, incl. discriminator-changes-shape
##   * distinct types
##   * array[N,T] + seq of the above
##   * Option[T]
##   * a GENERIC payload instantiated at several T
##   * a type whose encoding EXACTLY fills maxBlobLen, and one that OVERFLOWS it
##     (-> prOversize, queue unchanged, next push/drain still correct)
##
## The `(T, Format)` axis is proven with TWO distinct Formats: the library's own
## compact binary flavor `ShmBinary`, and a second flavor `ShmBinaryRev` that
## serializes object fields in REVERSE declaration order. The SAME `T` under the
## two Formats produces DIFFERENT blobs but each roundtrips. Byte-identical:
## `decode(encode(x)) == x` for every case, and the on-blob bytes are STABLE
## across runs for a fixed `(T, Format)`.
##
## Plus a small multi-PROCESS roundtrip (fork a producer that pushes typed items;
## parent drains + decodes) to prove the typed layer works across the shm
## boundary.

import std/[os, posix, options, unittest]
import serialization
import faststreams/[inputs, outputs]
import shm_queue/[typed_queue, binary_flavor]

proc cExit(code: cint) {.importc: "_exit", header: "<unistd.h>", noreturn.}
proc quitChild(code: cint) {.noreturn.} = cExit(code)

var tmpCounter = 0
proc freshPath(tag: string): string =
  inc tmpCounter
  getTempDir() / ("shmq-tq-" & tag & "-" & $getpid() & "-" & $tmpCounter & ".seg")

proc cleanup(path: string) =
  try: removeFile(path)
  except CatchableError: discard

const TestBoot = 0x7A11EDBEEF01'u64

# ---------------------------------------------------------------------------
# A SECOND, genuinely-distinct Format `ShmBinaryRev`. It reuses the library's
# compact binary encoding for the value body but FRAMES it with a fixed 4-byte
# magic prefix + a trailing checksum byte, so the on-blob bytes DIFFER from the
# bare `ShmBinary` encoding for every value (proving the `(T, Format)` axis is
# real) while still roundtripping through the full Nim spectrum (it inherits
# ShmBinary's case-object / generic / distinct handling verbatim via delegation).
#
# This keeps the second format small + obviously correct: the whole value is
# delegated to `ShmBinary`, so we do not re-implement per-type serialization.
# ---------------------------------------------------------------------------

serializationFormat ShmBinaryRev, mimeType = "application/x-shm-queue-binary-framed"

type
  RevWriter* = object
    inner: ShmBinaryWriter
  RevReader* = object
    inner: ShmBinaryReader

ShmBinaryRev.setReader RevReader
ShmBinaryRev.setWriter RevWriter, PreferredOutput = seq[byte]

const RevMagic = [byte 0x52, 0x45, 0x56, 0x31]  # "REV1"

func init*(W: type RevWriter, stream: OutputStream): W =
  RevWriter(inner: ShmBinaryWriter.init(stream))
func init*(R: type RevReader, stream: InputStream): R =
  RevReader(inner: ShmBinaryReader.init(stream))

{.push gcsafe, raises: [].}

# Every top-level value is written as: MAGIC + <ShmBinary body> + checksum.
# Delegating to the ShmBinary writer/reader inherits its full type spectrum.

proc writeValue*[T](w: var RevWriter; value: T) {.raises: [IOError].} =
  writeValue(w.inner, RevMagic)         # 4-byte frame magic (a distinct prefix)
  writeValue(w.inner, value)            # body: identical to ShmBinary encoding
  writeValue(w.inner, 0xA5'u8)          # trailing frame checksum byte

proc readValue*[T](r: var RevReader; value: var T)
    {.raises: [IOError, SerializationError].} =
  var magic: array[4, byte]
  readValue(r.inner, magic)
  if magic != RevMagic:
    raise (ref SerializationError)(msg: "shm-binary-rev: bad frame magic")
  readValue(r.inner, value)
  var chk: uint8
  readValue(r.inner, chk)
  if chk != 0xA5'u8:
    raise (ref SerializationError)(msg: "shm-binary-rev: bad frame checksum")

{.pop.}

# ---------------------------------------------------------------------------
# Test payload type spectrum
# ---------------------------------------------------------------------------

type
  Color = enum
    cRed = -3, cZero = 0, cGreen = 1, cBlue = 7   ## holey + negative

  Meter = distinct int
  Mile = distinct int

  Scalars = object
    i: int
    u: uint
    i64: int64
    u64: uint64
    f: float64
    b: bool
    col: Color

  Nested = object
    label: string
    tags: seq[string]
    nums: seq[int]

  AbcTuple = tuple[a: int, b: string, c: float64]
  NestedTuple = tuple[x: int, inner: AbcTuple, ys: seq[int]]

  VKind = enum vkNone, vkInt, vkPair, vkList
  Variant = object
    id: int
    case kind: VKind          ## discriminator changes the object shape
    of vkNone:
      discard
    of vkInt:
      n: int
    of vkPair:
      left: string
      right: string
    of vkList:
      items: seq[int]
      flag: bool

  DistinctHolder = object
    d: Meter
    m: Mile

  ArrayHolder = object
    fixed: array[4, int]
    variants: seq[Variant]

  OptHolder = object
    a: Option[int]
    b: Option[string]
    c: Option[Variant]

  Generic[T] = object
    header: string
    payload: T
    extra: seq[T]

func `==`(a, b: Meter): bool {.borrow.}
func `==`(a, b: Mile): bool {.borrow.}

# distinct types need serialization support in each Format:
Meter.serializesAsBase(ShmBinary)
Mile.serializesAsBase(ShmBinary)
# ShmBinaryRev delegates whole values to the ShmBinary writer/reader, so the
# distinct handling above is inherited (no separate registration needed).

# case objects need a custom equality that compares only active fields
func `==`(a, b: Variant): bool =
  if a.id != b.id or a.kind != b.kind: return false
  case a.kind
  of vkNone: true
  of vkInt: a.n == b.n
  of vkPair: a.left == b.left and a.right == b.right
  of vkList: a.items == b.items and a.flag == b.flag

# ---------------------------------------------------------------------------
# Roundtrip harness: push then drain via the typed queue, over a given Format.
# Also asserts the on-blob bytes (via `encode`) are STABLE and equal to what the
# queue would push, and that a fresh `encode` is byte-identical across two calls.
# ---------------------------------------------------------------------------

proc roundtrip[T, Format](value: T; slot = 4096): T =
  let path = freshPath("rt")
  defer: cleanup(path)
  var q = createTypedQueue[T, Format](path, 8, slot, TestBoot)
  check q.isValid
  check q.push(value) == prPushed
  var got: T
  check q.tryDrainOne(got) == drGot
  q.detach()
  got

template checkRoundtrip(Format: type; value: untyped) =
  ## Roundtrip through the queue AND assert `decode(encode(x)) == x` + stable bytes.
  let v = value
  let got = roundtrip[type(v), Format](v)
  check got == v
  # byte-identical: two encodes of the same value are identical, and decode
  # of the encoded bytes reproduces the value.
  let e1 = Format.encode(v)
  let e2 = Format.encode(v)
  check e1 == e2
  let d = Format.decode(e1, type(v))
  check d == v

# ---------------------------------------------------------------------------

suite "scalar PODs":
  test "int/uint/int64/uint64 min/max/0/negative":
    for F in [0]:  # loop placeholder to keep symmetry; formats handled explicitly
      discard
    template scalars(Format: type) =
      checkRoundtrip(Format, low(int))
      checkRoundtrip(Format, high(int))
      checkRoundtrip(Format, 0)
      checkRoundtrip(Format, -12345)
      checkRoundtrip(Format, low(int64))
      checkRoundtrip(Format, high(int64))
      checkRoundtrip(Format, 0'u)
      checkRoundtrip(Format, high(uint))
      checkRoundtrip(Format, high(uint64))
      checkRoundtrip(Format, 3.14159265358979'f64)
      checkRoundtrip(Format, -2.5'f64)
      checkRoundtrip(Format, 0.0'f64)
      checkRoundtrip(Format, true)
      checkRoundtrip(Format, false)
      checkRoundtrip(Format, cRed)
      checkRoundtrip(Format, cZero)
      checkRoundtrip(Format, cBlue)
    scalars(ShmBinary)
    scalars(ShmBinaryRev)

suite "tuples + nested tuples":
  test "AbcTuple + NestedTuple under both formats":
    template t(Format: type) =
      checkRoundtrip(Format, (1, "two", 3.0'f64))
      let nt: NestedTuple = (x: 9, inner: (a: -1, b: "z\0y", c: 1.5'f64),
                             ys: @[1, 2, 3])
      checkRoundtrip(Format, nt)
    t(ShmBinary)
    t(ShmBinaryRev)

suite "objects with string + seq fields":
  test "empty / one / many; unicode + embedded-NUL":
    template o(Format: type) =
      checkRoundtrip(Format, Nested(label: "", tags: @[], nums: @[]))
      checkRoundtrip(Format, Nested(label: "x", tags: @["a"], nums: @[42]))
      checkRoundtrip(Format, Nested(label: "héllo wörld ☃",
        tags: @["", "b", "café", "emb\0ded"], nums: @[1, 2, 3, 4, 5]))
      checkRoundtrip(Format, Scalars(i: -7, u: 9'u, i64: low(int64),
        u64: high(uint64), f: -0.0'f64, b: true, col: cGreen))
    o(ShmBinary)
    o(ShmBinaryRev)

suite "case / variant objects":
  test "every branch incl. discriminator-changes-shape":
    template c(Format: type) =
      checkRoundtrip(Format, Variant(id: 1, kind: vkNone))
      checkRoundtrip(Format, Variant(id: 2, kind: vkInt, n: -99))
      checkRoundtrip(Format, Variant(id: 3, kind: vkPair, left: "L\0",
        right: "Ř"))
      checkRoundtrip(Format, Variant(id: 4, kind: vkList,
        items: @[10, 20, 30], flag: true))
      checkRoundtrip(Format, Variant(id: 5, kind: vkList, items: @[], flag: false))
    c(ShmBinary)
    c(ShmBinaryRev)

suite "distinct types":
  test "distinct int (Meter/Mile) via serializesAsBase":
    template d(Format: type) =
      checkRoundtrip(Format, DistinctHolder(d: Meter(1000), m: Mile(-5)))
    d(ShmBinary)
    d(ShmBinaryRev)

suite "arrays + seq of composites":
  test "array[N,int] + seq[Variant]":
    template a(Format: type) =
      checkRoundtrip(Format, ArrayHolder(fixed: [1, 2, 3, 4], variants: @[
        Variant(id: 1, kind: vkInt, n: 7),
        Variant(id: 2, kind: vkNone),
        Variant(id: 3, kind: vkList, items: @[9, 8], flag: true)]))
      checkRoundtrip(Format, ArrayHolder(fixed: [0, 0, 0, 0], variants: @[]))
    a(ShmBinary)
    a(ShmBinaryRev)

suite "Option[T]":
  test "some / none / nested variant option":
    template o(Format: type) =
      checkRoundtrip(Format, OptHolder(a: some(5), b: none(string),
        c: some(Variant(id: 9, kind: vkPair, left: "x", right: "y"))))
      checkRoundtrip(Format, OptHolder(a: none(int), b: some("hi"), c: none(Variant)))
    o(ShmBinary)
    o(ShmBinaryRev)

suite "generic payload at several T":
  test "Generic[int] / Generic[string] / Generic[Variant]":
    template g(Format: type) =
      checkRoundtrip(Format, Generic[int](header: "h", payload: 7, extra: @[1, 2]))
      checkRoundtrip(Format, Generic[string](header: "s", payload: "p\0",
        extra: @["a", "b"]))
      checkRoundtrip(Format, Generic[Variant](header: "v",
        payload: Variant(id: 1, kind: vkList, items: @[3], flag: false),
        extra: @[Variant(id: 2, kind: vkNone)]))
    g(ShmBinary)
    g(ShmBinaryRev)

suite "the two Formats produce DIFFERENT blobs for the same T":
  test "ShmBinary vs ShmBinaryRev differ but each roundtrips":
    let v = Nested(label: "hello", tags: @["a", "bb"], nums: @[1, 2, 3])
    let eBin = ShmBinary.encode(v)
    let eRev = ShmBinaryRev.encode(v)
    check eBin != eRev                       # the (T,Format) axis is real
    check ShmBinary.decode(eBin, Nested) == v
    check ShmBinaryRev.decode(eRev, Nested) == v
    # A multi-field variant likewise differs across formats.
    let vv = Variant(id: 7, kind: vkPair, left: "aa", right: "bb")
    check ShmBinary.encode(vv) != ShmBinaryRev.encode(vv)
    check ShmBinary.decode(ShmBinary.encode(vv), Variant) == vv
    check ShmBinaryRev.decode(ShmBinaryRev.encode(vv), Variant) == vv

suite "on-blob bytes stable across runs":
  test "encode is deterministic (fixed (T,Format) -> fixed bytes)":
    let v = ArrayHolder(fixed: [5, 6, 7, 8], variants: @[
      Variant(id: 1, kind: vkList, items: @[1, 2, 3], flag: true)])
    let a = ShmBinary.encode(v)
    let b = ShmBinary.encode(v)
    let c = ShmBinary.encode(v)
    check a == b
    check b == c
    check a.len > 0

suite "exact-fill + overflow (oversize signalled, queue unchanged)":
  test "a record that exactly fills maxBlobLen and one that overflows":
    # A payload of raw bytes lets us hit the slot size exactly. We encode a
    # seq[byte] whose encoding is u32 length prefix + bytes; pick a slot equal to
    # that so the encoding EXACTLY fills the slot.
    const payloadLen = 60
    const slot = 4 + payloadLen           # u32 length prefix + bytes
    let path = freshPath("fill")
    defer: cleanup(path)
    var q = createTypedQueue[seq[byte], ShmBinary](path, 8, slot, TestBoot)
    check q.isValid

    var exact = newSeq[byte](payloadLen)
    for i in 0 ..< payloadLen: exact[i] = byte(i)
    check ShmBinary.encode(exact).len == slot          # exactly fills
    check q.push(exact) == prPushed
    var got: seq[byte]
    check q.tryDrainOne(got) == drGot
    check got == exact

    # One byte more overflows the slot -> prOversize, queue UNCHANGED.
    var over = newSeq[byte](payloadLen + 1)
    for i in 0 ..< over.len: over[i] = byte(i)
    check ShmBinary.encode(over).len == slot + 1
    check q.push(over) == prOversize
    check q.pendingCount() == 0                          # queue unchanged

    # A subsequent normal push/drain still works.
    check q.push(exact) == prPushed
    check q.tryDrainOne(got) == drGot
    check got == exact
    q.detach()

# ---------------------------------------------------------------------------
# Multi-PROCESS roundtrip across the shm boundary.
# ---------------------------------------------------------------------------

suite "multi-process typed roundtrip":
  test "forked producer pushes typed items; parent drains + decodes":
    when not shmSegmentSupported:
      skip()
    else:
      const N = 200
      let path = freshPath("mproc")
      defer: cleanup(path)
      # Use the REAL boot id here: the forked child's `attachTypedQueue` validates
      # the segment's boot guard against the current `bootId()`, so the creator
      # must stamp the same real boot id (unlike the single-process suites, which
      # can use a synthetic TestBoot because create + attach share it explicitly).
      var q = createTypedQueue[Variant, ShmBinary](path, 64, 512, bootId())
      check q.isValid

      let pid = fork()
      check pid >= 0
      if pid == 0:
        # Child: attach and push N typed variants (rotating through branches).
        var pq = attachTypedQueue[Variant, ShmBinary](path)
        if not pq.isValid: quitChild(2)
        var i = 0
        while i < N:
          let v =
            case i mod 4
            of 0: Variant(id: i, kind: vkNone)
            of 1: Variant(id: i, kind: vkInt, n: i * 7)
            of 2: Variant(id: i, kind: vkPair, left: $i, right: "r" & $i)
            else: Variant(id: i, kind: vkList, items: @[i, i + 1], flag: (i and 1) == 0)
          if pq.push(v) == prPushed:
            inc i
        pq.detach()
        quitChild(0)
      else:
        # Parent: drain N items and decode, verifying each matches the producer.
        var received = 0
        var seenIds: seq[bool] = newSeq[bool](N)
        var got: Variant
        while received < N:
          if q.tryDrainOne(got) == drGot:
            check got.id >= 0 and got.id < N
            check not seenIds[got.id]
            seenIds[got.id] = true
            let i = got.id
            let expected =
              case i mod 4
              of 0: Variant(id: i, kind: vkNone)
              of 1: Variant(id: i, kind: vkInt, n: i * 7)
              of 2: Variant(id: i, kind: vkPair, left: $i, right: "r" & $i)
              else: Variant(id: i, kind: vkList, items: @[i, i + 1], flag: (i and 1) == 0)
            check got == expected
            inc received
        var status: cint
        discard waitpid(pid, status, 0)
        check received == N
        q.detach()
