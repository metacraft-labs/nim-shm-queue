## A compact positional binary `serialization` format used by Layer 2's
## `TypedShmQueue` by default, so a consumer gets a working `(T, binaryFormat)`
## out of the box. The `(T, Format)` axis stays open: a consumer may supply any
## other `serialization` `Format` (e.g. json-serialization's `Json`) instead.
##
## This is a real `serialization` `SerializationFormat` (via `serializationFormat`
## + `setReader`/`setWriter`), so `encode(ShmBinary, x)` / `decode(ShmBinary, blob, T)`
## and the generic `writeValue`/`readValue` protocol from nim-serialization apply.
## It is *positional*: fields are written in declaration order with no field
## names or tags, so the encoding is compact and stable, but reader and writer
## must agree on the type (which the `(T, Format)` pair guarantees).
##
## Wire layout (little-endian, self-contained — no external framing):
##   * unsigned integers  -> fixed-width LE (uint8/16/32/64 by the Nim width)
##   * signed integers    -> two's-complement LE at the Nim width
##   * bool               -> 1 byte (0/1)
##   * char               -> 1 byte
##   * float32/float64    -> IEEE-754 bits as the matching-width uint, LE
##   * enum               -> its ordinal as an LE int64 (covers holey/negative)
##   * string / seq[byte] -> u32 length prefix + raw bytes (embedded NULs OK)
##   * seq[T]             -> u32 count + each element
##   * array[N, T]        -> N elements, no count (N is static)
##   * tuple / object     -> each serialized field in order (case objects:
##                           discriminator first, then the active branch fields)
##   * Option[T]          -> 1 byte tag (0 = none, 1 = some) + value if some
##   * distinct T         -> as its base type
##   * ref T              -> 1 byte tag (0 = nil, 1 = present) + value if present
##
## Case/variant objects: `writeValue` uses `enumInstanceSerializedFields`, which
## visits the discriminator first and then ONLY the currently active branch
## fields. `readValue` reads the discriminator, constructs the object with it,
## then reads the now-active fields — so a discriminator that changes the object
## shape roundtrips exactly.

import std/[typetraits, options, macros]
import serialization
import faststreams/[inputs, outputs]
export serialization

serializationFormat ShmBinary,
  mimeType = "application/x-shm-queue-binary"

type
  ShmBinaryWriter* = object
    stream: OutputStream

  ShmBinaryReader* = object
    stream: InputStream

ShmBinary.setReader ShmBinaryReader
ShmBinary.setWriter ShmBinaryWriter, PreferredOutput = seq[byte]

func init*(W: type ShmBinaryWriter, stream: OutputStream): W =
  ShmBinaryWriter(stream: stream)

func init*(R: type ShmBinaryReader, stream: InputStream): R =
  ShmBinaryReader(stream: stream)

{.push gcsafe, raises: [].}

# --- low-level fixed-width primitives ---------------------------------------

proc writeRaw(w: var ShmBinaryWriter; p: pointer; n: int) {.raises: [IOError].} =
  if n > 0:
    let ua = cast[ptr UncheckedArray[byte]](p)
    w.stream.write(ua.toOpenArray(0, n - 1))

template writeLE(w: var ShmBinaryWriter; v: SomeInteger) =
  ## Write an unsigned/signed integer as fixed-width little-endian, matching the
  ## Nim type's byte width. Two's-complement for signed types.
  var tmp = v
  when cpuEndian == bigEndian:
    when sizeof(v) == 2: tmp = cast[type(v)](swapBytes(cast[uint16](v)))
    elif sizeof(v) == 4: tmp = cast[type(v)](swapBytes(cast[uint32](v)))
    elif sizeof(v) == 8: tmp = cast[type(v)](swapBytes(cast[uint64](v)))
  w.writeRaw(addr tmp, sizeof(tmp))

proc readRaw(r: var ShmBinaryReader; p: pointer; n: int)
    {.raises: [IOError, SerializationError].} =
  if n == 0: return
  let ua = cast[ptr UncheckedArray[byte]](p)
  if not r.stream.readInto(ua.toOpenArray(0, n - 1)):
    raise (ref SerializationError)(msg: "shm-binary: truncated input")

template readLE(r: var ShmBinaryReader; T: type): untyped =
  block:
    var tmp: T
    r.readRaw(addr tmp, sizeof(tmp))
    when cpuEndian == bigEndian:
      when sizeof(T) == 2: tmp = cast[T](swapBytes(cast[uint16](tmp)))
      elif sizeof(T) == 4: tmp = cast[T](swapBytes(cast[uint32](tmp)))
      elif sizeof(T) == 8: tmp = cast[T](swapBytes(cast[uint64](tmp)))
    tmp

proc writeLen(w: var ShmBinaryWriter; n: int) {.raises: [IOError].} =
  w.writeLE(uint32(n))

proc readLen(r: var ShmBinaryReader): int
    {.raises: [IOError, SerializationError].} =
  int(readLE(r, uint32))

# --- scalar writeValue/readValue --------------------------------------------

proc writeValue*(w: var ShmBinaryWriter; value: bool) {.raises: [IOError].} =
  var b = byte(ord(value))
  w.writeRaw(addr b, 1)

proc readValue*(r: var ShmBinaryReader; value: var bool)
    {.raises: [IOError, SerializationError].} =
  var b: byte
  r.readRaw(addr b, 1)
  value = b != 0

proc writeValue*(w: var ShmBinaryWriter; value: char) {.raises: [IOError].} =
  var b = byte(value)
  w.writeRaw(addr b, 1)

proc readValue*(r: var ShmBinaryReader; value: var char)
    {.raises: [IOError, SerializationError].} =
  var b: byte
  r.readRaw(addr b, 1)
  value = char(b)

proc writeValue*(w: var ShmBinaryWriter; value: SomeUnsignedInt)
    {.raises: [IOError].} =
  w.writeLE(value)

proc readValue*(r: var ShmBinaryReader; value: var SomeUnsignedInt)
    {.raises: [IOError, SerializationError].} =
  value = readLE(r, type(value))

proc writeValue*(w: var ShmBinaryWriter; value: SomeSignedInt)
    {.raises: [IOError].} =
  w.writeLE(value)

proc readValue*(r: var ShmBinaryReader; value: var SomeSignedInt)
    {.raises: [IOError, SerializationError].} =
  value = readLE(r, type(value))

proc writeValue*(w: var ShmBinaryWriter; value: float32) {.raises: [IOError].} =
  w.writeLE(cast[uint32](value))

proc readValue*(r: var ShmBinaryReader; value: var float32)
    {.raises: [IOError, SerializationError].} =
  value = cast[float32](readLE(r, uint32))

proc writeValue*(w: var ShmBinaryWriter; value: float64) {.raises: [IOError].} =
  w.writeLE(cast[uint64](value))

proc readValue*(r: var ShmBinaryReader; value: var float64)
    {.raises: [IOError, SerializationError].} =
  value = cast[float64](readLE(r, uint64))

proc writeValue*(w: var ShmBinaryWriter; value: enum) {.raises: [IOError].} =
  ## Ordinal as an LE int64 — covers negative and holey enums.
  w.writeLE(int64(ord(value)))

proc readValue*[T: enum](r: var ShmBinaryReader; value: var T)
    {.raises: [IOError, SerializationError].} =
  value = T(readLE(r, int64))

# --- string / seq[byte] -----------------------------------------------------

proc writeValue*(w: var ShmBinaryWriter; value: string) {.raises: [IOError].} =
  w.writeLen(value.len)
  if value.len > 0:
    w.writeRaw(unsafeAddr value[0], value.len)

proc readValue*(r: var ShmBinaryReader; value: var string)
    {.raises: [IOError, SerializationError].} =
  let n = r.readLen()
  value.setLen(n)
  if n > 0:
    r.readRaw(addr value[0], n)

# --- case object reader (needs a per-type generated reader) -----------------
#
# The generic `enumInstanceSerializedFields` reader cannot construct a case
# object because assigning a discriminator would change the object shape
# mid-iteration. `readCaseObject` reads every field in DECLARATION ORDER: leading
# plain fields + all top-level discriminators are read into temporaries and fed
# to an object constructor (establishing the active branch), then the remaining
# active branch fields are read in order (skipping the already-read leading /
# discriminator fields). This preserves the writer's positional order exactly,
# including the discriminator-changes-shape case.

proc idDefNames(idefs: NimNode): seq[string] {.compileTime.} =
  for i in 0 ..< idefs.len - 2:
    var n = idefs[i]
    if n.kind == nnkPostfix: n = n[1]
    if n.kind == nnkPragmaExpr: n = n[0]
    if n.kind == nnkPostfix: n = n[1]
    result.add $n

macro readCaseObject*(r: var ShmBinaryReader; value: typed): untyped =
  var recList = getImpl(value.getTypeInst)[2]
  while recList.kind in {nnkRefTy, nnkPtrTy}:
    recList = getImpl(recList[0])[2]
  doAssert recList.kind == nnkObjectTy, "readCaseObject expects an object type"
  let fields = recList[2]

  var
    preReads = newStmtList()          # read leading-plain + discriminator temps
    constr = nnkObjConstr.newTree(value.getTypeInst)
    preReadNames: seq[string]         # field names already consumed in preReads

  # Walk top-level record fields in declaration order. Everything up to and
  # including the (single) case discriminator is read positionally into temps and
  # supplied to the constructor. (Multiple/nested top-level cases are rare; a
  # leading plain field after a case is handled by the post pass below.)
  var sawCase = false
  for node in fields:
    case node.kind
    of nnkIdentDefs:
      if sawCase: continue           # trailing plain fields -> post pass
      let ftype = node[^2]
      for nm in idDefNames(node):
        let tmp = genSym(nskVar, "pre")
        preReads.add quote do:
          var `tmp`: `ftype`
          readValue(`r`, `tmp`)
        constr.add nnkExprColonExpr.newTree(ident(nm), tmp)
        preReadNames.add nm
    of nnkRecCase:
      sawCase = true
      let discDefs = node[0]
      let discType = discDefs[^2]
      for nm in idDefNames(discDefs):
        let tmp = genSym(nskVar, "disc")
        preReads.add quote do:
          var `tmp`: `discType`
          readValue(`r`, `tmp`)
        constr.add nnkExprColonExpr.newTree(ident(nm), tmp)
        preReadNames.add nm
    else: discard

  let skipLit = newLit(preReadNames)
  result = newStmtList()
  result.add preReads
  result.add newAssignment(value, constr)
  result.add quote do:
    enumInstanceSerializedFields(`value`, fieldName, field):
      when fieldName notin `skipLit`:
        readValue(`r`, field)

# --- generic composite writeValue/readValue ---------------------------------

proc writeValue*[T](w: var ShmBinaryWriter; value: seq[T]) {.raises: [IOError].}
proc readValue*[T](r: var ShmBinaryReader; value: var seq[T])
    {.raises: [IOError, SerializationError].}
proc writeValue*[N, T](w: var ShmBinaryWriter; value: array[N, T])
    {.raises: [IOError].}
proc readValue*[N, T](r: var ShmBinaryReader; value: var array[N, T])
    {.raises: [IOError, SerializationError].}
proc writeValue*[T](w: var ShmBinaryWriter; value: Option[T]) {.raises: [IOError].}
proc readValue*[T](r: var ShmBinaryReader; value: var Option[T])
    {.raises: [IOError, SerializationError].}
proc writeValue*[T: tuple|object](w: var ShmBinaryWriter; value: T)
    {.raises: [IOError].}
proc readValue*[T: tuple|object](r: var ShmBinaryReader; value: var T)
    {.raises: [IOError, SerializationError].}
proc writeValue*[T](w: var ShmBinaryWriter; value: ref T) {.raises: [IOError].}
proc readValue*[T](r: var ShmBinaryReader; value: var ref T)
    {.raises: [IOError, SerializationError].}

proc writeValue*[T](w: var ShmBinaryWriter; value: seq[T]) =
  when T is byte:
    w.writeLen(value.len)
    if value.len > 0:
      w.writeRaw(unsafeAddr value[0], value.len)
  else:
    w.writeLen(value.len)
    for i in 0 ..< value.len:
      writeValue(w, value[i])

proc readValue*[T](r: var ShmBinaryReader; value: var seq[T]) =
  let n = r.readLen()
  value.setLen(n)
  when T is byte:
    if n > 0:
      r.readRaw(addr value[0], n)
  else:
    for i in 0 ..< n:
      readValue(r, value[i])

proc writeValue*[N, T](w: var ShmBinaryWriter; value: array[N, T]) =
  for i in low(value) .. high(value):
    writeValue(w, value[i])

proc readValue*[N, T](r: var ShmBinaryReader; value: var array[N, T]) =
  for i in low(value) .. high(value):
    readValue(r, value[i])

proc writeValue*[T](w: var ShmBinaryWriter; value: Option[T]) =
  if value.isSome:
    var tag = 1'u8
    w.writeRaw(addr tag, 1)
    writeValue(w, value.get)
  else:
    var tag = 0'u8
    w.writeRaw(addr tag, 1)

proc readValue*[T](r: var ShmBinaryReader; value: var Option[T]) =
  var tag: byte
  r.readRaw(addr tag, 1)
  if tag == 0:
    value = none(T)
  else:
    var v: T
    readValue(r, v)
    value = some(v)

proc writeValue*[T](w: var ShmBinaryWriter; value: ref T) =
  if value.isNil:
    var tag = 0'u8
    w.writeRaw(addr tag, 1)
  else:
    var tag = 1'u8
    w.writeRaw(addr tag, 1)
    writeValue(w, value[])

proc readValue*[T](r: var ShmBinaryReader; value: var ref T) =
  var tag: byte
  r.readRaw(addr tag, 1)
  if tag == 0:
    value = nil
  else:
    value = new(T)
    readValue(r, value[])

# --- tuples + objects (incl. case/variant objects) --------------------------

proc writeValue*[T: tuple|object](w: var ShmBinaryWriter; value: T) =
  # `enumInstanceSerializedFields` visits the discriminator first, then only the
  # currently-active branch fields (for case objects), in declaration order.
  enumInstanceSerializedFields(value, fieldName, field):
    writeValue(w, field)

proc readValue*[T: tuple|object](r: var ShmBinaryReader; value: var T) =
  when T is tuple:
    enumInstanceSerializedFields(value, fieldName, field):
      readValue(r, field)
  else:
    when isCaseObject(T):
      readCaseObject(r, value)
    else:
      enumInstanceSerializedFields(value, fieldName, field):
        readValue(r, field)

{.pop.}
