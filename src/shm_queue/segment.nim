## Versioned, boot-guarded shared-memory segment for the Layer-1 byte-blob ring.
##
## A `ShmSegment` owns a fixed-size file mapped `PROT_READ|PROT_WRITE`,
## MAP_SHARED, so every process that maps the same path observes the same bytes.
## The segment is created via a unique temp file + atomic `rename()` (so a
## concurrent attacher never sees a half-initialised file) and carries a header
## that is validated on attach:
##
##   * `magic` — a fixed tag identifying an shm_queue ring segment.
##   * `formatVersion` — bumped on any wire-layout change; attach fails loudly
##     (returns an invalid segment) on a mismatch.
##   * `creatorBootId` — a per-boot identity. A segment file left over from a
##     previous boot has a stale `creatorBootId`; attach rejects it so a caller
##     never trusts pre-reboot shared memory (the caller recreates instead).
##   * `capacity` + `maxBlobLen` — the ring geometry, stored in the segment so an
##     attacher never has to agree with the creator out of band. `attachRing`
##     reads them back to size its slot arithmetic.
##
## This module is the same shape as `repro_shm_index/segment.nim` +
## `repro_shm_index/mapping.nim` and `io-mon/.../dep_queue.nim`'s segment header,
## generalised so `capacity` and `maxBlobLen` are RUNTIME parameters (stored in
## the header) rather than compile-time constants.
##
## Platform: Linux + macOS (POSIX mmap MAP_SHARED). `shmSegmentSupported` is
## false elsewhere; every op then reports an invalid segment so a caller can
## fall back.

const shmSegmentSupported* = defined(linux) or defined(macosx)

# --- alignment helpers (const-context usable) ------------------------------

func align8*(n: int): int = (n + 7) and not 7
func align4k*(n: int): int = (n + 4095) and not 4095

const
  ShmRingMagic* = 0x5147_4D48_53_00_01'u64
    ## Fixed magic identifying an shm_queue Layer-1 ring segment.
  ShmRingFormatVersion* = 1'u32
    ## Bumped on any change to the on-segment layout below.

# --- fixed segment-header byte layout (offset-only, base-independent) -------
#
# All 8-byte fields sit on 8-byte-aligned offsets so the atomic accesses are
# well-defined on every process's mapping. The header is followed by the ring
# header + the fixed slot array (laid out by ring.nim from `capacity` /
# `maxBlobLen`, which are stored here).
const
  HdrOffMagic* = 0                                # u64
  HdrOffFormatVersion* = HdrOffMagic + 8          # u32
  HdrOffFlags* = HdrOffFormatVersion + 4          # u32 (reserved)
  HdrOffCreatorBootId* = HdrOffFlags + 4          # u64
  HdrOffCapacity* = HdrOffCreatorBootId + 8       # u64
  HdrOffMaxBlobLen* = HdrOffCapacity + 8          # u64
  HdrOffHead* = HdrOffMaxBlobLen + 8              # u64 (consumer-owned)
  HdrOffTail* = HdrOffHead + 8                    # u64 (producers CAS)
  HdrOffDropped* = HdrOffTail + 8                 # u64 (drop-on-full count)
  SegHeaderSize* = align8(HdrOffDropped + 8)

# One slot: {ready(u64 publication ticket), blobLen(u32), pad(u32),
#            blobBytes[maxBlobLen]}. `ready == 0` means never-written;
# `ready == ticket+1` means the blob for `ticket` is published.
const
  SlotOffReady* = 0                  # u64 atomic
  SlotOffBlobLen* = SlotOffReady + 8 # u32
  SlotOffPad* = SlotOffBlobLen + 4   # u32 pad
  SlotOffBlob* = SlotOffPad + 4      # byte[maxBlobLen]

func slotStrideFor*(maxBlobLen: int): int {.inline.} =
  ## Byte stride of one ring slot for a given `maxBlobLen`.
  align8(SlotOffBlob + maxBlobLen)

func segRegionSize*(capacity, maxBlobLen: int): int {.inline.} =
  ## Page-rounded byte size of a ring segment with `capacity` slots.
  align4k(SegHeaderSize + capacity * slotStrideFor(maxBlobLen))

when shmSegmentSupported:
  import std/[os, posix, times]

  type
    ShmBase* = ptr UncheckedArray[byte]
      ## The mapped base of the shared region; offsets are relative to this.

    ShmSegment* = object
      ## An attached view of a ring segment. `base` is nil / `size` is 0 on any
      ## failure; `isValid` reports whether the mapping is live.
      base*: ShmBase
      size*: int
      fd*: cint
      path*: string
      capacity*: int
      maxBlobLen*: int

  proc bootId*(): uint64 =
    ## Per-boot identity used to invalidate a stale post-reboot segment. Same
    ## shape as `repro_shm_index.bootId` / io-mon's `bootId`. Never returns zero.
    when defined(linux):
      try:
        let raw = readFile("/proc/sys/kernel/random/boot_id")
        var h: uint64 = 1469598103934665603'u64
        for ch in raw:
          if ch != '-' and ch != '\n':
            h = (h xor uint64(ord(ch))) * 1099511628211'u64
        return (h or 1'u64)
      except CatchableError:
        discard
    let secs = uint64(epochTime().int64)
    (secs or 1'u64)

  proc isValid*(s: ShmSegment): bool {.inline.} =
    not s.base.isNil and s.size > 0

  # --- offset-addressed atomics (C11/GCC builtins, no process-shared mutex) ---
  template atField[T](base: ShmBase; offset: int): ptr T =
    cast[ptr T](addr base[offset])

  proc loadU64Acquire*(base: ShmBase; off: int): uint64 {.inline.} =
    atomicLoadN(atField[uint64](base, off), ATOMIC_ACQUIRE)
  proc loadU64Relaxed*(base: ShmBase; off: int): uint64 {.inline.} =
    atomicLoadN(atField[uint64](base, off), ATOMIC_RELAXED)
  proc storeU64Release*(base: ShmBase; off: int; v: uint64) {.inline.} =
    atomicStoreN(atField[uint64](base, off), v, ATOMIC_RELEASE)
  proc storeU64Relaxed*(base: ShmBase; off: int; v: uint64) {.inline.} =
    atomicStoreN(atField[uint64](base, off), v, ATOMIC_RELAXED)
  proc casU64*(base: ShmBase; off: int; expected: var uint64;
      desired: uint64): bool {.inline.} =
    atomicCompareExchangeN(atField[uint64](base, off), addr expected, desired,
      false, ATOMIC_ACQ_REL, ATOMIC_ACQUIRE)
  proc fetchAddU64*(base: ShmBase; off: int; delta: uint64): uint64 {.inline.} =
    atomicAddFetch(atField[uint64](base, off), delta, ATOMIC_SEQ_CST)
  proc loadU32Acquire*(base: ShmBase; off: int): uint32 {.inline.} =
    atomicLoadN(atField[uint32](base, off), ATOMIC_ACQUIRE)
  proc storeU32Release*(base: ShmBase; off: int; v: uint32) {.inline.} =
    atomicStoreN(atField[uint32](base, off), v, ATOMIC_RELEASE)

  proc mapFd(fd: cint; size: int): ShmBase =
    let p = mmap(nil, size, PROT_READ or PROT_WRITE, MAP_SHARED, fd, 0)
    if p == MAP_FAILED:
      return nil
    cast[ShmBase](p)

  proc headerLooksValid(base: ShmBase; expectBoot: uint64): bool =
    loadU64Acquire(base, HdrOffMagic) == ShmRingMagic and
      loadU32Acquire(base, HdrOffFormatVersion) == ShmRingFormatVersion and
      loadU64Relaxed(base, HdrOffCreatorBootId) == expectBoot

  proc initHeader(base: ShmBase; capacity, maxBlobLen: int; boot: uint64) =
    storeU64Relaxed(base, HdrOffHead, 0)
    storeU64Relaxed(base, HdrOffTail, 0)
    storeU64Relaxed(base, HdrOffDropped, 0)
    storeU32Release(base, HdrOffFormatVersion, ShmRingFormatVersion)
    storeU64Relaxed(base, HdrOffFlags, 0)
    storeU64Relaxed(base, HdrOffCapacity, uint64(capacity))
    storeU64Relaxed(base, HdrOffMaxBlobLen, uint64(maxBlobLen))
    storeU64Relaxed(base, HdrOffCreatorBootId, boot)
    # Publish the magic LAST (release) so a concurrent attacher that observes
    # the magic also observes the zeroed ring header + geometry fields.
    storeU64Release(base, HdrOffMagic, ShmRingMagic)

  proc createSegment*(path: string; capacity, maxBlobLen: int;
      boot: uint64): ShmSegment =
    ## CONSUMER/owner side: create + map a fresh, zero-filled segment. The file
    ## is written to a unique temp path then `rename()`d into place so a
    ## concurrent attacher never sees a half-initialised file.
    result.base = nil
    result.size = 0
    result.fd = -1
    result.path = path
    result.capacity = capacity
    result.maxBlobLen = maxBlobLen
    let size = segRegionSize(capacity, maxBlobLen)
    try:
      let dir = parentDir(path)
      if dir.len > 0:
        createDir(dir)
    except CatchableError:
      return
    let uniq = int(epochTime() * 1_000_000) mod 1_000_000
    let tmp = path & ".tmp." & $getpid() & "." & $uniq
    let tfd = open(tmp.cstring, O_RDWR or O_CREAT or O_EXCL, 0o600)
    if tfd < 0:
      return
    if ftruncate(tfd, Off(size)) != 0:
      discard close(tfd); removeFile(tmp); return
    discard close(tfd)
    try:
      moveFile(tmp, path)
    except OSError:
      removeFile(tmp); return
    let fd = open(path.cstring, O_RDWR)
    if fd < 0:
      return
    let p = mapFd(fd, size)
    if p.isNil:
      discard close(fd); return
    result.fd = fd
    result.base = p
    result.size = size
    initHeader(p, capacity, maxBlobLen, boot)

  proc attachSegment*(path: string): ShmSegment =
    ## PRODUCER/consumer side: attach to an existing segment. Returns an invalid
    ## segment (so the caller can fall back / recreate) when the file is missing,
    ## the wrong size, or the header is stale (wrong magic / formatVersion /
    ## creatorBootId). Never CREATES — a producer must not race a fresh init.
    ##
    ## The mapping size is derived from the on-disk file size so an attacher
    ## never has to know the geometry up front; the header's `capacity` /
    ## `maxBlobLen` are read back into the returned segment.
    result.base = nil
    result.size = 0
    result.fd = -1
    result.path = path
    if not fileExists(path):
      return
    var size = 0
    try:
      size = int(getFileSize(path))
    except CatchableError:
      return
    if size <= SegHeaderSize:
      return
    let fd = open(path.cstring, O_RDWR)
    if fd < 0:
      return
    let p = mapFd(fd, size)
    if p.isNil:
      discard close(fd); return
    if not headerLooksValid(p, bootId()):
      discard munmap(cast[pointer](p), size)
      discard close(fd)
      return
    result.fd = fd
    result.base = p
    result.size = size
    result.capacity = int(loadU64Relaxed(p, HdrOffCapacity))
    result.maxBlobLen = int(loadU64Relaxed(p, HdrOffMaxBlobLen))

  proc detach*(s: var ShmSegment) =
    ## Unmap + close. Does NOT unlink the backing file (other processes may still
    ## hold the segment). A default-constructed segment (base=nil) is a no-op;
    ## fd 0 is never closed.
    if not s.base.isNil:
      discard munmap(cast[pointer](s.base), s.size)
      s.base = nil
      if s.fd > 0:
        discard close(s.fd)
    s.fd = -1
    s.size = 0

else:
  type
    ShmBase* = ptr UncheckedArray[byte]
    ShmSegment* = object
      base*: ShmBase
      size*: int
      path*: string
      capacity*: int
      maxBlobLen*: int

  proc bootId*(): uint64 = 1'u64
  proc isValid*(s: ShmSegment): bool = false
  proc createSegment*(path: string; capacity, maxBlobLen: int;
      boot: uint64): ShmSegment =
    ShmSegment(path: path, capacity: capacity, maxBlobLen: maxBlobLen)
  proc attachSegment*(path: string): ShmSegment =
    ShmSegment(path: path)
  proc detach*(s: var ShmSegment) = discard
