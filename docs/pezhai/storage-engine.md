# Pezhai Storage Engine Specification

This document is the normative specification for `Pezhai`, the storage engine of `kalanjiyam`.
It defines the local-storage contract that future `pezhai` implementations MUST follow.

Normative keywords:

- `MUST`: required for correctness or compatibility
- `MUST NOT`: forbidden
- `SHOULD`: strongly recommended unless there is a clear reason not to
- `MAY`: optional

All text in this repository is ASCII-only by rule.

Source of truth:

- byte-layout tables and exact field rules are authoritative for durable bytes
- pseudocode blocks labeled `Normative pseudocode` are authoritative for algorithmic behavior,
  validation order, and state transitions
- prose examples, diagrams, and timelines remain explanatory unless a subsection explicitly says
  `Behavioral clarification`

Reading guide:

- Sections 1 through 6 define the conceptual model, invariants, and state
- Sections 7 and 8 define wire encodings and durable on-disk layout
- Sections 9 through 12 define public behavior, maintenance behavior, and crash semantics
- Sections 13 and 14 define the acceptance criteria, required tests, and performance guidance

Naming rules:

- `Pezhai` is the canonical prose name of the storage engine defined by this document
- future implementation-facing names for this storage-engine subsystem SHOULD use `pezhai` for
  code, module, file, and directory identifiers
- compatibility-sensitive public and durable identifiers such as `[engine]`,
  `engine.sync_mode`, `engine_instance_id`, and `open_engine` remain unchanged unless a later
  specification revision explicitly changes them

## 1. Problem Restatement

### 1.1 What is being built

Build `Pezhai`, a single-node storage engine for a future distributed, ACID-compliant key-value
store. [BEHAVIORAL]

Pezhai owns one shared levelled LSM tree for the full keyspace. [BEHAVIORAL] Immutable data files
are stored in `store/data/*.kjm`. [BEHAVIORAL] Logical shards are latest-only metadata entries that
partition the keyspace into ordered half-open ranges and track current `live_size_bytes` metadata
for each range. [BEHAVIORAL]

### 1.2 Why it exists

Pezhai exists to provide a precise local-storage foundation before adding distributed concerns such
as replication, consensus, or cross-node movement.

The core design goals are:

- deterministic write visibility and crash recovery [BEHAVIORAL]
- current logical-shard size tracking without putting the write path behind mandatory read-before-
  write lookups [PERF]
- metadata-only local split and merge of logical shards [BEHAVIORAL]
- immutable shared data files with background maintenance [BEHAVIORAL]
- stable byte-compatible durable formats across languages [BEHAVIORAL]

### 1.3 What success looks like

Success means:

- the public operations `open`, `create_snapshot`, `release_snapshot`, `put`, `delete`, `get`,
  `scan`, `sync`, and `stats` have fully specified externally visible behavior [BEHAVIORAL]
- the on-disk layout is defined at byte level for `CURRENT`, WAL segments, metadata checkpoints,
  and shared data `.kjm` files [BEHAVIORAL]
- `config.toml` is parsed semantically as durable operator input rather than as a byte-for-byte
  compatibility artifact [BEHAVIORAL]
- snapshot visibility is unambiguous and does not depend on logical-shard history [BEHAVIORAL]
- logical shard split and merge remain metadata-only [BEHAVIORAL]
- stale background data publishes (flush, compaction, logical split/merge) are rejected by current-generation or source-range preconditions [MERGED with #11.5] [BEHAVIORAL]

Worked example:

```text
Current shared data state:
  active memtable + frozen memtables + published data files

Current logical shard map:
  [-inf, "m")  -> live_size_bytes = 900 MiB
  ["m", +inf)  -> live_size_bytes = 1300 MiB

put("yak", "v2"):
  1. appends one Put WAL record
  2. inserts one record into the shared active memtable
  3. leaves logical-shard byte refresh to later maintenance

The read path still probes the shared data plane only.
No data file, memtable, or compaction state belongs to one logical shard.
```

ASCII diagram:

```text
                +-------------------------------+
                | shared data plane             |
                | active memtable               |
user mutation ->| frozen memtables             |-> flush/compaction -> data files
                | published files by level      |
                +-------------------------------+
                               |
                               v
                +-------------------------------+
                | latest logical-shard map      |
                | [start, end) -> live_size     |
                | split/merge metadata only     |
                +-------------------------------+
```

Normative pseudocode:

```text
function conceptual_write(state, mutation):
    append_mutation_to_wal(state, mutation)
    apply_mutation_to_shared_data_plane(state, mutation)
```

## 2. Scope

### 2.1 In scope

- single-process storage semantics [BEHAVIORAL]
- one shared levelled LSM tree for all user keys [BEHAVIORAL]
- latest-only logical-shard metadata [BEHAVIORAL]
- metadata-only local split and merge [BEHAVIORAL]
- semantic `config.toml` parsing and validation [BEHAVIORAL]
- WAL durability through fixed-size segments [BEHAVIORAL]
- immutable shared data `.kjm` file format [BEHAVIORAL]
- immutable metadata-checkpoint `.kjm` file format [BEHAVIORAL]
- memtables, flushes, compaction, logical-shard maintenance, and garbage collection [BEHAVIORAL]
- snapshot reads and scans [BEHAVIORAL]
- crash recovery [BEHAVIORAL]

### 2.2 Out of scope

- replication [BEHAVIORAL]
- consensus [BEHAVIORAL]
- leader election [BEHAVIORAL]
- cross-node repartition and data movement [BEHAVIORAL]
- user-visible multi-key transactions [BEHAVIORAL]
- authentication and authorization [BEHAVIORAL]
- encryption at rest [BEHAVIORAL]
- secondary indexes [BEHAVIORAL]
- TTL and expiration [BEHAVIORAL]
- reverse scans [BEHAVIORAL]

### 2.3 Non-goals

- keeping logical shards snapshot-versioned [BEHAVIORAL]

Worked example:

```text
In scope:
  split one logical shard into two current logical shards
  merge two adjacent logical shards into one current logical shard

Out of scope:
  move one logical shard to another node
  preserve an older logical-shard map for an old snapshot handle
```

ASCII diagram:

```text
This specification:
  shared LSM + current logical-shard map + local metadata-only split/merge

Future distributed layer:
  replication + consensus + cross-node movement + placement policy
```

Normative pseudocode:

```text
function classify_feature(feature_name):
    if feature_name in {
        "shared-lsm",
        "logical-shard-split",
        "logical-shard-merge",
        "checkpoint",
        "crash-recovery"
    }:
        return InScope
    return FutureOrOutOfScope
```

## 3. Persistent-Format Policy

### 3.1 Normative durable formats

The durable artifacts in this specification are:

- `store/config.toml` [BEHAVIORAL]
- `store/CURRENT` [BEHAVIORAL]
- `store/wal/*.log` [BEHAVIORAL]
- `store/meta/*.kjm` [BEHAVIORAL]
- `store/data/*.kjm` [BEHAVIORAL]

The durable format rules are:

- `config.toml` is semantic TOML durable state [BEHAVIORAL]
- the remaining durable artifacts use the binary layouts defined in this document [BEHAVIORAL]
- all persistent bytes are defined by this document [BEHAVIORAL]
- a reader MUST reject any durable artifact that violates the byte-level rules in this document [BEHAVIORAL]

Directory layout overview:

```text
store/
  config.toml
  CURRENT
  wal/
  meta/
  data/
```

## 4. Common Binary Conventions

### 4.1 Integer and alignment rules

- all numeric fields are little-endian [BEHAVIORAL]
- `u8`, `u16`, `u32`, `u64`, and `i64` have their standard fixed widths [BEHAVIORAL]
- all offsets are byte offsets from the start of the containing artifact or block unless explicitly
  stated otherwise [BEHAVIORAL]
- all page-based file structures are aligned to `page_size_bytes` [PERF]
- all WAL records are aligned to 8 bytes [PERF]
- writers MUST zero all unused padding bytes, reserved fields, and reserved bytes [BEHAVIORAL]
- readers MUST follow the checksum and field-validation rules in later sections; reserved and
  padding bytes have no meaning unless a later section assigns one [BEHAVIORAL]
- `NONE_U64` is `0xffffffffffffffff` [MERGED with #4.3] [BEHAVIORAL]

### 4.2 Checksums and digests

The checksum algorithm used for corruption detection is `CRC32C`. [BEHAVIORAL]

Exact CRC32C parameters:

- polynomial: Castagnoli
- reflected input: yes
- reflected output: yes
- initial value: `0xffffffff`
- final xor: `0xffffffff`

Rules:

- every checksum field stores the CRC32C of the covered bytes [BEHAVIORAL]
- when a structure contains its own checksum field, that field is zeroed for the checksum
  computation [BEHAVIORAL]
- all bytes covered by a checksum, including required-zero padding bytes, MUST match exactly [BEHAVIORAL]

### 4.4 Enum encodings

- `RecordKind`: `0 = Delete`, `1 = Put` [BEHAVIORAL]
- `FileKind`: `1 = DataFile`, `2 = MetadataCheckpoint` [BEHAVIORAL]
- `BlockKind`:
  `1 = InternalPage`, `2 = DataLeafBlock`, `3 = FileManifestBlock`,
  `4 = LogicalShardBlock` [BEHAVIORAL]
- `WalRecordType`:
  `1 = Put`, `2 = Delete`, `3 = FlushPublish`, `4 = CompactPublish`,
  `5 = LogicalShardInstall` [BEHAVIORAL]

Worked example:

```text
One accepted logical split produces:
  Put/Delete records for ordinary writes
  FlushPublish and CompactPublish for shared data files
  LogicalShardInstall for the metadata-only range change
```

ASCII diagram:

```text
WAL record groups

  data mutations:      Put, Delete
  data maintenance:    FlushPublish, CompactPublish
  logical metadata:    LogicalShardInstall
```

Normative pseudocode:

```text
function validate_wal_record_type(record_type):
    assert_invariant(
        record_type in {
            Put, Delete, FlushPublish, CompactPublish,
            LogicalShardInstall
        },
        Corruption)
```

### 4.5 Variable-length fields

- user-key length fields are `u16` [BEHAVIORAL]
- value length fields are `u32` [BEHAVIORAL]
- vector counts are `u32` [BEHAVIORAL]
- any variable-length wire structure with a fixed prefix includes `fixed_bytes` [BEHAVIORAL]
- the total size of such a structure is `fixed_bytes + sum(variable_section_lengths)` [BEHAVIORAL]

### 4.6 `BoundWire`

`BoundWire` exists to encode logical-shard boundaries and other current range descriptions. [BEHAVIORAL]

Exact encoding:

- byte `0`: `bound_kind`
  - `0 = Finite`
  - `1 = NegInf`
  - `2 = PosInf`
- if `bound_kind = Finite`:
  - bytes `1..2`: `key_len` as `u16`
  - bytes `3..(3 + key_len - 1)`: raw key bytes
- if `bound_kind = NegInf` or `PosInf`:
  - no more bytes follow

Rules:

- a finite boundary key length MUST be in `1..1024` [BEHAVIORAL]
- `NegInf` and `PosInf` MUST have total byte length `1` [BEHAVIORAL]
- a logical shard start boundary MUST be either `NegInf` or `Finite` [BEHAVIORAL]
- a logical shard end boundary MUST be either `Finite` or `PosInf` [BEHAVIORAL]

`BoundWire` total order:

- `NegInf < Finite(key) < PosInf` for every finite `key`
- finite boundaries are ordered by unsigned-byte lexicographic order of their raw key bytes
- when a user key `k` is compared to a boundary, the user key MUST be treated as conceptual
  `Finite(k)`

Worked example:

```text
Logical shard map:
  [-inf, "m")
  ["m", +inf)

Comparisons:
  NegInf < Finite("a") < Finite("m") < Finite("yak") < PosInf
```

ASCII diagram:

```text
Bound order:

  NegInf -------- Finite("a") ---- Finite("m") ---- Finite("yak") ---- PosInf
```

Normative pseudocode:

```text
function compare_bound(a, b):
    rank(NegInf) = 0
    rank(Finite) = 1
    rank(PosInf) = 2

    if rank(a.kind) < rank(b.kind):
        return -1
    if rank(a.kind) > rank(b.kind):
        return +1

    if a.kind != Finite:
        return 0

    return compare_lexicographic(a.key_bytes, b.key_bytes)
```

### 4.8 Shared helper functions

The following helpers are referenced by later pseudocode blocks.

Normative pseudocode:

```text
function logical_range_contains(entry, user_key):
    bound = Finite(user_key)
    return compare_bound(entry.start_bound, bound) <= 0 and
           compare_bound(bound, entry.end_bound) < 0

function allocate_file_ids(state, output_file_count):
    first_id = state.next_file_id
    ids = []
    for i in 0 .. output_file_count - 1:
        ids.push(first_id + i)
    state.next_file_id <- first_id + output_file_count
    return ids
```

## 5. `config.toml`

### 5.1 Path and parsing rules

The config file path is:

`store/config.toml`

The file is normative durable operator input. The engine MUST parse it as TOML and validate the
resulting values semantically. [BEHAVIORAL]

Rules:

- the file MUST be valid TOML [BEHAVIORAL]
- unknown keys MAY be ignored
- comments, blank lines, key order, and table order MUST NOT change the accepted meaning [BEHAVIORAL]
- implementations MAY rewrite the file into a preferred canonical layout when they create or update
  it, but `open()` MUST validate values rather than exact source bytes [BEHAVIORAL]

Example configuration:

```toml
[engine]
sync_mode = "per_write"

[wal]
segment_bytes = 1073741824

[lsm]
memtable_flush_bytes = 67108864
base_level_bytes = 268435456
level_fanout = 10
l0_file_threshold = 8
max_levels = 7
```

If `config.toml` is not valid TOML or violates the schema and validation rules in this section,
`open()` MUST return `InvalidArgument`.

### 5.2 Schema

- `engine.sync_mode`:
  type enum string; default `"per_write"`; mutability reopen-only; validation MUST be
  `"per_write"` or `"manual"` [PERF]
- `wal.segment_bytes`:
  type `u64`; default `1073741824`; mutability reopen-only; validation
  `> max_single_wal_record_bytes + 256` [PERF]
- `lsm.memtable_flush_bytes`:
  type `u64`; default `67108864`; mutability reopen-only; validation `>= 4 * 4096` [PERF]
- `lsm.base_level_bytes`:
  type `u64`; default `268435456`; mutability reopen-only; validation `>= memtable_flush_bytes` [PERF]
- `lsm.level_fanout`:
  type `u32`; default `10`; mutability reopen-only; validation `2..32` [PERF]
- `lsm.l0_file_threshold`:
  type `u32`; default `8`; mutability reopen-only; validation `>= 2` [PERF]
- `lsm.max_levels`:
  type `u32`; default `7`; mutability reopen-only; validation `4..16` [PERF]

Implementation constants:

- `format_major = 1`
- `page_size_bytes = 4096`
- `max_key_bytes = 1024`
- `max_value_bytes = 268435455`
- `max_single_wal_record_bytes = 64 + 8 + max_key_bytes + max_value_bytes + 8`
- `checksum_type = crc32c`

`sync_mode` semantics:

- `"per_write"`:
  - `put`, `delete`, and all publish records acknowledge only after their WAL record is durable
  - multiple waiting operations MAY share one fsync
- `"manual"`:
  - `put`, `delete`, and publish records acknowledge after the record bytes are appended to the
    active segment file
  - a crash MAY lose acknowledged records that are not covered by a completed `sync()`

### 5.3 Update semantics

Rules:

- config changes are reopen-only [BEHAVIORAL]
- there is no live config mutation API in v1 [BEHAVIORAL]

## 6. Data and State Model

### 6.1 Keys and logical ranges

User-key ordering is unsigned-byte lexicographic.

Logical shards are the current ordered range partition used for current `live_size_bytes`
metadata, split and merge planning, and future placement guidance. The shared data plane serves
the full keyspace globally: every file, memtable, and read path operation spans all logical shard
ranges without any shard-local restriction.

Rules:

- all logical shard ranges are half-open: `[start, end)` [BEHAVIORAL]
- the current logical shard set MUST be sorted by `start` ascending [BEHAVIORAL]
- the first current logical shard MUST use `start = NegInf` [BEHAVIORAL]
- the last current logical shard MUST use `end = PosInf` [BEHAVIORAL]
- current logical shard ranges MUST be disjoint and contiguous [BEHAVIORAL]
- the current logical-shard map MUST contain at least one entry [BEHAVIORAL]

Worked example:

```text
Current logical shard map:
  [-inf, "m")  -> 900 MiB
  ["m", +inf)  -> 1300 MiB

Every current user key belongs to exactly one current logical shard.
This does not mean the key belongs to one file or one memtable.
```

ASCII diagram:

```text
current logical shards

  [-inf, "m") | ["m", +inf)
```

Normative pseudocode:

```text
function validate_current_logical_shards(entries):
    validate_contiguous_logical_entry_list(entries)
    assert_invariant(len(entries) >= 1, Corruption)
    assert_invariant(entries[0].start_bound == NegInf, Corruption)
    assert_invariant(entries[len(entries) - 1].end_bound == PosInf,
                     Corruption)

function validate_contiguous_logical_entry_list(entries):
    for i in 0 .. len(entries) - 1:
        assert_invariant(
            compare_bound(entries[i].start_bound,
                          entries[i].end_bound) < 0,
            Corruption)
        if i > 0:
            assert_invariant(
                entries[i - 1].end_bound == entries[i].start_bound,
                Corruption)
```

### 6.2 Global sequence numbers

`GlobalSeqNo` is a monotonic `u64`. [BEHAVIORAL]

Rules:

- every committed WAL record consumes exactly one `GlobalSeqNo` [BEHAVIORAL]
- after restart, seqno `N` is committed if and only if recovery finds one full valid WAL record
  with `seqno = N` [BEHAVIORAL]
- `Put` and `Delete` records become internal-key versions [BEHAVIORAL]
- control records also consume sequence numbers so that recovery and metadata visibility are totally
  ordered [BEHAVIORAL]
- `next_seqno` starts at `1` [BEHAVIORAL]
- a snapshot from a brand-new empty store uses `snapshot_seqno = 0` [BEHAVIORAL]

Worked example:

```text
seq 41: Put("ant", ...)
seq 42: LogicalShardInstall(...)
seq 43: Delete("yak")

The metadata change at seq 42 is totally ordered relative to ordinary writes.
```

ASCII diagram:

```text
time ---->  41          42                   43
            Put   LogicalShardInstall      Delete
```

Normative pseudocode:

```text
function allocate_seqno(state):
    seqno = state.next_seqno
    state.next_seqno <- state.next_seqno + 1
    return seqno
```

### 6.3 Internal-key ordering

Data-record internal keys are ordered as:

`(user_key ASC, seqno DESC, kind ASC)` [BEHAVIORAL]

Where:

- `kind = Delete` sorts before `Put` [BEHAVIORAL]
- only `Put` and `Delete` records appear in shared data files and memtables [BEHAVIORAL]

Worked example:

```text
For user key "yak":
  ("yak", 9, Delete)
  ("yak", 8, Put)
  ("yak", 3, Put)

The first record with seqno <= snapshot_seqno decides visibility.
```

ASCII diagram:

```text
internal order for one user key:

  newest seqno ----> oldest seqno
```

Normative pseudocode:

```text
function compare_internal(a, b):
    if a.user_key < b.user_key:
        return -1
    if a.user_key > b.user_key:
        return +1

    if a.seqno > b.seqno:
        return -1
    if a.seqno < b.seqno:
        return +1

    if a.kind < b.kind:
        return -1
    if a.kind > b.kind:
        return +1

    return 0
```

### 6.4 Shared data manifest

The shared data manifest exists because reads and scans must see one snapshot-pinned global view of
the shared read-source set. Published data files remain one part of that source set, and the
manifest remains independent of logical-shard boundaries. [BEHAVIORAL]

Rules:

- the shared data manifest is one immutable snapshot identified by `data_generation` [BEHAVIORAL]
- reads and scans use the pinned `data_generation` from their snapshot handle, not the
  latest logical map [BEHAVIORAL]
- historical data manifest snapshots MAY remain in memory while active snapshots or background jobs
  reference them [BEHAVIORAL]
- L0 files MAY overlap by user key [BEHAVIORAL]
- files in each level `>= 1` MUST be strictly ordered by user key and MUST NOT overlap by user-key
  range [BEHAVIORAL]

Worked example:

```text
Snapshot A pins data_generation = 12.
Later compaction publishes data_generation = 13.

Snapshot A still reads manifest generation 12.
stats() still reports the latest current logical-shard map.
```

ASCII diagram:

```text
snapshot A ----> pinned data manifest generation 12
latest engine -> current data manifest generation 13
latest engine -> current logical-shard map
```

Normative pseudocode:

```text
function files_covering_key(data_manifest, user_key):
    result = []

    for each file_meta in data_manifest.levels[0] in descending file_id order:
        if file_meta.min_user_key <= user_key <= file_meta.max_user_key:
            result.push(file_meta)

    for level_no in 1 .. data_manifest.max_level_with_files:
        file_meta = unique_nonoverlapping_file_containing_key(
            data_manifest.levels[level_no], user_key)
        if file_meta != None:
            result.push(file_meta)

    return result
```

Complexity note:

- `files_covering_key(...)` is `Theta(F0 + Lh * log Fh)` time and `Theta(F0 + Lh)` extra RAM, where
  `F0` is the current L0 file count, `Lh` is the number of higher levels examined, and `Fh` is the
  maximum file count in one higher level

### 6.4A Shared-data state machine

The shared-data state machine exists to define exactly which sources are visible to reads, which
transitions create a new `data_generation`, and which durable records are required to recover the
same visible shared-data state after restart. [BEHAVIORAL]

One current or historical `data_generation` is the immutable read-source tuple:

- `levels[]`: the published shared data files by level [BEHAVIORAL]
- `active_memtable_ref`: exactly one memtable reference, which MAY be empty [BEHAVIORAL]
- `frozen_memtable_refs[]`: zero or more immutable memtable references ordered from oldest to
  newest freeze [BEHAVIORAL]

Rules:

- the tuple above is the complete shared-data source set for reads at that generation [BEHAVIORAL]
- `data_generation` identifies the source set, not the latest seqno contained inside those sources
  [BEHAVIORAL]
- `Put` and `Delete` mutate only the current generation's `active_memtable_ref` and MUST NOT change
  `data_generation` [BEHAVIORAL]
- `freeze_active_memtable(...)` creates the next `data_generation` by moving the old
  `active_memtable_ref` to the end of `frozen_memtable_refs[]` and installing one new empty active
  memtable [BEHAVIORAL]
- an accepted `FlushPublish` creates the next `data_generation` by removing exactly one frozen
  memtable source from the current generation and adding the payload output files to L0 [BEHAVIORAL]
- an accepted `CompactPublish` creates the next `data_generation` by removing exactly the payload
  input file set from the current generation and adding the payload output files [BEHAVIORAL]
- a new generation MUST preserve every read source from the prior generation except the sources
  explicitly replaced by the accepted transition [BEHAVIORAL]
- historical generations MAY share file references or memtable references; sharing MUST NOT make a
  historical generation mutable [BEHAVIORAL]
- reads for one explicit or implicit snapshot MUST consult exactly the pinned generation's
  `active_memtable_ref`, then its `frozen_memtable_refs[]` in descending `frozen_memtable_id`, then
  its published files [BEHAVIORAL]
- pre-crash historical generations are not recovered after restart because snapshot handles do not
  survive restart; recovery MUST reconstruct only the latest visible shared-data state [BEHAVIORAL]
- a checkpoint-captured generation MUST have zero frozen memtables and an empty active memtable so
  the checkpointed file manifest alone represents the exact visible shared-data state at the
  checkpoint frontier [BEHAVIORAL]

Worked example:

```text
Current generation 12:
  active  = A7 containing seq 181..190
  frozen  = [F21 containing seq 101..150,
             F22 containing seq 151..180]
  files   = L0: [42], L1: [7, 8]

Snapshot S captures:
  snapshot_seqno = 188
  data_generation = 12

Later live transitions:
  Freeze(A7)         -> generation 13
  FlushPublish(F21)  -> generation 14

Snapshot S still reads exactly:
  active  = A7
  frozen  = [F21, F22]
  files   = generation-12 files
```

ASCII diagram:

```text
Put/Delete
  |
  v
same generation
  active memtable contents change only

Freeze
  |
  v
next generation
  old active -> frozen tail
  new empty active installed

FlushPublish
  |
  v
next generation
  remove one frozen source
  add output L0 files

CompactPublish
  |
  v
next generation
  replace input file set
  keep same memtable refs

Checkpoint capture allowed only when:
  active empty and frozen set empty
```

Normative pseudocode:

```text
function generation_read_sources(data_manifest):
    return {
        active_memtable_ref = data_manifest.active_memtable_ref,
        frozen_memtable_refs = data_manifest.frozen_memtable_refs,
        levels = data_manifest.levels
    }

function install_generation_after_freeze(state, old_active_memtable):
    next_manifest = clone_current_data_manifest(state)
    next_manifest.data_generation <- state.data_generation + 1
    next_manifest.active_memtable_ref <- new_empty_memtable()
    next_manifest.frozen_memtable_refs.push(old_active_memtable)
    retain_manifest_snapshot(state, next_manifest)
    state.current_data_manifest <- next_manifest
    state.active_memtable <- next_manifest.active_memtable_ref
    state.frozen_memtables <- next_manifest.frozen_memtable_refs
    state.data_generation <- next_manifest.data_generation

function install_generation_after_flush_publish(state, source_frozen, outputs):
    next_manifest = clone_current_data_manifest(state)
    next_manifest.data_generation <- state.data_generation + 1
    remove_one_matching_frozen_ref(next_manifest.frozen_memtable_refs,
                                   source_frozen.frozen_memtable_id)
    prepend_outputs_to_l0(next_manifest.levels, outputs)
    retain_manifest_snapshot(state, next_manifest)
    state.current_data_manifest <- next_manifest
    state.frozen_memtables <- next_manifest.frozen_memtable_refs
    state.data_generation <- next_manifest.data_generation

function install_generation_after_compact_publish(state, input_file_ids, outputs):
    next_manifest = clone_current_data_manifest(state)
    next_manifest.data_generation <- state.data_generation + 1
    remove_input_files(next_manifest.levels, input_file_ids)
    add_output_files(next_manifest.levels, outputs)
    retain_manifest_snapshot(state, next_manifest)
    state.current_data_manifest <- next_manifest
    state.data_generation <- next_manifest.data_generation
```

### 6.5 Current logical-shard map

The current logical-shard map exists to track latest-only range metadata and to drive metadata-only
split and merge. It does not decide the read path.

Rules:

- only the current logical-shard map is retained after a logical-shard install succeeds
- old snapshots do not pin old logical-shard maps
- if recovery does not load a logical-shard map from a checkpoint, `open()` MUST initialize one
  current logical shard `[-inf, +inf)` with `live_size_bytes = 0`
- ordinary `put` and `delete` operations MUST NOT depend on exact old-value reads in order to keep
  `live_size_bytes` current
- `live_size_bytes` is advisory metadata; implementations MAY refresh it eagerly, lazily, or on
  demand
- `get` and `scan` MUST NOT consult the logical-shard map for visibility

Worked example:

```text
Fresh open with no checkpoint:
  current logical shards = [[-inf, +inf) -> 0]

Later writes:
  may change the true live bytes before the next recomputation pass
```

ASCII diagram:

```text
after open: [-inf, +inf)
```

Normative pseudocode:

```text
function find_logical_shard_covering_key(entries, user_key):
    search = Finite(user_key)
    idx = upper_bound(entries, search,
                      lambda entry, bound:
                          compare_bound(entry.start_bound, bound)) - 1
    assert_invariant(idx >= 0, Corruption)
    assert_invariant(logical_range_contains(entries[idx], user_key),
                     Corruption)
    return idx
```

Complexity note:

- `find_logical_shard_covering_key(...)` is `Theta(log S_log)` time and `Theta(1)` extra RAM,
  where `S_log` is the current logical-shard count

### 6.6 Runtime entities

- `FileId`: monotonic `u64`, never reused
- `FileMeta`:
  `file_id`, `level_no`, `min_user_key`, `max_user_key`, `min_seqno`, `max_seqno`, `entry_count`,
  `logical_bytes`, `physical_bytes`
- `DataManifestSnapshot`:
  `data_generation`, `levels[]`, `active_memtable_ref`, `frozen_memtable_refs[]`, `refcount`;
  it is one immutable shared-data source set exactly as defined in Section 6.4A
- `empty_data_manifest_snapshot`:
  the immutable in-memory data manifest for `data_generation = 0`; it has zero files, one empty
  active memtable reference, zero frozen memtable references, is created for every successful
  `open()`, and remains retained until engine close so generation-0 snapshots stay readable
- `LogicalShardEntry`:
  `start_bound`, `end_bound`, `live_size_bytes`
- `durable_current`:
  the authoritative `CURRENT` fields known to the open engine instance, or `None` before the first
  successful checkpoint install
- `SnapshotHandle`:
  `engine_instance_id`, `snapshot_id`, `snapshot_seqno`, `data_generation`
- `active_snapshots`:
  keyed by `snapshot_id`; contains only currently active snapshot handles
- `StatsResponse`:
  `observation_seqno`, `data_generation`, `levels[]`, `logical_shards[]`

### 6.7 Generations

The engine uses one snapshot-visible generation space plus monotonic allocators. The shared data
plane and the current logical-shard map still change for different reasons, but only the shared
data plane needs a public generation counter in v1.

Rules:

- `data_generation`
  - identifies one full immutable in-memory shared-data source set
  - generation `0` is reserved for the empty data manifest snapshot
  - increments by `1` on every accepted shared-data structural transition that changes the current
    read-source set: freeze, flush publish, or compaction publish
  - one shared-data structural transition is accepted at a time in v1
- `next_file_id`
  - starts at `1`
  - advances by the output file count on every accepted flush or compaction publish
- `next_snapshot_id`
  - starts at `1`
- `next_frozen_memtable_id`
  - starts at `1`

Worked example:

```text
Freeze:
  11 -> 12

Flush publish:
  data_generation_expected = 12
  accepted result generation = 13

Compaction publish:
  data_generation_expected = 13
  accepted result generation = 14
```

ASCII diagram:

```text
data-generation history: 11 -> 12 -> 13 -> 14
```

### 6.8 Byte accounting and helper values

For any internal data record:

- `logical_record_bytes(Put) = len(user_key) + len(value)`
- `logical_record_bytes(Delete) = len(user_key)`

For shared data files:

- `FileMeta.entry_count` counts all stored internal records, including tombstones and shadowed
  versions
- `FileMeta.logical_bytes` is the exact sum of `logical_record_bytes(record)` over every stored
  record in the file
- `FileMeta.physical_bytes` is the exact durable file length in bytes

For current logical shards:

- `live_size_bytes` is the latest tracked current-byte estimate for that range
- implementations MAY recompute an exact value on demand by summing, over all user keys in that
  current range whose highest visible version in the latest state is a `Put`, `len(key) + len(value)`
- user keys whose highest visible version is `Delete`, or that are absent, contribute `0`
- ordinary writes MAY leave `live_size_bytes` stale until the next recomputation pass
- exact recomputation is optional and affects observability and maintenance quality, not read
  correctness

Snapshot-derived helper value:

- `oldest_snapshot_seqno` is the minimum `snapshot_seqno` across active snapshot handles
- if there is no active snapshot handle, `oldest_snapshot_seqno = next_seqno`

Worked example:

```text
After many blind writes:
  stored live_size_bytes may lag

An implementation may later recompute from current visible state.
```

ASCII diagram:

```text
logical-shard size refresh

  ordinary writes ---> current bytes may drift
  optional pass   ---> exact value restored
```

Normative pseudocode:

```text
function oldest_snapshot_seqno_or_next(state):
    active = [snapshot.snapshot_seqno for snapshot in state.active_snapshots]
    if active is empty:
        return state.next_seqno
    return min(active)
```

Complexity note:

- `oldest_snapshot_seqno_or_next(...)` is `Theta(S_open)` time and `Theta(1)` extra RAM, where
  `S_open` is the number of tracked snapshot handles for the open engine instance

## 7. Common Wire Types

Complexity note:

- not applicable; this section defines fixed encodings rather than asymptotically growing
  algorithms

### 7.1 `FileMetaWire`

`FileMetaWire` is used in WAL publish payloads and metadata checkpoint blocks.

Fixed prefix layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `file_id` |
| `8` | `8` | `min_seqno` |
| `16` | `8` | `max_seqno` |
| `24` | `8` | `entry_count` |
| `32` | `8` | `logical_bytes` |
| `40` | `8` | `physical_bytes` |
| `48` | `2` | `level_no` |
| `50` | `2` | `min_key_len` |
| `52` | `2` | `max_key_len` |
| `54` | `2` | `reserved_zero` |
| `56` | `4` | `fixed_bytes = 60` |

Variable sections:

- bytes `60..`: `min_user_key`
- followed immediately by `max_user_key`

Total bytes:

`60 + min_key_len + max_key_len`

Rules:

- canonical filename is derived, not stored:
  - `data/<file_id_20d>.kjm`
- `min_key_len` and `max_key_len` MUST be nonzero
- `min_user_key <= max_user_key`
- `file_id >= 1`

Worked example:

```text
One L1 file:
  file_id = 42
  level_no = 1
  min_user_key = "ant"
  max_user_key = "yak"

Path:
  data/00000000000000000042.kjm
```

ASCII diagram:

```text
FileMetaWire

  fixed 60 bytes | min_user_key bytes | max_user_key bytes
```

### 7.2 `LogicalShardEntryWire`

`LogicalShardEntryWire` is used in `LogicalShardInstall` payloads and metadata checkpoint blocks.

Fixed prefix layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `live_size_bytes` |
| `8` | `4` | `start_bound_bytes` |
| `12` | `4` | `end_bound_bytes` |
| `16` | `4` | `fixed_bytes = 24` |
| `20` | `4` | `reserved_zero` |

Variable sections:

- bytes `24..`: `start_bound` as canonical `BoundWire`
- followed immediately by `end_bound` as canonical `BoundWire`

Total bytes:

`24 + start_bound_bytes + end_bound_bytes`

Rules:

- `start_bound` MUST be `NegInf` or `Finite`
- `end_bound` MUST be `Finite` or `PosInf`
- the range `[start_bound, end_bound)` MUST be non-empty under `compare_bound(...)`

Worked example:

```text
One logical shard entry:
  start_bound = Finite("m")
  end_bound = PosInf
  live_size_bytes = 1300 MiB
```

ASCII diagram:

```text
LogicalShardEntryWire

  fixed 24 bytes | start BoundWire | end BoundWire
```

### 7.3 `SnapshotHandle`

`SnapshotHandle` is a public conceptual type, not a durable on-disk type.

Required logical fields:

- `engine_instance_id: u64`
- `snapshot_id: u64`
- `snapshot_seqno: u64`
- `data_generation: u64`

Rules:

- `engine_instance_id` identifies one successful `open()` result
- a `SnapshotHandle` is valid only for the currently open engine instance whose
  `engine_instance_id` matches the handle
- a snapshot handle pins `snapshot_seqno` and `data_generation`
- a snapshot handle does not pin any historical logical-shard metadata
- after a successful `release_snapshot()`, the engine MUST remove the tracked snapshot entry

Worked example:

```text
Snapshot A:
  snapshot_seqno = 500
  data_generation = 12

Later logical split:
  current logical-shard map changes

Snapshot A still reads data_generation 12.
```

ASCII diagram:

```text
SnapshotHandle

  engine_instance_id | snapshot_id | snapshot_seqno | data_generation
```

Normative pseudocode:

```text
function validate_snapshot_handle(state, handle):
    tracked = state.active_snapshots[handle.snapshot_id]
    assert_invariant(tracked != None, InvalidArgument)
    assert_invariant(
        tracked.engine_instance_id == handle.engine_instance_id and
        tracked.snapshot_id == handle.snapshot_id and
        tracked.snapshot_seqno == handle.snapshot_seqno and
        tracked.data_generation == handle.data_generation,
        InvalidArgument)
```

### 7.4 `LevelStats` and `LogicalShardStats`

These are public conceptual types, not durable on-disk types.

`LevelStats` fields:

- `level_no`
- `file_count`
- `logical_bytes`
- `physical_bytes`

`LogicalShardStats` fields:

- `start_bound`
- `end_bound`
- `live_size_bytes`

Rules:

- `LevelStats` entries MUST be returned in ascending `level_no`
- `LogicalShardStats` entries MUST be returned in ascending `start_bound`
- `live_size_bytes` is the current tracked value for that logical shard

Worked example:

```text
levels[]:
  L0 -> 3 files
  L1 -> 14 files

logical_shards[]:
  [-inf, "m")
  ["m", +inf)
```

ASCII diagram:

```text
stats full response

  levels[]        engine-wide shared data file totals
  logical_shards[] latest-only current range totals
```

Normative pseudocode:

```text
function validate_stats_arrays(levels, logical_shards):
    assert_invariant(levels are sorted by level_no ascending, Corruption)
    assert_invariant(logical_shards are sorted by start_bound ascending,
                     Corruption)
```

### 7.5 `StatsResponse`

This is a public conceptual type, not a durable on-disk type.

`StatsResponse`:

- `observation_seqno`
- `data_generation`
- `levels[]`
- `logical_shards[]`

Worked example:

```text
stats():
  returns engine-wide level totals and the current logical shards
```

ASCII diagram:

```text
stats response:
  levels[] + logical_shards[]
```

## 8. Storage Layout and Durable Artifacts

### 8.1 Directory layout

```text
store/
  config.toml
  CURRENT
  wal/
    wal-00000000000000000001-open.log
    wal-00000000000000000001-00000000000000001024.log
  meta/
    meta-00000000000000000001.kjm
  data/
    00000000000000000001.kjm
```

Filename rules:

- all `u64` numbers in filenames are zero-padded decimal with width 20
- closed WAL segment filenames sort lexicographically in the same order as their seqno ranges
- files that do not match an exact durable filename pattern MUST NOT affect visible state

Worked example:

```text
Data file id 42:
  store/data/00000000000000000042.kjm

Checkpoint generation 9:
  store/meta/meta-00000000000000000009.kjm
```

ASCII diagram:

```text
WAL -> shared data files + metadata checkpoints
```

Normative pseudocode:

```text
function canonical_data_filename(file_id):
    return "store/data/" + zero_pad_20(file_id) + ".kjm"
```

### 8.2 `CURRENT` file

`CURRENT` is a fixed-size binary file of exactly 72 bytes.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `magic = "KJCURR1"` |
| `8` | `2` | `format_major = 1` |
| `10` | `2` | `reserved_zero_header` |
| `12` | `4` | `file_bytes = 72` |
| `16` | `8` | `checkpoint_generation` |
| `24` | `8` | `checkpoint_max_seqno` |
| `32` | `8` | `checkpoint_data_generation` |
| `40` | `8` | `reserved_zero_logical` |
| `48` | `4` | `reserved_zero_mid` |
| `52` | `4` | `body_crc32c` |
| `56` | `16` | `reserved_zero_tail` |

Rules:

- `checkpoint_generation` identifies `meta/meta-<checkpoint_generation_20d>.kjm`
- the first installed checkpoint in a store MUST use `checkpoint_generation = 1`
- `checkpoint_max_seqno` is the highest committed seqno whose visible effects are fully represented
  by the referenced metadata checkpoint together with the published data files that the checkpoint
  names
- `checkpoint_data_generation` is the shared data-manifest generation represented by that checkpoint
- `reserved_zero_header`, `reserved_zero_logical`, `reserved_zero_mid`, and
  `reserved_zero_tail` MUST be zero
- `body_crc32c` covers bytes `0..71` with bytes `52..55` zeroed
- `CURRENT` is authoritative for the latest durable checkpoint pointer
- if `CURRENT` exists, the referenced metadata checkpoint file MUST exist

Install protocol:

- write the checkpoint file so that either the old file or the full new file is durable after a
  crash
- replace `CURRENT` atomically so that recovery sees either the old checkpoint pointer or the full
  new checkpoint pointer
- after successful install, a crash MUST recover the new checkpoint exactly when the new `CURRENT`
  pointer is durable

Worked example:

```text
CURRENT points to:
  checkpoint_generation = 9
  checkpoint_max_seqno = 140
  checkpoint_data_generation = 12
```

ASCII diagram:

```text
write meta tmp -> fsync meta tmp -> rename to meta-N.kjm -> fsync meta/
                                               |
                                               v
write CURRENT tmp -> fsync CURRENT tmp -> rename to CURRENT -> fsync CURRENT
                                                               |
                                                               v
                                                         fsync store/
```

### 8.3 WAL segments

#### 8.3.1 WAL filename rules

Closed WAL segment:

`wal-<first_seqno_20d>-<last_seqno_20d>.log`

Active WAL segment:

`wal-<first_seqno_20d>-open.log`

#### 8.3.2 Segment header

Every WAL segment begins with a fixed 128-byte header.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `magic = "KJWALSE1"` |
| `8` | `2` | `format_major = 1` |
| `10` | `2` | `reserved_zero` |
| `12` | `4` | `header_bytes = 128` |
| `16` | `8` | `first_seqno_or_none` |
| `24` | `8` | `segment_bytes` |
| `32` | `4` | `header_crc32c` |
| `36` | `92` | `reserved_zero` |

Rules:

- `first_seqno_or_none = NONE_U64` only for a brand new active segment with no committed records yet
- `header_crc32c` covers bytes `0..127` with bytes `32..35` zeroed

#### 8.3.3 Segment footer

Every closed WAL segment ends with a fixed 128-byte footer. Active segments do not have a footer.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `magic = "KJWALF1"` |
| `8` | `8` | `first_seqno` |
| `16` | `8` | `last_seqno` |
| `24` | `8` | `record_count` |
| `32` | `8` | `payload_bytes_used` |
| `40` | `4` | `footer_crc32c` |
| `44` | `84` | `reserved_zero` |

Rules:

- the footer checksum covers bytes `0..127` with bytes `40..43` zeroed
- the closed filename range MUST exactly match `(first_seqno, last_seqno)`

#### 8.3.4 Record header

Every WAL record begins with a fixed 64-byte header.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `4` | `magic = "KJWR"` |
| `4` | `1` | `record_type` |
| `5` | `3` | `reserved_zero` |
| `8` | `4` | `total_bytes` |
| `12` | `4` | `payload_bytes` |
| `16` | `8` | `seqno` |
| `24` | `4` | `header_crc32c` |
| `28` | `4` | `payload_crc32c` |
| `32` | `32` | `reserved_zero` |

Rules:

- reserved bytes `5..7` MUST be zero
- `total_bytes` MUST be a multiple of 8
- `total_bytes >= 64 + payload_bytes`
- padding bytes between the payload end and `total_bytes` MUST be zero
- `header_crc32c` covers bytes `0..63` with bytes `24..27` zeroed
- `payload_crc32c` covers exactly the `payload_bytes` immediately following the header

#### 8.3.5 Record payloads

`Put` payload:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `2` | `key_len` |
| `2` | `2` | `reserved_zero` |
| `4` | `4` | `value_len` |
| `8` | `key_len` | `key_bytes` |
| `8 + key_len` | `value_len` | `value_bytes` |

`Delete` payload:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `2` | `key_len` |
| `2` | `6` | `reserved_zero` |
| `8` | `key_len` | `key_bytes` |

`FlushPublish` payload:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `data_generation_expected` |
| `8` | `8` | `source_first_seqno` |
| `16` | `8` | `source_last_seqno` |
| `24` | `8` | `source_record_count` |
| `32` | `4` | `output_file_count` |
| `36` | `4` | `fixed_bytes = 40` |
| `40` | variable | `output_file_metas[]` as consecutive `FileMetaWire` values |

`CompactPublish` payload:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `data_generation_expected` |
| `8` | `4` | `input_file_count` |
| `12` | `4` | `output_file_count` |
| `16` | `4` | `fixed_bytes = 24` |
| `20` | `4` | `reserved_zero` |
| `24` | variable | `input_file_ids[]` as consecutive `u64` values |
| ... | variable | `output_file_metas[]` as consecutive `FileMetaWire` values |

`LogicalShardInstall` payload:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `2` | `source_entry_count` |
| `2` | `2` | `output_entry_count` |
| `4` | `4` | `fixed_bytes = 8` |
| `8` | variable | `source_entries[]` as consecutive `LogicalShardEntryWire` values |
| ... | variable | `output_entries[]` as consecutive `LogicalShardEntryWire` values |

Rules:

- `Put` and `Delete` payloads MUST NOT contain any logical-shard identifier
- `FlushPublish.source_first_seqno <= FlushPublish.source_last_seqno`
- `FlushPublish.source_record_count >= 1`
- `FlushPublish.output_file_metas[]` and `CompactPublish.output_file_metas[]` MUST use unique
  previously unused `FileId` values at accepted publish time
- `CompactPublish.input_file_ids[]` MUST contain one or more unique `FileId` values sorted in
  ascending numeric order
- `LogicalShardInstall.source_entry_count` MUST be `1` or `2`
- `LogicalShardInstall.output_entry_count` MUST be `1` or `2`
- `LogicalShardInstall` MUST preserve the same outer bounds from the source entry set to the output
  entry set
- `LogicalShardInstall` MUST NOT publish a no-op update
- for a split or merge, stored `live_size_bytes` values are advisory metadata produced by the
  implementation's current tracking policy

Worked example:

```text
Logical split payload:
  source_entries = [[-inf, +inf) -> 2300 MiB]
  output_entries = [[-inf, "moose") -> 1350 MiB,
                    ["moose", +inf) -> 950 MiB]
```

ASCII diagram:

```text
LogicalShardInstall

  source entries:  [-inf, +inf)
  output entries:  [-inf, "moose") | ["moose", +inf)

No data file payload appears in this record.
```

Normative pseudocode:

```text
function validate_logical_shard_install_payload(payload):
    assert_invariant(payload.source_entry_count in {1, 2}, Corruption)
    assert_invariant(payload.output_entry_count in {1, 2}, Corruption)
    assert_invariant(len(payload.source_entries) == payload.source_entry_count,
                     Corruption)
    assert_invariant(len(payload.output_entries) == payload.output_entry_count,
                     Corruption)
    validate_contiguous_logical_entry_list(payload.source_entries)
    validate_contiguous_logical_entry_list(payload.output_entries)

    source_start = payload.source_entries[0].start_bound
    source_end =
        payload.source_entries[payload.source_entry_count - 1].end_bound
    output_start = payload.output_entries[0].start_bound
    output_end =
        payload.output_entries[payload.output_entry_count - 1].end_bound

    assert_invariant(source_start == output_start, Corruption)
    assert_invariant(source_end == output_end, Corruption)
```

#### 8.3.6 WAL append and rollover protocol

WAL append exists to impose one durable total order on mutations, shared-data publishes, and
logical metadata installs.

Rules:

- every live append allocates exactly one seqno before any durable bytes for that record are written
- the owner thread MUST append records in seqno order
- if the active segment lacks room for the next full aligned record plus a footer, the engine MUST:
  1. write the current segment footer
  2. fsync the current segment
  3. rename it to the closed filename
  4. fsync the `wal/` directory
  5. create and fsync a new active segment header
- after a live append path returns `IO` after seqno allocation, the current open instance MUST stop
  accepting further writes and MAY continue serving reads until the next `open()`

Worked example:

```text
seqno 101 allocated
append of Put(seqno = 101) returns IO after partial bytes

The open instance must stop accepting further writes.
After restart, seqno 101 is committed only if one full valid record 101 survived.
```

ASCII diagram:

```text
allocate seqno -> append bytes -> optional fsync -> success
                       |
                       v
                  read-only on IO after seqno allocation
```

Normative pseudocode:

```text
function append_wal_record(state, record_type, payload, sync_mode):
    seqno = allocate_seqno(state)
    record_bytes = encode_wal_record(record_type, payload, seqno)
    attempt append_bytes_to_active_segment(state, record_bytes)
    if append failed:
        state.open_instance_failed <- true
        return Err(IO)
    if sync_mode == per_write:
        attempt fsync(state.active_wal_segment)
        if fsync failed:
            state.open_instance_failed <- true
            return Err(IO)
    return Ok({seqno = seqno})
```

Complexity note:

- one append is `Theta(|payload| + B_roll)` time and `Theta(1)` extra RAM, where `B_roll` is the
  number of bytes written for a forced footer and new-segment header

#### 8.3.7 WAL replay and binary search

Replay exists to reconstruct the latest durable shared data state and the latest durable
logical-shard map from the last durable checkpoint.

Replay rules:

1. determine `replay_start_seqno`:
   - if `CURRENT` exists: `CURRENT.checkpoint_max_seqno + 1`
   - otherwise `1`
2. locate the first candidate segment by any method that is consistent with the durable filename
   rules
3. replay full valid records from that point in physical WAL order
4. after the last replayed record, materialize one recovered current generation containing:
   - the recovered published file manifest
   - one recovered active memtable with every replayed `Put` and `Delete` not covered by a replayed
     `FlushPublish`
   - zero recovered frozen memtables

Replay actions:

- `Put` and `Delete`:
  - append the mutation to the replay-state pending mutation set
- `FlushPublish` and `CompactPublish`:
  - validate referenced output files exist and pass file validation
  - validate output `FileId` values are unique and unused in replay state
  - `FlushPublish` MUST remove exactly the oldest replay-state pending-mutation prefix whose
    seqno interval and record count match the payload source fields
  - `CompactPublish` MUST remove exactly the payload input file set from the replay-state published
    file manifest
  - install the next replay-state published file manifest and advance `next_file_id`
- `LogicalShardInstall`:
  - validate that the current replay-state source entries exactly equal the payload source entries
  - replace those source entries with the payload output entries

Worked example:

```text
checkpoint_max_seqno = 150

replay records:
  151 Put("yak", "v2")
  152 LogicalShardInstall(...)
  153 Delete("ant")

Replay order is exactly 151, then 152, then 153.
The logical split at 152 changes only the current range map.
```

ASCII diagram:

```text
checkpoint ---- replay Put/Delete ---- replay data publish ---- replay logical install
      |
      v
materialize current generation:
  recovered files + one recovered active memtable + zero frozen memtables
```

Normative pseudocode:

```text
function replay_wal_from_state(state, replay_records):
    for each record in replay_records in physical seqno order:
        if record.record_type == Put:
            append_record_to_replay_pending_mutations(state, record)

        else if record.record_type == Delete:
            append_record_to_replay_pending_mutations(state, record)

        else if record.record_type == FlushPublish or
                record.record_type == CompactPublish:
            validate_publish_outputs_exist(state, record.payload.output_file_metas)
            install_replay_manifest_update(state, record)

        else if record.record_type == LogicalShardInstall:
            validate_logical_shard_install_payload(record.payload)
            replace_source_entries_with_outputs(state, record.payload)

    materialize_recovered_current_generation(state)

function install_replay_manifest_update(state, record):
    if record.record_type == FlushPublish:
        remove_oldest_matching_pending_prefix(
            state.replay_pending_mutations,
            record.payload.source_first_seqno,
            record.payload.source_last_seqno,
            record.payload.source_record_count)
        add_output_files_to_replay_l0(state, record.payload.output_file_metas)
        state.data_generation <- state.data_generation + 1
        advance_next_file_id_from_outputs(state, record.payload.output_file_metas)
        return

    remove_exact_input_files_from_replay_manifest(
        state.replay_levels, record.payload.input_file_ids)
    add_output_files_to_replay_levels(state, record.payload.output_file_metas)
    state.data_generation <- state.data_generation + 1
    advance_next_file_id_from_outputs(state, record.payload.output_file_metas)
```

Complexity note:

- replay is `Theta(R_replay * T_apply + B_pub)` time and `Theta(1)` extra RAM beyond
  any buffers used by the replay-state data plane, where `R_replay` is the replayed
  record count, `T_apply` is the per-record apply cost, and `B_pub` is the total
  validation bytes read from newly referenced data files

#### 8.3.8 WAL truncation

After a checkpoint is installed, the engine MAY delete any closed WAL segment whose `last_seqno <=
CURRENT.checkpoint_max_seqno`.

Rules:

- if deletion of one eligible segment fails, the engine MUST leave that segment retained, MUST NOT
  roll back the checkpoint, and MUST NOT fail the checkpoint install

Worked example:

```text
CURRENT.checkpoint_max_seqno = 1000

Closed segments:
  1..400   eligible
  401..800 eligible
  801..999 eligible
  1000..1200 not eligible
```

ASCII diagram:

```text
closed WAL segments

  [eligible][eligible][eligible][retained]
```

Normative pseudocode:

```text
function maybe_truncate_closed_wal_segments(state):
    for each segment in state.closed_wal_segments in stable filename order:
        if segment.last_seqno > state.durable_current.checkpoint_max_seqno:
            continue
        attempt delete(segment.path)
```

### 8.4 `.kjm` common container

Both shared data files and metadata checkpoint files use the `.kjm` container family.

#### 8.4.1 Common header page

Every `.kjm` file begins with a fixed 128-byte header page.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `magic = "KJKJM001"` |
| `8` | `2` | `format_major = 1` |
| `10` | `2` | `reserved_zero` |
| `12` | `4` | `header_bytes` |
| `16` | `2` | `file_kind` |
| `18` | `2` | `reserved_zero` |
| `20` | `4` | `page_size_bytes` |
| `24` | `8` | `root_block_id_or_none` |
| `32` | `8` | `block_count` |
| `40` | `8` | `entry_count` |
| `48` | `8` | `min_seqno_or_none` |
| `56` | `8` | `max_seqno_or_none` |
| `64` | `8` | `metadata_generation_or_zero` |
| `72` | `8` | `logical_bytes_total` |
| `80` | `8` | `physical_bytes_total` |
| `88` | `4` | `header_crc32c` |
| `92` | `36` | `reserved_zero` |

Rules:

- `header_crc32c` covers bytes `0..127` with bytes `88..91` zeroed
- `physical_bytes_total` MUST equal the exact durable file length
- shared data files MUST use `header_bytes = 128`
- metadata checkpoint files MUST use `header_bytes = 192`

#### 8.4.2 Common footer page

Every `.kjm` file ends with a fixed 128-byte footer page.

Layout:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `8` | `magic = "KJKJMF01"` |
| `8` | `2` | `format_major = 1` |
| `10` | `2` | `reserved_zero` |
| `12` | `4` | `footer_bytes = 128` |
| `16` | `8` | `root_block_id_or_none` |
| `24` | `8` | `block_count` |
| `32` | `8` | `entry_count` |
| `40` | `8` | `min_seqno_or_none` |
| `48` | `8` | `max_seqno_or_none` |
| `56` | `8` | `metadata_generation_or_zero` |
| `64` | `8` | `logical_bytes_total` |
| `72` | `8` | `physical_bytes_total` |
| `80` | `4` | `footer_crc32c` |
| `84` | `44` | `reserved_zero` |

Rules:

- `footer_crc32c` covers bytes `0..127` with bytes `80..83` zeroed
- header and footer totals MUST agree exactly

#### 8.4.3 Internal page

Internal pages exist only in shared data files because metadata checkpoints are linear lists rather
than routing trees.

Fixed 64-byte prefix:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `1` | `block_kind = 1` |
| `1` | `1` | `reserved_zero` |
| `2` | `2` | `cell_count` |
| `4` | `4` | `block_span_pages` |
| `8` | `8` | `rightmost_child_block_id` |
| `16` | `2` | `variable_bytes_begin_offset` |
| `18` | `2` | `reserved_zero` |
| `20` | `4` | `variable_bytes_total` |
| `24` | `4` | `block_crc32c` |
| `28` | `36` | `reserved_zero` |

Each internal cell is 16 bytes:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `2` | `separator_offset` |
| `2` | `2` | `separator_length` |
| `4` | `4` | `reserved_zero` |
| `8` | `8` | `child_block_id` |

Rules:

- separator keys in shared data files use full internal-key order
- `upper_bound(separator_keys, search_key, compare_internal)` chooses a child index in
  `0..cell_count`, where index `cell_count` means `rightmost_child_block_id`

Worked example:

```text
separator keys:
  ("m", 100, Put)
  ("t", 80, Put)

search key:
  ("yak", 0, Put)

child choice:
  rightmost child
```

ASCII diagram:

```text
child 0 | sep("m") | child 1 | sep("t") | child 2
```

Normative pseudocode:

```text
function choose_internal_child_block(page, search_key, comparator):
    idx = upper_bound(page.separator_keys, search_key, comparator)
    if idx == page.cell_count:
        return page.rightmost_child_block_id
    return page.cells[idx].child_block_id
```

### 8.5 Shared data `.kjm` files

#### 8.5.1 Filename

Canonical filename:

`data/<file_id_20d>.kjm`

#### 8.5.2 Header values

For shared data files:

- `file_kind = DataFile`
- `header_bytes = 128`
- `root_block_id_or_none != NONE_U64`
- `metadata_generation_or_zero = 0`

#### 8.5.3 Data leaf block

Fixed 64-byte prefix:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `1` | `block_kind = 2` |
| `1` | `1` | `reserved_zero` |
| `2` | `2` | `reserved_zero` |
| `4` | `4` | `entry_count` |
| `8` | `4` | `block_span_pages` |
| `12` | `4` | `reserved_zero` |
| `16` | `2` | `variable_bytes_begin_offset` |
| `18` | `2` | `reserved_zero` |
| `20` | `4` | `variable_bytes_total` |
| `24` | `8` | `next_leaf_block_id_or_none` |
| `32` | `4` | `block_crc32c` |
| `36` | `28` | `reserved_zero` |

Each data leaf slot is 24 bytes:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `2` | `key_offset` |
| `2` | `2` | `key_length` |
| `4` | `4` | `value_offset_or_zero` |
| `8` | `4` | `value_length` |
| `12` | `8` | `seqno` |
| `20` | `1` | `record_kind` |
| `21` | `3` | `reserved_zero` |

Rules:

- slots are ordered by full internal-key order
- for `Delete`, `value_offset_or_zero` and `value_length` MUST both be zero
- key bytes and optional value bytes are packed from the block end backward in reverse slot order
- the block checksum covers the full block bytes with bytes `32..35` zeroed

#### 8.5.4 In-file search semantics

Point lookup in one shared data file for user key `k` and snapshot seqno `S`:

1. descend the B+Tree by full internal-key order using search key `(k, S, Delete)`
2. in the reached data leaf and any immediate successor leaves needed for the same user
   key, find the first record whose `user_key = k` and `seqno <= S`
3. if that record is `Put`, return its value
4. if that record is `Delete`, return not found
5. if no such record exists, return not found

Worked example:

```text
data file records for "yak":
  ("yak", 9, Delete)
  ("yak", 8, Put, "v2")
  ("yak", 3, Put, "v1")

At snapshot 8:
  first seqno <= 8 is Put("v2")
At snapshot 9:
  first seqno <= 9 is Delete
```

ASCII diagram:

```text
root internal page -> leaf page -> possible next leaf for same user key
```

Normative pseudocode:

```text
function find_visible_record_in_file(data_file, user_key, snapshot_seqno):
    search_key = {user_key = user_key, seqno = snapshot_seqno, kind = Delete}
    leaf = descend_to_data_leaf(data_file, search_key)

    while leaf != None:
        for each slot in leaf.slots in internal-key order:
            if slot.user_key != user_key:
                if slot.user_key > user_key:
                    return None
                continue
            if slot.seqno <= snapshot_seqno:
                return slot
        if leaf.next_leaf_block_id_or_none == NONE_U64:
            return None
        leaf = load_block_by_id(data_file, leaf.next_leaf_block_id_or_none)
```

#### 8.5.5 Output-file publication protocol

Shared data files are published only by flush and compaction.

Protocol:

1. write each output file to a temporary path not matching the canonical filename pattern
2. fsync each temporary file
3. allocate unique unused `FileId` values
4. rename each temporary file to `data/<file_id_20d>.kjm`
5. fsync the `data/` directory
6. append the `FlushPublish` or `CompactPublish` WAL record
7. install the next shared data manifest snapshot

If the data files are durable but the WAL publish record is not accepted, those files remain orphan
files and MUST remain invisible.

#### 8.5.6 File immutability

Once a shared data file is published, it MUST NOT be modified in place.

### 8.6 Metadata checkpoint `.kjm` files

#### 8.6.1 Filename

Canonical filename:

`meta/meta-<checkpoint_generation_20d>.kjm`

#### 8.6.2 Header extension

Metadata checkpoint files use `file_kind = MetadataCheckpoint` and `header_bytes = 192`.

Bytes `128..191` in the header page are the metadata extension:

| Offset | Size | Field |
| --- | --- | --- |
| `128` | `8` | `checkpoint_max_seqno` |
| `136` | `8` | `next_seqno` |
| `144` | `8` | `next_file_id` |
| `152` | `4` | `manifest_entry_count` |
| `156` | `4` | `logical_shard_count` |
| `160` | `8` | `checkpoint_data_generation` |
| `168` | `24` | `reserved_zero` |

Rules:

- `logical_bytes_total = 0`
- `root_block_id_or_none = NONE_U64`
- `metadata_generation_or_zero = checkpoint_generation`
- `checkpoint_data_generation` is the current shared-data generation represented by the checkpoint
- the checkpoint represents the exact visible state at `checkpoint_max_seqno`

#### 8.6.3 Metadata list blocks

Metadata checkpoints are linear lists, not routing trees.

Two block kinds are used:

- `FileManifestBlock = 3`
- `LogicalShardBlock = 4`

Each block uses the following fixed 64-byte prefix:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `1` | `block_kind` |
| `1` | `1` | `reserved_zero` |
| `2` | `2` | `reserved_zero` |
| `4` | `4` | `entry_count` |
| `8` | `4` | `block_span_pages` |
| `12` | `4` | `reserved_zero` |
| `16` | `2` | `variable_bytes_begin_offset` |
| `18` | `2` | `reserved_zero` |
| `20` | `4` | `variable_bytes_total` |
| `24` | `8` | `next_block_id_or_none` |
| `32` | `4` | `block_crc32c` |
| `36` | `28` | `reserved_zero` |

Each metadata slot is 8 bytes:

| Offset | Size | Field |
| --- | --- | --- |
| `0` | `4` | `value_offset` |
| `4` | `4` | `value_length` |

Rules:

- `FileManifestBlock` values are `FileMetaWire` values sorted by `(level_no, file_id)`
- `LogicalShardBlock` values are `LogicalShardEntryWire` values sorted by `start_bound`
- manifest blocks MUST come before logical shard blocks in block-id order
- the current logical shard entries stored in the checkpoint MUST be disjoint and contiguous

Worked example:

```text
Checkpoint file block order:
  block 1: FileManifestBlock
  block 2: FileManifestBlock
  block 3: LogicalShardBlock
```

ASCII diagram:

```text
header -> manifest block chain -> logical-shard block chain -> footer
```

Normative pseudocode:

```text
function validate_metadata_list_block(block_bytes):
    block = parse_metadata_list_block(block_bytes)
    assert_invariant(
        block.block_kind == FileManifestBlock or
        block.block_kind == LogicalShardBlock,
        Corruption)
    assert_invariant(
        crc32c(zero_range(block_bytes, 32, 4)) == block.block_crc32c,
        Checksum)
    return block
```

## 9. External Operations

### 9.1 `open(config_path)` (`FR-OPEN`)

Why it exists:

`open()` establishes the initial shared data manifest, the current logical-shard map, the retained
generation-0 empty data manifest snapshot, the current durable checkpoint frontier, and one current
shared-data generation whose visible source set is fully defined by Sections 6.4 and 6.4A.

Behavior:

1. parse and validate `config.toml` [BEHAVIORAL]
2. load and validate `CURRENT` if it exists [BEHAVIORAL]
3. load the referenced metadata checkpoint if `CURRENT` exists [BEHAVIORAL]
4. initialize the replay state from the checkpoint or from a one-range empty state [BEHAVIORAL]
5. replay WAL records after `checkpoint_max_seqno` [BEHAVIORAL]
6. materialize the recovered current shared-data generation [BEHAVIORAL]
7. create `empty_data_manifest_snapshot` for generation `0` [BEHAVIORAL]
8. make the engine ready for reads and writes [BEHAVIORAL]

Recovery rules:

- if `CURRENT` is absent and there are no WAL segments and no metadata files, `open()` starts from
  one logical shard `[-inf, +inf)` and an empty shared data state
- if `CURRENT` is absent and WAL segments exist, `open()` replays from that one-range empty state
- if `CURRENT` is absent and metadata checkpoint files exist but no WAL segment exists, `open()`
  MUST fail with `Corruption`
- metadata checkpoints not referenced by `CURRENT` are orphan files and MUST NOT affect visible
  state
- data files not referenced by the recovered current data manifest are orphan files and MUST NOT
  affect visible state

Worked example:

```text
No CURRENT
One WAL record:
  Put("ant", "v1")

open():
  starts from one current logical shard [-inf, +inf)
  replays the Put
  becomes ready with the recovered current logical-shard map
```

ASCII diagram:

```text
config -> CURRENT? -> checkpoint? -> WAL replay -> ready
```

Normative pseudocode:

```text
function open_engine(config_path):
    config_bytes = read_exact_file(config_path)
    config = parse_toml(config_bytes)
    validate_config(config)

    current_file = maybe_load_current()
    checkpoint_state = load_checkpoint_if_named(current_file)
    replay_records = load_replay_records_after(current_file)

    state = initialize_state_from_checkpoint(checkpoint_state)
    replay_wal_from_state(state, replay_records)
    install_current_manifest_from_replay_state(state)
    state.empty_data_manifest_snapshot = make_empty_data_manifest_snapshot()
    return Ok(state)
```

Complexity:

- `open()` is `Theta(B_cp + R_replay * T_apply + B_pub)` time, where `B_cp` is the checkpoint bytes
  read, `R_replay` is the replayed WAL record count, `T_apply` is the per-record apply cost, and
  `B_pub` is the validation bytes read from referenced data files

### 9.2 `create_snapshot()` (`FR-SNAPSHOT-CREATE`)

Behavior:

- capture `snapshot_seqno = last_committed_seqno`
- capture `data_generation = current data_generation`
- allocate `snapshot_id`
- store the snapshot entry in `active_snapshots`

Worked example:

```text
Before create_snapshot():
  last_committed_seqno = 500
  data_generation = 12

Returned handle:
  snapshot_seqno = 500
  data_generation = 12
```

ASCII diagram:

```text
snapshot handle pins:
  seqno + data_generation
```

Normative pseudocode:

```text
function create_snapshot(state):
    handle = {
        engine_instance_id = state.engine_instance_id,
        snapshot_id = state.next_snapshot_id,
        snapshot_seqno = state.last_committed_seqno,
        data_generation = state.data_generation
    }
    state.next_snapshot_id <- state.next_snapshot_id + 1
    state.active_snapshots[handle.snapshot_id] <- handle
    return Ok(handle)
```

Complexity:

- `create_snapshot()` is `Theta(1)` time and `Theta(1)` extra RAM

### 9.3 `release_snapshot(handle)` (`FR-SNAPSHOT-RELEASE`)

Behavior:

- validate the handle against `active_snapshots`
- remove the tracked entry from `active_snapshots`
- a second release of the same handle MUST return `InvalidArgument`

Worked example:

```text
release_snapshot(A) once:
  tracked entry is removed

release_snapshot(A) twice:
  InvalidArgument
```

ASCII diagram:

```text
present -> removed
removed -> InvalidArgument
```

Normative pseudocode:

```text
function release_snapshot(state, handle):
    validate_snapshot_handle(state, handle)
    remove state.active_snapshots[handle.snapshot_id]
    return Ok(())
```

Complexity:

- `release_snapshot()` is `Theta(1)` time and `Theta(1)` extra RAM

### 9.4 `put(key, value)` (`FR-PUT`)

Why it exists:

`put` adds one new visible version to the shared data plane. Logical-shard size metadata may be
refreshed later.

Behavior:

1. validate key and value lengths
2. append one `Put` WAL record
3. insert one `Put` record into the shared active memtable
4. trigger memtable freeze or background maintenance if thresholds now require it

Worked example:

```text
put("yak", "value-two"):
  appends to WAL
  inserts into the active memtable
  leaves logical-shard byte refresh to later maintenance
```

ASCII diagram:

```text
Put
  -> WAL
  -> active memtable
```

Normative pseudocode:

```text
function put(state, key, value):
    validate_key_and_value(key, value)

    append_result = append_wal_record(
        state, Put, encode_put_payload(key, value), state.config.engine.sync_mode)
    if append_result is Err(error):
        return Err(error)

    insert_put_into_active_memtable(state, append_result.value.seqno, key, value)
    state.last_committed_seqno <- append_result.value.seqno
    return Ok(())
```

Complexity:

- `put()` is `Theta(|key| + |value|)` owner-thread time and `Theta(1)` extra RAM beyond the
  inserted memtable record

### 9.5 `delete(key)` (`FR-DELETE`)

Behavior:

1. validate key length
2. append one `Delete` WAL record
3. insert one `Delete` tombstone into the shared active memtable

Worked example:

```text
delete("yak"):
  appends a tombstone to WAL and the active memtable
```

ASCII diagram:

```text
Delete
  -> WAL
  -> active memtable tombstone
```

Normative pseudocode:

```text
function delete(state, key):
    validate_key(key)

    append_result = append_wal_record(
        state, Delete, encode_delete_payload(key), state.config.engine.sync_mode)
    if append_result is Err(error):
        return Err(error)

    insert_delete_into_active_memtable(state, append_result.value.seqno, key)
    state.last_committed_seqno <- append_result.value.seqno
    return Ok(())
```

Complexity:

- `delete()` is `Theta(|key|)` owner-thread time and `Theta(1)` extra RAM beyond the inserted
  memtable record

### 9.6 `get(key, snapshot?)` (`FR-GET`)

Why it exists:

`get` reads one user key from the shared data plane at one snapshot. It MUST ignore logical-shard
metadata except for argument validation unrelated to visibility.

Behavior:

- resolve the read snapshot:
  - if the caller supplies a snapshot handle, use its `snapshot_seqno` and `data_generation`
  - otherwise use the current `last_committed_seqno` and current `data_generation`
- read order:
  1. the pinned generation's `active_memtable_ref`
  2. the pinned generation's `frozen_memtable_refs[]` in descending `frozen_memtable_id`
  3. all candidate L0 files from the pinned generation in descending `file_id`
  4. at most one file per higher level from the pinned generation
- return the first visible `Put` value or not found if the first visible record is `Delete` or no
  visible record exists

Worked example:

```text
Snapshot A:
  snapshot_seqno = 500
  data_generation = 12

Later logical split:
  current logical-shard map changes

get("yak", snapshot A):
  still reads the shared data state at seq 500 and generation 12
```

ASCII diagram:

```text
get()
  -> snapshot resolution
  -> memtables
  -> L0 files
  -> one file per higher level
```

Normative pseudocode:

```text
function get(state, key, snapshot_handle_or_none):
    snapshot = resolve_snapshot_for_read(state, snapshot_handle_or_none)
    manifest = manifest_for_generation(state, snapshot.data_generation)

    record = find_visible_record_in_one_memtable(
        manifest.active_memtable_ref, key, snapshot.snapshot_seqno)
    if record != None:
        return visible_result(record)

    record = find_visible_record_in_memtable_list(
        manifest.frozen_memtable_refs, key, snapshot.snapshot_seqno)
    if record != None:
        return visible_result(record)

    for each file_meta in files_covering_key(manifest, key):
        record = find_visible_record_in_file(
            load_data_file(file_meta), key, snapshot.snapshot_seqno)
        if record != None:
            return visible_result(record)

    return Ok(NotFound)
```

Complexity:

- `get()` is `Theta(T_mem + F0 * H_file + Lh * H_file)` time and `Theta(1)` extra RAM, where
  `T_mem` is the memtable probe cost, `F0` is the current L0 candidate count, `Lh` is the higher
  level count examined, and `H_file` is one in-file tree descent cost

### 9.7 `scan(range, snapshot?)` (`FR-SCAN`)

Behavior:

- resolve the read snapshot exactly as in `get()`
- load the pinned generation identified by the resolved `data_generation`
- create iterators over:
  - the pinned generation's `active_memtable_ref`
  - the pinned generation's `frozen_memtable_refs[]`
  - the pinned generation's data files whose key range overlaps the scan range
- merge those iterators by internal-key order
- suppress shadowed versions and tombstones according to `snapshot_seqno`
- emit user keys in ascending user-key order

Worked example:

```text
scan(["ant", "yak"), snapshot A):
  returns the same keys before and after a later logical merge,
  because the scan uses the pinned shared data manifest and seqno only
```

ASCII diagram:

```text
scan()
  -> iterator merge over shared sources
  -> version suppression
  -> ascending user-key output
```

Normative pseudocode:

```text
function scan(state, range, snapshot_handle_or_none):
    snapshot = resolve_snapshot_for_read(state, snapshot_handle_or_none)
    manifest = manifest_for_generation(state, snapshot.data_generation)
    iterators = build_scan_iterators(manifest, range, snapshot)
    return merge_scan_iterators(iterators, snapshot.snapshot_seqno)
```

Complexity:

- `scan()` is `Theta(N_scan * log K_scan)` time and `Theta(K_scan)` extra RAM, where `N_scan` is the
  number of internal records examined and `K_scan` is the iterator count merged

### 9.8 `sync()` (`FR-SYNC`)

Behavior:

- fsync the active WAL segment
- if the active WAL segment was already durable up to the current append frontier, `sync()` MAY
  return success without writing additional bytes

Worked example:

```text
sync() after two acknowledged writes in manual mode:
  both writes become durable together
```

ASCII diagram:

```text
manual mode
  append -> append -> sync -> durable frontier advances
```

Normative pseudocode:

```text
function sync(state):
    fsync(state.active_wal_segment)
    return Ok(())
```

Complexity:

- `sync()` is `Theta(1)` owner-thread work plus one WAL fsync

### 9.9 `stats()` (`FR-STATS`)

Why it exists:

`stats()` reports engine-wide shared-data information and the current logical-shard map.

Worked example:

```text
stats():
  levels[] report global shared data file totals
  logical_shards[] report the current tracked range totals
```

ASCII diagram:

```text
levels[] + logical_shards[]
```

Normative pseudocode:

```text
function stats(state):
    response = {
        observation_seqno = state.last_committed_seqno,
        data_generation = state.data_generation,
        levels = build_global_level_stats(state.current_data_manifest),
        logical_shards = state.current_logical_shards
    }

    return Ok(response)
```

Complexity:

- `stats()` is `Theta(L_max + S_log)` time and `Theta(L_max + S_log)` extra RAM, where `L_max` is
  `max_levels` and `S_log` is the current logical-shard count

## 10. Internal Operations

### 10.1 Freeze active memtable (`IM-FREEZE`)

Why it exists:

Freeze moves the mutable shared memtable into the immutable background-flush set.

Behavior:

- if the active memtable is empty, do nothing
- otherwise assign a new `frozen_memtable_id`, move the active memtable to the frozen set, and
  create a fresh empty active memtable
- a successful non-empty freeze MUST install the next `data_generation`

Worked example:

```text
active memtable reaches memtable_flush_bytes
-> freeze
-> new empty active memtable
```

ASCII diagram:

```text
active memtable -> frozen memtable queue -> flush
```

Normative pseudocode:

```text
function freeze_active_memtable(state):
    if state.active_memtable.entry_count == 0:
        return Ok(None)
    frozen = state.active_memtable
    frozen.frozen_memtable_id <- state.next_frozen_memtable_id
    state.next_frozen_memtable_id <- state.next_frozen_memtable_id + 1
    install_generation_after_freeze(state, frozen)
    return Ok(frozen)
```

Complexity:

- `freeze_active_memtable()` is `Theta(1)` time and `Theta(1)` extra RAM

### 10.2 Flush frozen memtable (`IM-FLUSH`)

Why it exists:

Flush turns one immutable shared memtable into immutable L0 data files and advances the shared data
manifest.

Behavior:

1. choose the oldest frozen memtable
2. rewrite its contents into sorted L0 output files sized by `target_file_bytes(L0)`
3. write those output files to temporary paths
4. at publish time, validate:
   - current `data_generation == data_generation_expected`
   - the chosen source frozen memtable still matches the oldest frozen source identified by
     `source_first_seqno`, `source_last_seqno`, and `source_record_count`
5. allocate unique unused `FileId` values
6. rename the temporary files to canonical filenames
7. append one `FlushPublish` record
8. install the next data manifest snapshot

Worked example:

```text
Frozen memtable 21:
  90 MiB of sorted internal records

Flush:
  produces two L0 files
  installs data_generation + 1
```

ASCII diagram:

```text
frozen memtable -> temp files -> canonical data files -> FlushPublish -> new data manifest
```

Normative pseudocode:

```text
function flush_one_frozen_memtable(state, frozen):
    outputs = build_l0_files_from_frozen_memtable(state, frozen)
    update = {
        data_generation_expected = state.data_generation,
        source_first_seqno = frozen.min_seqno,
        source_last_seqno = frozen.max_seqno,
        source_record_count = frozen.entry_count
    }
    publish_data_manifest_update(state, FlushPublish, update, outputs)
```

Complexity:

- `IM-FLUSH` is `Theta(N_frozen + B_out)` time and `Theta(B_build)` extra RAM, where `N_frozen` is
  the frozen memtable record count, `B_out` is the output file bytes written, and `B_build` is any
  temporary builder memory

### 10.3 Shared-data maintenance (`IM-COMPACT`)

The exact scoring, input-selection, and file-sizing policy for shared-data maintenance is
implementation-defined in v1.

Rules:

- the engine SHOULD compact when needed to preserve the levelled LSM invariant and limit L0 buildup
- any compaction input set and output grouping is acceptable if the resulting published manifest
  preserves the read semantics in Sections 9 and 11
- an accepted `CompactPublish` MUST durably name the exact input file set being removed from the
  current generation
- published output files MUST remain internally sorted by internal-key order
- levels `>= 1` MUST remain strictly ordered and non-overlapping by user-key range after publish
- compaction publication MUST follow the same crash-safety rules as any other shared-data publish:
  unreferenced output files remain orphan and invisible until the publish record is accepted

### 10.6 Logical-shard maintenance (`IM-LOGICAL`)

Why it exists:

Logical-shard maintenance updates the latest-only range map without rewriting any shared data
files.

Rules:

- the owner thread MAY attempt split or merge work under any implementation-defined scheduling or
  triggering policy
- the owner thread MAY recompute exact `live_size_bytes` for candidate source or output entries,
  but v1 does not require exact live-byte accounting for correctness
- a split MUST replace exactly one current logical-shard entry with exactly two contiguous output
  entries that preserve the same outer bounds
- a merge MUST replace exactly two adjacent current logical-shard entries with exactly one output
  entry that preserves the same outer bounds
- split-boundary choice, merge pairing, and any size-based heuristics are not cross-implementation
  compatibility requirements in v1
- logical maintenance MUST publish only `LogicalShardInstall` and MUST NOT create, rewrite, or
  publish shared data files
- if the chosen source entries no longer match the current logical-shard map at install time, the
  install MUST fail with `Stale`

Worked example:

```text
Current logical shards:
  [-inf, +inf) -> 2300 MiB

Possible split:
  source  = [[-inf, +inf)]
  outputs = [[-inf, "moose"), ["moose", +inf)]

Possible merge:
  source  = [[-inf, "m"), ["m", +inf)]
  output  = [[-inf, +inf)]
```

ASCII diagram:

```text
split: one entry -> two entries
merge: two adjacent entries -> one entry
```

Complexity:

- implementation-defined in v1; any implementation MUST still preserve the publication, bounds,
  and stale-check rules above

### 10.10 Garbage collection (`IM-GC`)

Why it exists:

Garbage collection reclaims orphan or obsolete shared data files and old in-memory data manifest
snapshots. Logical shard installs themselves create no data-file garbage.

Rules:

- a data file is collectible only if:
  1. it is not referenced by the current shared data manifest
  2. it is not referenced by any snapshot-pinned historical data manifest
  3. it is not required by the durable checkpoint referenced by `CURRENT`
- old in-memory data manifest snapshots are collectible only when no snapshot or background job
  references them
- old logical-shard maps are not snapshot-pinned and MAY be freed immediately after a new map is
  installed and no local job still references the old entries

Worked example:

```text
Compaction publishes new data files.
Old source files remain:
  pinned by one active snapshot -> not yet collectible
After that snapshot is released:
  collectible
```

ASCII diagram:

```text
current manifest refs + snapshot refs + durable checkpoint refs -> GC eligibility
```

Normative pseudocode:

```text
function garbage_collect(state):
    for each file in candidate_data_files(state) in stable filename order:
        if data_file_is_gc_eligible(state, file):
            delete(file.path)

    for each manifest in historical_data_manifests(state):
        if manifest.refcount == 0:
            free_manifest_snapshot(manifest)
```

Complexity:

- `garbage_collect(...)` is `Theta(F_gc + M_gc)` time and `Theta(1)` extra RAM, where `F_gc` is the
  candidate data-file count and `M_gc` is the retained historical data-manifest count

### 10.11 Metadata checkpointing (`IM-CHECKPOINT`)

Why it exists:

Checkpointing captures one durable shared-data manifest plus one durable current logical-shard map
so that later `open()` can start from a compact durable frontier instead of replaying all WAL bytes.

Behavior:

1. choose a mutation-coverage target `S_target`
2. defer non-required flush, compaction, and logical-install publications
3. freeze the active memtable if it is non-empty
4. wait until every current frozen memtable is flushed and its publish is accepted
5. assert that the current generation has zero frozen memtables and an empty active memtable
6. enter a checkpoint-capture pause before any new seqno-allocating owner-thread operation changes
   the visible shared data state or the current logical-shard map
7. let `S_cp = last_committed_seqno`
8. capture the current shared data manifest, current logical-shard map, and allocator state exactly
   as of `S_cp`
9. leave the checkpoint-capture pause
10. write `meta-<checkpoint_generation>.kjm`
11. install `CURRENT`
12. optionally truncate closed WAL segments now fully covered by the checkpoint

Rules:

- the captured allocator state includes `next_seqno` and `next_file_id`
- the captured shared-data manifest MUST contain only published files; it MUST contain zero frozen
  memtables and an empty active memtable
- the captured checkpoint MUST include the current logical-shard map visible at `S_cp`
- a checkpoint is durable only after `CURRENT` is durable
- `CURRENT` is the only authoritative durable checkpoint pointer in v1

Worked example:

```text
seq 300  last mutation that MUST be covered by the checkpoint
seq 301  required FlushPublish

capture checkpoint at S_cp = 301
CURRENT points at that checkpoint
```

ASCII diagram:

```text
freeze/flush all mutable state -> capture file-only state -> write meta -> install CURRENT
```

Normative pseudocode:

```text
function install_checkpoint(state):
    S_target = choose_checkpoint_frontier(state)
    defer_nonrequired_publications(state, S_target)
    if state.active_memtable.entry_count > 0:
        freeze_active_memtable(state)
    wait_until_no_frozen_memtables_remain(state)
    assert_invariant(state.active_memtable.entry_count == 0, Corruption)

    enter_checkpoint_capture_pause(state)
    S_cp = state.last_committed_seqno
    captured = capture_checkpoint_state(state, S_cp)
    leave_checkpoint_capture_pause(state)

    checkpoint_generation = next_checkpoint_generation(state)
    checkpoint_path = write_checkpoint_file(captured, checkpoint_generation)
    current_bytes = build_current_bytes(captured, checkpoint_generation)
    install_current(checkpoint_path, current_bytes)
```

Complexity:

- checkpointing is `Theta(F_cp + S_log + B_cp + B_durable)` time and `Theta(F_cp + S_log + B_cp)`
  extra RAM, where `F_cp` is the checkpoint file-manifest entry count, `S_log` is the current
  logical-shard count, `B_cp` is the emitted checkpoint byte count, and `B_durable` is the durable
  bytes written by the checkpoint file and `CURRENT`

## 11. Behavior Details

### 11.1 Snapshot semantics

Snapshots pin:

- `snapshot_seqno`
- `data_generation`

Snapshots do not pin:

- any historical logical-shard map

Implications:

- `get()` and `scan()` are stable for one snapshot handle even if later logical-shard splits or
  merges happen
- `get()` and `scan()` MUST read exactly the shared-data source set named by the pinned
  `data_generation`
- `stats()` always reports the latest current logical-shard map, not a historical one

Worked example:

```text
Snapshot A captures:
  snapshot_seqno = 500
  data_generation = 12

Later:
  one current logical shard is split into two

Snapshot A:
  still reads data_generation 12
  does not read historical logical-shard metadata
```

ASCII diagram:

```text
snapshot A ----> data_generation 12
latest state --> current logical-shard map
```

Normative pseudocode:

```text
function resolve_snapshot_for_read(state, handle_or_none):
    if handle_or_none == None:
        return {
            snapshot_seqno = state.last_committed_seqno,
            data_generation = state.data_generation
        }
    validate_snapshot_handle(state, handle_or_none)
    return {
        snapshot_seqno = handle_or_none.snapshot_seqno,
        data_generation = handle_or_none.data_generation
    }
```

### 11.2 Publication semantics

Shared data publishes and logical-shard installs are independent: [BEHAVIORAL]

- non-empty freeze, `FlushPublish`, and `CompactPublish` each create one next shared
  `data_generation`
- `FlushPublish` and `CompactPublish` also consume one WAL seqno and become durable by WAL replay
- `LogicalShardInstall` updates only the current logical-shard map
- shared data publishes change snapshot-visible read state
- logical-shard installs change only the latest current range metadata

Worked example:

```text
seq 600 CompactPublish -> data_generation 14
seq 601 LogicalShardInstall -> current logical_shards[] updated

Old snapshot pinned to data_generation 13:
  sees data_generation 13
  does not care about the current logical-shard metadata
```

ASCII diagram:

```text
shared data publish:    affects read snapshots
logical metadata:       affects current logical range map only
```

Normative pseudocode:

```text
function publish_data_manifest_update(state, record_type, update, outputs):
    assert_invariant(state.data_generation == update.data_generation_expected,
                     Stale)
    payload = encode_data_publish_payload(record_type, update, outputs)
    append_result = append_wal_record(
        state, record_type, payload, state.config.engine.sync_mode)
    if append_result is Err(error):
        return Err(error)
    if record_type == FlushPublish:
        source_frozen = oldest_frozen_memtable_matching_source_fields(state, update)
        install_generation_after_flush_publish(state, source_frozen, outputs)
    else:
        install_generation_after_compact_publish(state, update.input_file_ids, outputs)
    state.last_committed_seqno <- append_result.value.seqno
    return Ok(())
```

### 11.4 Error classes

- `InvalidArgument`: invalid key, value, range, config, request shape, or snapshot handle
- `IO`: required filesystem I/O failed, the engine is not open, or the live append path failed
- `Checksum`: checksum validation failed
- `Corruption`: a durable artifact violated a byte rule or an invariant that should already hold in
  durable state
- `Stale`: a background result no longer matches the current publish preconditions
- `Cancelled`: a scan or background task was cancelled before completion

Worked example:

```text
Bad CURRENT checksum -> Corruption
Stale compaction publish -> Stale
```

ASCII diagram:

```text
caller error | durable-state error | transient stale/background error
```

Normative pseudocode:

```text
function normalize_error(error_class):
    assert_invariant(
        error_class in {
            InvalidArgument, IO, Checksum,
            Corruption, Stale, Cancelled
        },
        Corruption)
```

### 11.5 Stale background-result rejection

Shared-data background work uses simple stale checks in v1:

- flush:
  - current `data_generation` MUST equal `data_generation_expected`
  - the oldest current frozen memtable MUST still match `source_first_seqno`, `source_last_seqno`,
    and `source_record_count`
- compaction:
  - current `data_generation` MUST equal `data_generation_expected`
  - the exact payload `input_file_ids[]` MUST still exist in the current generation
- logical split and merge:
  - the current source logical-shard entries MUST still equal the source entry set being replaced

Worked example:

```text
Compaction planned at data_generation 14:
  any later accepted data publish makes it stale

Logical split planned for current source entries:
  a later logical install that changes those entries makes it stale
```

ASCII diagram:

```text
data generation changed: stale reject
publish source changed:  stale reject
range change:            stale reject
```

Normative pseudocode:

```text
function flush_job_is_stale(state, update):
    if state.data_generation != update.data_generation_expected:
        return true
    return not oldest_frozen_memtable_matches_source_fields(state, update)

function compact_job_is_stale(state, update):
    if state.data_generation != update.data_generation_expected:
        return true
    return not all_input_file_ids_present(state.current_data_manifest,
                                          update.input_file_ids)

function logical_job_is_stale(state, captured_job):
    return not current_source_entries_equal(
        state.current_logical_shards, captured_job.source_entries)
```

## 12. Edge Cases and Crash Windows

### 12.1 Edge cases

- open without a recovered logical-shard map initializes one current logical shard `[-inf, +inf)`
- delete of a missing key leaves the true logical-shard bytes unchanged; a later recomputation
  preserves that value
- if only one current logical shard exists, there is no adjacent pair to merge
- split is infeasible when the implementation cannot produce a valid two-entry replacement for the
  chosen source entry
- merge is infeasible when the implementation cannot produce a valid one-entry replacement for the
  chosen adjacent source pair
- logical split and merge do not change `get()` or `scan()` results for any fixed snapshot
- orphan data files and orphan metadata checkpoints MUST NOT affect visible state during `open()`

Worked example:

```text
Only one logical shard exists:
  [-inf, +inf) -> 300 MiB

No merge is possible because there is no adjacent source pair.
```

ASCII diagram:

```text
one current logical shard

  [-inf, +inf)
```

### 12.2 Crash-window matrix

Crash windows that MUST recover deterministically:

- crash point `before first WAL segment header`:
  - durable outcome: no WAL
  - recovery outcome: one-range empty state
- crash point `after full Put/Delete record bytes reach WAL, before memtable insert`:
  - durable outcome: mutation WAL record present
  - recovery outcome: replay re-applies the mutation
- crash point `after shared data output rename and directory fsync, before FlushPublish or
  CompactPublish WAL record`:
  - durable outcome: canonical data files exist but no publish record
  - recovery outcome: files are orphan and remain invisible
- crash point `after LogicalShardInstall WAL record is durable, before in-memory logical map swap`:
  - durable outcome: logical metadata update is committed
  - recovery outcome: replay installs the new current logical-shard map
- crash point `after metadata checkpoint write, before CURRENT update`:
  - durable outcome: new checkpoint file only
  - recovery outcome: older `CURRENT` remains authoritative
- crash point `after CURRENT update, before WAL truncation`:
  - durable outcome: new checkpoint is authoritative
  - recovery outcome: replay starts after `CURRENT.checkpoint_max_seqno`

Worked example:

```text
Logical split:
  crash after LogicalShardInstall WAL durability but before in-memory swap

Recovery:
  replay sees the durable record
  installs the new current logical shard map
```

ASCII diagram:

```text
durable record? yes -> replay installs it
durable record? no  -> old state remains authoritative
```

Normative pseudocode:

```text
function crash_recovery_outcome(has_full_valid_record):
    if has_full_valid_record:
        return CommittedOnReplay
    return NotCommitted
```

## 13. Acceptance Criteria and Test Matrix

### 13.1 Acceptance criteria

This section is normative.

The engine is correct only if all of the following hold:

1. the shared data plane is independent from the logical-shard map [BEHAVIORAL]
2. the current logical-shard map is range-only and contains no numeric shard identifier [BEHAVIORAL]
3. `open()` initializes one current logical shard `[-inf, +inf)` when recovery does not load a
   checkpointed logical-shard map [BEHAVIORAL]
4. `Put` and `Delete` WAL payloads contain no logical-shard identifier [BEHAVIORAL]
5. every committed WAL record consumes exactly one seqno and is recovered if and only if one full
   valid record survives [BEHAVIORAL]
6. the current `data_generation` changes only when the shared read-source set changes; non-empty
   freeze changes it in memory, and shared data publishes change it only after their WAL publish
   record is accepted [BEHAVIORAL]
7. logical split and merge become visible only after their `LogicalShardInstall` WAL record is
   accepted [BEHAVIORAL]
8. logical split and merge are metadata-only and do not write or rewrite data files [BEHAVIORAL]
9. `get()` and `scan()` ignore logical-shard history and use only `snapshot_seqno` plus the exact
    shared-data source set named by pinned `data_generation` [BEHAVIORAL]
10. `stats()` reports the latest current logical-shard map, not a historical one [BEHAVIORAL]
11. current logical-shard ranges are always disjoint, contiguous, and sorted by start bound [BEHAVIORAL]
12. `live_size_bytes` is advisory metadata and need not be exact for read correctness [BEHAVIORAL]
13. checkpoint capture persists the current published-file manifest and current logical-shard map
    only after draining mutable shared-data state so the checkpointed files represent the exact
    visible shared-data state [BEHAVIORAL]
14. orphan data files remain invisible [BEHAVIORAL]
15. stale background data publishes are rejected by current-generation preconditions plus exact
    flush-source or compaction-input-set checks [BEHAVIORAL]
16. stale logical split and merge results are rejected by exact source-range checks [BEHAVIORAL]
17. the current logical-shard map transitions atomically on every install: when a
    `LogicalShardInstall` WAL record is accepted, the full source entry set is replaced by the full
    output entry set with no intermediate state visible to concurrent reads or writes [BEHAVIORAL]
18. no background operation creates, renames, or modifies any data file in response to a logical
    split or merge; such operations affect only the durable WAL record and the in-memory current
    logical-shard map [BEHAVIORAL]

Worked example:

```text
Accepted logical split:
  changes stats().logical_shards[]
  does not change snapshot read results
  does not publish any new data file
```

ASCII diagram:

```text
correctness triangle:

  durable bytes
  live behavior
  recovery behavior
```

Normative pseudocode:

```text
function acceptance_violation(message):
    abort specification conformance with message
```

### 13.2 Required tests

Required tests MUST include at least:

- `Put` and `Delete` WAL payload encoding with no logical-shard identifier
- `open()` without a recovered logical-shard map initializes `[-inf, +inf)`
- `release_snapshot()` rejects a second release of the same handle with `InvalidArgument`
- `get()` ignores logical-shard changes for a fixed snapshot handle
- `scan()` ignores logical-shard changes for a fixed snapshot handle
- snapshot pinned before a freeze and later flush continues to read the pre-freeze source set
- flush publishes global shared data files and advances only `data_generation`
- compaction publishes global shared data files and advances only `data_generation`
- `FlushPublish` replay removes only the pending mutation prefix named by
  `source_first_seqno`, `source_last_seqno`, and `source_record_count`
- `CompactPublish` replay removes exactly the payload `input_file_ids[]`
- logical split publishes only `LogicalShardInstall` and no shared data files
- logical merge publishes only `LogicalShardInstall` and no shared data files
- stale logical split reject when the source range set changed
- stale flush reject when the data generation changed
- stale flush reject when the oldest frozen source no longer matches the payload source fields
- stale compaction reject when the exact input file set no longer matches the current generation
- checkpoint round-trip of the shared data manifest plus current logical-shard map after draining
  mutable shared-data state
- replay of `Put` and `Delete` preserves the shared data plane and latest logical-shard map
- orphan data file is ignored during `open()`
- crash after durable `LogicalShardInstall` but before in-memory swap recovers the new logical map
- `stats()` returns global `levels[]` and current `logical_shards[]` only

Worked example:

```text
Test:
  split one logical shard into two

Assertions:
  no new data file appears
  current logical_shards[] changes
  get()/scan() results for an old snapshot are unchanged
```

ASCII diagram:

```text
tests:
  encoding | live behavior | replay | crash recovery | stats
```

Normative pseudocode:

```text
function required_test_names():
    return set_of_required_tests_listed_in_this_section
```

## 14. Write-Amplification Design Target

This section is informative rather than normative.

For v1, the user-facing data path is expected to be dominated by WAL writes for `Put` and `Delete`
plus shared data-file bytes written by flush and compaction. Metadata-only bytes from logical-shard
installs and checkpoint maintenance are control-plane overhead and are outside the user-facing
target.

The intended steady-state design target is an amortized write amplification on the order of
`1 + max_levels`, but this is guidance only and is not part of conformance for v1. [PERF]

--- APPENDIX: OUT OF SCOPE FOR NOW ---
[11.3] `get()` and `scan()` are idempotent for a fixed explicit snapshot handle — revisit when distributed scaling requires it
[11.3] a stale flush or compaction result MAY be rebuilt against the latest shared data manifest and retried — revisit when distributed scaling requires it
[11.3] retry MUST NOT reuse a seqno from a failed live append attempt — revisit when distributed scaling requires it
