# Write-Optimised Storage for Driver Location & Trip Events — LSM Tree

DSA case study. **Primary data structure: LSM Tree.** Pure Python 3 (standard library only — no `pip install`).

## Run it

```bash
python run.py                 # then open http://127.0.0.1:8000
python -m unittest discover -s tests -v      # 11 tests
```
Options: `--port 9000`, `--data-dir mydata`, `--memtable-kb 32`.

**Demo script (3 minutes):** press *Start simulator* → watch the WAL/MemTable bar fill, SSTables appear in L0 and
merge into L1/L2 → press *Run write benchmark* (≈10k+ LSM writes/sec) → in *Key explorer* GET `driver:latest:D005`
(hit in MemTable) and `driver:latest:D999` (watch Bloom filters / range checks skip every table) →
*Go offline* on a driver (writes a tombstone) → stop the Python process with Ctrl+C or kill it, start again: the data is still there (WAL + MANIFEST recovery).

## Team split (backend) — one file each, clean interfaces

| Member | File | Owns | Syllabus topics |
|---|---|---|---|
| **1** | `lsm/member1_memtable_wal.py` | **Skip List**, **MemTable**, **Write-Ahead Log** (the whole write path front-end) | Skip lists, linked structures, complexity |
| **2** | `lsm/member2_sstable_bloom.py` | **Bloom filter**, **SSTable** writer/reader, sparse index + binary search | Bloom filter, hashing, arrays, binary search, external-memory searching |
| **3** | `lsm/member3_compaction.py` | **Manifest**, level set, **k-way merge using a min-heap**, **compaction** (size-tiered), tombstone GC | Heaps / priority queues, merge (external sorting), complexity |
| **4** | `lsm/member4_engine.py`, `member4_ride_service.py`, `member4_server.py` | **LSM engine** (put/get/delete/scan, flush, crash recovery), **ride-hailing domain layer + simulator**, **HTTP API** | Map/Dictionary ADT, key design, ordered range scans |

`lsm/common.py` (config + entry format) is shared. `frontend/index.html` is shared (not split).
`tests/test_lsm.py` has a test class per member.

### Interfaces between members (what each calls from the others)
```
Member 4 engine ──put──▶ M1 WAL.append + M1 MemTable.put
                 ──full─▶ M2 write_sstable(memtable.items())  → M3 LevelSet.add → M3 Compactor.maybe_compact
                 ──get──▶ M1 MemTable.get → for table in M3 LevelSet.read_order(): M2 bloom + SSTable.get
                 ──scan─▶ M3 kway_merge([M1 memtable iter, M2 SSTable.scan ...])
M3 Compactor     ──────▶ M2 SSTable.scan (inputs) + M2 write_sstable (output)
```

## How the LSM tree works here

```
 write ─▶ WAL (disk, append-only) ─▶ MemTable (skip list, RAM)
                                         │ full (16 KB)
                                         ▼
 L0:  [SST][SST][SST][SST]   ── 4 tables ──▶ merge ─▶ L1: [SST] ... ─▶ L2 ─▶ L3
 read: MemTable → L0 (newest first) → L1 → ...   each table: key-range check → Bloom filter → sparse-index block
```
* **Write path is O(log n) RAM + one sequential append** — never a random disk write. That's the "write-optimised" part.
* **Delete** = write a *tombstone*; real removal happens when compaction reaches the last level.
* **Update** = write a newer version; older versions are discarded by compaction (`entries reclaimed`).
* **Invariant** used for correct reads: every table on level *i* is newer than every table on level *i+1*; inside a level, later = newer.
* **Crash safety:** WAL replay rebuilds the MemTable; the MANIFEST (atomically replaced) says which SSTables are live; orphan files from a crash mid-compaction are deleted on start.

## Ride-hailing key design (Member 4)

| Key | Value | Why |
|---|---|---|
| `driver:latest:<id>` | newest GPS ping | constantly overwritten — LSM absorbs it cheaply |
| `driver:hist:<id>:<10¹³ − ts_ms>` | every ping | **reversed timestamp** ⇒ newest sorts first ⇒ "last N pings" = prefix scan with `limit=N` |
| `tripstate:<trip>` | current status | state machine: REQUESTED → DRIVER_ASSIGNED → DRIVER_ARRIVED → TRIP_STARTED → TRIP_COMPLETED (or CANCELLED) |
| `trip:<trip>:<0001…>` | event log | prefix scan returns a trip's whole timeline in order |

## Complexity cheat-sheet (for viva)

| Operation | Cost |
|---|---|
| `put` / `delete` | WAL append O(1) + skip-list insert O(log m), *m* = MemTable size (amortised flush/compaction cost is paid in bulk) |
| MemTable lookup | O(log m) expected |
| SSTable lookup | key-range O(1) + Bloom O(k) + binary search on sparse index O(log(n/N)) + read one block of N records |
| Bloom filter | O(k) per op, k≈7 for 1 % false positives; **never** a false negative |
| k-way merge of K tables, N records | O(N log K) using a min-heap of size K |
| Range scan | O(log) seek per source + O(result · log K) merge |
| Space | live data + at most ~`compaction_trigger` stale copies per level (space amplification) |
| Write amplification | each record is written to WAL once, to L0 once, and once per level it is merged into (shown live in the UI) |

## Trade-offs worth mentioning
* **Reads are costlier than in a B-tree** (several tables may be probed) → mitigated by Bloom filters, key ranges and compaction.
* **Size-tiered** compaction = lowest write amplification but more space/read amplification; *leveled* compaction is the opposite (possible extension).
* `nearby` scans all `driver:latest:*` keys (fine for this demo). A production system would add a geohash/grid prefix to keys, e.g. `grid:<cell>:<driver>`, so a nearby-search is a short prefix scan.
* Single-process, one lock around the engine, synchronous flush/compaction: simple and correct for a course project; production engines flush/compact in background threads.

## Project layout
```
run.py                          start server
frontend/index.html             dashboard (shared)
lsm/common.py                   Config + entry format (shared)
lsm/member1_memtable_wal.py     SkipList, MemTable, WAL
lsm/member2_sstable_bloom.py    BloomFilter, SSTable, write_sstable
lsm/member3_compaction.py       Manifest, LevelSet, kway_merge, Compactor
lsm/member4_engine.py           LSMEngine
lsm/member4_ride_service.py     RideService, Simulator, benchmark
lsm/member4_server.py           HTTP API
tests/test_lsm.py               unit + randomised model tests + crash recovery
```
