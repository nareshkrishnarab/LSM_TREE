"""
=====================================================================
MEMBER 3 - KEEPING THE TREE HEALTHY:  Manifest, Levels, K-way Merge, Compaction
=====================================================================
Syllabus topics used : Heaps / Priority queues (Unit 2), Arrays & lists (Unit 1),
                       Merge idea behind External-memory sorting (Unit 3),
                       Amortised / complexity analysis (Unit 1)

Because memtable flushes keep creating new SSTables, reads would have to check
more and more files. COMPACTION fixes this by merging SSTables together:

    * duplicates are removed (only the newest version of a key survives) -> reclaims space
    * tombstones are dropped once nothing older can resurface            -> deletes become real
    * fewer files to search                                              -> faster reads

Strategy: SIZE-TIERED.  When a level holds `compaction_trigger` (4) tables,
merge ALL of them into ONE table on the next level.

    L0:  [t1][t2][t3][t4]  --merge-->  L1: [T]      (and so on up to the last level)

Invariant that makes reads correct: every table on level i is NEWER than every
table on level i+1, and within a level later tables are newer.

Complexities
    kway_merge of K sorted inputs holding N records in total : O(N log K)  (min-heap of size K)
"""
import heapq
import json
import os
import time
from collections import deque

from .member2_sstable_bloom import SSTable, write_sstable


# --------------------------------------------------------------------------
# 1. MANIFEST - the single source of truth on disk about which tables are live
# --------------------------------------------------------------------------
class Manifest:
    FILE = "MANIFEST.json"

    def __init__(self, dir_, max_levels):
        self.path = os.path.join(dir_, self.FILE)
        self.max_levels = max_levels
        self.next_table_id = 1
        self.next_wal_id = 1
        self.last_seq = 0
        self.levels = [[] for _ in range(max_levels)]     # level -> [table ids], oldest first
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.next_table_id = d["next_table_id"]
            self.next_wal_id = d["next_wal_id"]
            self.last_seq = d["last_seq"]
            for i, ids in enumerate(d["levels"][:max_levels]):
                self.levels[i] = list(ids)

    def new_table_id(self):
        tid = self.next_table_id
        self.next_table_id += 1
        return tid

    def new_wal_id(self):
        wid = self.next_wal_id
        self.next_wal_id += 1
        return wid

    def save(self):
        """Atomic: write a temp file, then rename over the old manifest."""
        d = {"next_table_id": self.next_table_id, "next_wal_id": self.next_wal_id,
             "last_seq": self.last_seq, "levels": self.levels}
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


# --------------------------------------------------------------------------
# 2. LEVEL SET - the loaded SSTable objects, organised by level
# --------------------------------------------------------------------------
class LevelSet:
    def __init__(self, dir_, manifest, max_levels):
        self.dir = dir_
        self.manifest = manifest
        self.levels = [[] for _ in range(max_levels)]     # level -> [SSTable], oldest first
        live = set()
        for lvl, ids in enumerate(manifest.levels):
            for tid in ids:
                meta_path = os.path.join(dir_, "%06d.meta.json" % tid)
                self.levels[lvl].append(SSTable.load(meta_path))
                live.add(tid)
        self._remove_orphans(live)

    def _remove_orphans(self, live):
        """Delete files of tables that are not in the manifest (left by a crash mid-flush/compaction)."""
        for name in os.listdir(self.dir):
            if name.endswith(".tmp"):
                os.remove(os.path.join(self.dir, name))
            elif name.endswith(".sst") or name.endswith(".meta.json"):
                if int(name.split(".")[0]) not in live:
                    os.remove(os.path.join(self.dir, name))

    def add(self, level, table):
        self.levels[level].append(table)
        self.manifest.levels[level].append(table.id)

    def read_order(self):
        """Yield (level, table) NEWEST first - the order in which a point read must probe them."""
        for lvl, tables in enumerate(self.levels):
            for t in reversed(tables):
                yield lvl, t

    def table_count(self):
        return sum(len(t) for t in self.levels)

    def replace(self, level, old_tables, dest_level, new_table):
        """Swap `old_tables` (on `level`) for `new_table` (on `dest_level`) and persist atomically."""
        gone = {t.id for t in old_tables}
        self.levels[level] = [t for t in self.levels[level] if t.id not in gone]
        self.manifest.levels[level] = [i for i in self.manifest.levels[level] if i not in gone]
        if new_table is not None:
            self.add(dest_level, new_table)
        self.manifest.save()                      # <-- the commit point
        for t in old_tables:
            t.delete_files()                      # safe: manifest no longer references them


# --------------------------------------------------------------------------
# 3. K-WAY MERGE with a MIN-HEAP
# --------------------------------------------------------------------------
def kway_merge(iterators, drop_tombstones=False):
    """
    Merge K sorted iterators of (key, seq, value) into ONE sorted iterator with unique keys.
    For equal keys the entry with the HIGHEST seq wins (newest version).

    The heap holds one "head" record per input iterator and is ordered by
    (key, -seq, source): the smallest key comes out first, and among equal keys the newest first.
    """
    heap = []
    for src, it in enumerate(iterators):
        first = next(it, None)
        if first is not None:
            heap.append((first[0], -first[1], src, first, it))
    heapq.heapify(heap)

    last_key = None
    while heap:
        key, _negseq, src, entry, it = heapq.heappop(heap)
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt[0], -nxt[1], src, nxt, it))
        if key == last_key:
            continue                              # older version of a key we already emitted
        last_key = key
        if drop_tombstones and entry[2] is None:
            continue                              # deleted AND nothing older exists -> forget it
        yield entry


# --------------------------------------------------------------------------
# 4. COMPACTOR
# --------------------------------------------------------------------------
class Compactor:
    def __init__(self, cfg, manifest, levelset, counters):
        self.cfg = cfg
        self.manifest = manifest
        self.ls = levelset
        self.counters = counters                  # shared dict owned by the engine (for stats)
        self.events = deque(maxlen=50)            # recent compactions, shown in the UI

    def maybe_compact(self):
        """Run compactions until no level is over its limit. Returns the list of events produced."""
        done = []
        again = True
        while again:
            again = False
            for lvl in range(self.cfg.max_levels):
                if len(self.ls.levels[lvl]) >= self.cfg.compaction_trigger:
                    done.append(self.compact_level(lvl))
                    again = True
                    break
        return done

    def compact_level(self, level):
        inputs = list(self.ls.levels[level])
        last = self.cfg.max_levels - 1
        dest = level + 1 if level < last else level      # last level compacts into itself

        # A tombstone may only be discarded if no OLDER copy of the key can exist anywhere else.
        if dest == level:
            drop = True                                   # we are merging every table of the last level
        else:
            drop = all(not self.ls.levels[l] for l in range(dest, self.cfg.max_levels))

        t0 = time.perf_counter()
        entries_in = sum(t.count for t in inputs)
        bytes_in = sum(t.size_bytes for t in inputs)
        iters = [t.scan() for t in inputs]
        merged = kway_merge(iters, drop_tombstones=drop)
        new_table = write_sstable(self.ls.dir, self.manifest.new_table_id(), dest,
                                  merged, entries_in, self.cfg)
        self.ls.replace(level, inputs, dest, new_table)

        entries_out = new_table.count if new_table else 0
        bytes_out = new_table.size_bytes if new_table else 0
        self.counters["compactions"] += 1
        self.counters["compaction_bytes"] += new_table.written_bytes if new_table else 0
        self.counters["entries_reclaimed"] += entries_in - entries_out
        event = {
            "time": time.time(), "from_level": level, "to_level": dest,
            "tables_merged": len(inputs), "entries_in": entries_in, "entries_out": entries_out,
            "reclaimed": entries_in - entries_out, "bytes_in": bytes_in, "bytes_out": bytes_out,
            "tombstones_dropped": drop, "ms": round((time.perf_counter() - t0) * 1000, 1),
            "new_table": new_table.id if new_table else None,
        }
        self.events.append(event)
        return event
