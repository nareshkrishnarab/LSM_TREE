"""
=====================================================================
MEMBER 4 (part 1/3) - THE LSM ENGINE  (puts everything together)
=====================================================================
Syllabus topics used : ADT design (Map / Dictionary ADT, Unit 2), Recursion-free
                       iterative algorithms, Complexity analysis (Unit 1)

Public API (a Map/Dictionary ADT):
    put(key, value)        delete(key)        get(key)        scan(start, end, limit)

WRITE PATH   put -> WAL.append -> MemTable.put -> (full?) -> flush to L0 SSTable -> maybe compact
READ PATH    MemTable -> L0 (newest first) -> L1 -> L2 ...   each table: key-range check,
             then Bloom filter, then sparse-index block read.  First hit wins.
RECOVERY     on start: load tables from the MANIFEST, replay leftover WALs into a MemTable.
"""
import os
import shutil
import threading
import time

from .common import Config, entry_size
from .member1_memtable_wal import MemTable, WAL
from .member2_sstable_bloom import write_sstable
from .member3_compaction import Compactor, LevelSet, Manifest, kway_merge

COUNTER_KEYS = (
    "puts", "deletes", "gets", "scans", "memtable_hits", "bloom_negatives", "range_skips",
    "bloom_false_positives", "table_reads", "flushes", "compactions", "entries_reclaimed",
    "bytes_ingested", "wal_bytes", "flush_bytes", "compaction_bytes",
)


class LSMEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._lock = threading.RLock()
        self._open()

    # ------------------------------------------------------------------ lifecycle
    def _open(self):
        os.makedirs(self.cfg.data_dir, exist_ok=True)
        self.c = {k: 0 for k in COUNTER_KEYS}
        self.manifest = Manifest(self.cfg.data_dir, self.cfg.max_levels)
        self.levelset = LevelSet(self.cfg.data_dir, self.manifest, self.cfg.max_levels)
        self.compactor = Compactor(self.cfg, self.manifest, self.levelset, self.c)
        self.seq = self.manifest.last_seq
        self.mem = MemTable()
        self.wal = None
        self._recover()

    def _wal_files(self):
        names = sorted(n for n in os.listdir(self.cfg.data_dir) if n.startswith("wal_") and n.endswith(".log"))
        return [os.path.join(self.cfg.data_dir, n) for n in names]

    def _recover(self):
        """Replay old WAL files (crash recovery), persist them as an SSTable, then start a fresh WAL."""
        old = self._wal_files()
        for path in old:
            for key, seq, value in WAL.replay(path):
                self.mem.put(key, seq, value)
                self.seq = max(self.seq, seq)
        if len(self.mem) > 0:
            self._flush_locked()
        for path in old:
            os.remove(path)
        self._start_new_wal()

    def _start_new_wal(self):
        wid = self.manifest.new_wal_id()
        self.wal = WAL(os.path.join(self.cfg.data_dir, "wal_%06d.log" % wid), self.cfg.wal_fsync)

    def close(self):
        with self._lock:
            if self.wal:
                self.wal.close()

    def reset(self):
        """Wipe ALL data and start empty (used by the demo UI)."""
        with self._lock:
            self.close()
            shutil.rmtree(self.cfg.data_dir, ignore_errors=True)
            self._open()

    # ------------------------------------------------------------------ writes
    def put(self, key, value):
        if value is None:
            raise ValueError("value must not be None (use delete())")
        with self._lock:
            self.c["puts"] += 1
            return self._write(key, value)

    def delete(self, key):
        with self._lock:
            self.c["deletes"] += 1
            return self._write(key, None)        # a delete is just a write of a TOMBSTONE

    def _write(self, key, value):
        self.seq += 1
        seq = self.seq
        self.c["wal_bytes"] += self.wal.append(key, seq, value)       # 1) durability
        self.mem.put(key, seq, value)                                 # 2) sorted in RAM
        self.c["bytes_ingested"] += entry_size(key, value)
        if self.mem.is_full(self.cfg.memtable_max_bytes):
            self._flush_locked()
        return seq

    def flush(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if len(self.mem) == 0:
            return
        tid = self.manifest.new_table_id()
        table = write_sstable(self.cfg.data_dir, tid, 0, self.mem.items(), len(self.mem), self.cfg)
        self.manifest.last_seq = self.seq
        self.levelset.add(0, table)
        self.manifest.save()                         # commit: table is now live
        self.c["flushes"] += 1
        self.c["flush_bytes"] += table.written_bytes
        if self.wal is not None:                     # its contents are safely in the SSTable now
            self.wal.delete()
            self.mem = MemTable()
            self._start_new_wal()
        else:                                        # called during recovery
            self.mem = MemTable()
        self.compactor.maybe_compact()

    def compact(self):
        """Force compaction of the fullest non-empty level (demo button)."""
        with self._lock:
            self._flush_locked()
            for lvl in range(self.cfg.max_levels):
                if len(self.levelset.levels[lvl]) >= 2:
                    return self.compactor.compact_level(lvl)
            return None

    # ------------------------------------------------------------------ reads
    def _lookup(self, key, steps):
        hit = self.mem.get(key)
        if hit is not None:
            self.c["memtable_hits"] += 1
            steps.append({"where": "MemTable", "result": "hit" if hit[1] is not None else "tombstone"})
            return hit
        steps.append({"where": "MemTable", "result": "miss"})
        for lvl, t in self.levelset.read_order():
            name = "L%d/#%d" % (lvl, t.id)
            if not t.in_range(key):
                self.c["range_skips"] += 1
                steps.append({"where": name, "result": "skipped (key outside min/max range)"})
                continue
            if not t.bloom.might_contain(key):
                self.c["bloom_negatives"] += 1
                steps.append({"where": name, "result": "skipped (Bloom filter: definitely absent)"})
                continue
            self.c["table_reads"] += 1
            found = t.get(key)
            if found is None:
                self.c["bloom_false_positives"] += 1
                steps.append({"where": name, "result": "Bloom false positive (read disk, not found)"})
                continue
            steps.append({"where": name, "result": "hit" if found[1] is not None else "tombstone"})
            return found
        return None

    def get(self, key):
        """Return the value, or None if the key does not exist / was deleted."""
        with self._lock:
            self.c["gets"] += 1
            found = self._lookup(key, [])
            return None if found is None else found[1]

    def get_traced(self, key):
        """Like get() but also returns the list of places that were probed (for the UI)."""
        with self._lock:
            self.c["gets"] += 1
            steps = []
            t0 = time.perf_counter()
            found = self._lookup(key, steps)
            us = round((time.perf_counter() - t0) * 1e6)
            return {"key": key, "found": found is not None and found[1] is not None,
                    "value": None if found is None else found[1], "trace": steps, "micros": us}

    def scan(self, start=None, end=None, limit=None):
        """Sorted range scan: start <= key < end. Returns [(key, value)], skipping deleted keys."""
        with self._lock:
            self.c["scans"] += 1
            iters = [self.mem.items(start)]
            for _lvl, t in self.levelset.read_order():
                if t.overlaps(start, end):
                    iters.append(t.scan(start, end))
            out = []
            for key, _seq, value in kway_merge(iters):
                if end is not None and key >= end:
                    break
                if value is None:
                    continue
                out.append((key, value))
                if limit is not None and len(out) >= limit:
                    break
            return out

    def scan_prefix(self, prefix, limit=None):
        return self.scan(prefix, prefix + "\U0010ffff", limit)

    # ------------------------------------------------------------------ introspection (for the UI)
    def structure(self):
        with self._lock:
            sample = [k for k, _s, _v in list(self.mem.items())[:6]]
            return {
                "memtable": {"entries": len(self.mem), "bytes": self.mem.bytes,
                             "max_bytes": self.cfg.memtable_max_bytes, "first_keys": sample},
                "wal_bytes": self.wal.size() if self.wal else 0,
                "levels": [[t.info() for t in tables] for tables in self.levelset.levels],
                "compaction_trigger": self.cfg.compaction_trigger,
            }

    def stats(self):
        with self._lock:
            c = dict(self.c)
            disk = c["wal_bytes"] + c["flush_bytes"] + c["compaction_bytes"]
            skipped = c["bloom_negatives"] + c["range_skips"]            # table reads avoided in RAM
            probes = skipped + c["table_reads"]
            return {
                "counters": c, "seq": self.seq, "tables": self.levelset.table_count(),
                "write_amplification": round(disk / c["bytes_ingested"], 2) if c["bytes_ingested"] else 0,
                "probes_saved_pct": round(100 * skipped / probes, 1) if probes else 0,
                "recent_compactions": list(self.compactor.events)[-12:],
            }
