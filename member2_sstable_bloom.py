"""
=====================================================================
MEMBER 2 - THE ON-DISK STORAGE:  Bloom Filter  +  SSTable
=====================================================================
Syllabus topics used : Bloom filter (Unit 3), Hashing / Hash tables (Unit 2),
                       Arrays, Binary search (Unit 1), External-memory searching (Unit 3)

SSTable = Sorted String Table: an IMMUTABLE file of records sorted by key.
Each SSTable has a small in-RAM "meta" part:
    * Bloom filter  -> answers "key is DEFINITELY not here" without touching the disk
    * Sparse index  -> every N-th key + its byte offset, so a lookup does
                       binary search in RAM and then reads ONE small block from disk

Files per table:   000007.sst        data, one JSON array per line: [key, seq, value]
                   000007.meta.json  min/max key, counts, sparse index, bloom filter

Complexities
    Bloom.add / might_contain : O(k)  (k hash functions, k ~ 7 for 1% FP)
    SSTable.get               : O(log(n/N)) in RAM + O(N) records read from one disk block
    SSTable.scan              : O(log(n/N)) seek + O(result)
"""
import base64
import hashlib
import json
import math
import os
from bisect import bisect_right


# --------------------------------------------------------------------------
# 1. BLOOM FILTER
# --------------------------------------------------------------------------
class BloomFilter:
    """
    Bit array of m bits + k hash functions.
        add(x)           : set k bits
        might_contain(x) : False  -> x is DEFINITELY absent (no false negatives, ever)
                           True   -> x is PROBABLY present (false-positive rate ~ fp_rate)
    m = -n ln(p) / (ln 2)^2        k = (m/n) ln 2
    The k hashes come from ONE 128-bit hash using double hashing: h1 + i*h2.
    """

    def __init__(self, n_items, fp_rate=0.01, m=None, k=None, bits=None):
        n = max(1, n_items)
        if m is None:
            m = max(64, int(math.ceil(-n * math.log(fp_rate) / (math.log(2) ** 2))))
        if k is None:
            k = max(1, int(round(m / n * math.log(2))))
        self.m, self.k = m, k
        self.bits = bits if bits is not None else bytearray((m + 7) // 8)

    def _positions(self, key):
        d = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
        h1 = int.from_bytes(d[:8], "little")
        h2 = int.from_bytes(d[8:], "little") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def add(self, key):
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    def might_contain(self, key):
        for p in self._positions(key):
            if not (self.bits[p >> 3] >> (p & 7)) & 1:
                return False
        return True

    def to_dict(self):
        return {"m": self.m, "k": self.k, "bits": base64.b64encode(bytes(self.bits)).decode("ascii")}

    @classmethod
    def from_dict(cls, d):
        return cls(0, m=d["m"], k=d["k"], bits=bytearray(base64.b64decode(d["bits"])))


# --------------------------------------------------------------------------
# 2. SSTABLE (reader)
# --------------------------------------------------------------------------
class SSTable:
    def __init__(self, data_path, meta, written_bytes=0):
        self.data_path = data_path
        self.meta_path = data_path[:-4] + ".meta.json"
        self.id = meta["id"]
        self.level = meta["level"]
        self.count = meta["count"]
        self.tombstones = meta["tombstones"]
        self.min_key = meta["min_key"]
        self.max_key = meta["max_key"]
        self.size_bytes = meta["size_bytes"]
        self.sparse_every = meta["sparse_every"]
        self._idx_keys = [k for k, _ in meta["index"]]
        self._idx_offs = [o for _, o in meta["index"]]
        self.bloom = BloomFilter.from_dict(meta["bloom"])
        self.written_bytes = written_bytes       # data + meta bytes produced when this table was created

    @classmethod
    def load(cls, meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        return cls(meta_path[: -len(".meta.json")] + ".sst", meta)

    # ---- cheap in-RAM checks (no disk I/O) ----
    def in_range(self, key):
        return self.min_key <= key <= self.max_key

    def overlaps(self, start, end):
        """Does [min_key, max_key] intersect [start, end) ?  None = unbounded."""
        if end is not None and self.min_key >= end:
            return False
        if start is not None and self.max_key < start:
            return False
        return True

    # ---- disk reads ----
    def get(self, key):
        """Point lookup -> (seq, value) or None. Reads at most ONE sparse-index block."""
        i = bisect_right(self._idx_keys, key) - 1     # binary search on the sparse index (in RAM)
        if i < 0:
            return None
        with open(self.data_path, "rb") as f:
            f.seek(self._idx_offs[i])
            for _ in range(self.sparse_every):
                line = f.readline()
                if not line:
                    break
                k, seq, value = json.loads(line)
                if k == key:
                    return (seq, value)
                if k > key:
                    break
        return None

    def scan(self, start=None, end=None):
        """Yield (key, seq, value) for start <= key < end, in sorted order."""
        i = 0 if start is None else max(bisect_right(self._idx_keys, start) - 1, 0)
        with open(self.data_path, "rb") as f:
            f.seek(self._idx_offs[i])
            for line in f:
                k, seq, value = json.loads(line)
                if start is not None and k < start:
                    continue
                if end is not None and k >= end:
                    return
                yield (k, seq, value)

    def delete_files(self):
        for p in (self.data_path, self.meta_path):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    def info(self):
        return {
            "id": self.id, "level": self.level, "count": self.count,
            "tombstones": self.tombstones, "min_key": self.min_key, "max_key": self.max_key,
            "size_bytes": self.size_bytes, "bloom_bits": self.bloom.m, "bloom_hashes": self.bloom.k,
        }


# --------------------------------------------------------------------------
# 3. SSTABLE (writer)
# --------------------------------------------------------------------------
def write_sstable(dir_, table_id, level, entries, expected_count, cfg):
    """
    Stream a SORTED, key-unique iterator of (key, seq, value) into a new SSTable.
    Used by BOTH the MemTable flush (Member 4) and compaction (Member 3).
    `expected_count` only sizes the Bloom filter (an upper bound is fine).
    Returns the SSTable, or None if `entries` was empty (nothing is created).
    """
    base = os.path.join(dir_, "%06d" % table_id)
    data_path, meta_path = base + ".sst", base + ".meta.json"
    bloom = BloomFilter(max(expected_count, 1), cfg.bloom_fp_rate)
    index = []
    count = tombstones = offset = 0
    min_key = max_key = None

    with open(data_path + ".tmp", "wb") as f:
        for key, seq, value in entries:
            if count % cfg.sparse_index_every == 0:
                index.append([key, offset])
            line = (json.dumps([key, seq, value], separators=(",", ":")) + "\n").encode("utf-8")
            f.write(line)
            offset += len(line)
            bloom.add(key)
            count += 1
            if value is None:
                tombstones += 1
            if min_key is None:
                min_key = key
            max_key = key

    if count == 0:
        os.remove(data_path + ".tmp")
        return None

    os.replace(data_path + ".tmp", data_path)
    meta = {
        "id": table_id, "level": level, "count": count, "tombstones": tombstones,
        "min_key": min_key, "max_key": max_key, "size_bytes": offset,
        "sparse_every": cfg.sparse_index_every, "index": index, "bloom": bloom.to_dict(),
    }
    meta_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")
    with open(meta_path + ".tmp", "wb") as f:
        f.write(meta_bytes)
    os.replace(meta_path + ".tmp", meta_path)
    return SSTable(data_path, meta, written_bytes=offset + len(meta_bytes))
