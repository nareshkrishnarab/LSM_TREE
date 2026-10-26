"""
SHARED by all members (nobody "owns" this file - agree on it together).

Contains the engine configuration and the on-disk entry convention:

    entry = (key: str, seq: int, value: JSON-able object | None)

    * seq   - global, monotonically increasing write number. For the same key,
              the entry with the HIGHEST seq is the truth.
    * value - None means TOMBSTONE (the key was deleted).
"""
import json
from dataclasses import dataclass


@dataclass
class Config:
    data_dir: str = "lsm_data"
    memtable_max_bytes: int = 16 * 1024   # small on purpose so flushes are visible in the demo
    sparse_index_every: int = 16          # one index entry per 16 records in an SSTable
    bloom_fp_rate: float = 0.01           # target Bloom filter false-positive rate (1%)
    compaction_trigger: int = 4           # compact a level when it holds this many SSTables
    max_levels: int = 4                   # L0 .. L3
    wal_fsync: bool = False               # True = fsync every write (durable but slower)


def entry_size(key: str, value) -> int:
    """Approximate in-memory / on-disk size of one record, in bytes."""
    vlen = 0 if value is None else len(json.dumps(value, separators=(",", ":")))
    return len(key) + vlen + 16
