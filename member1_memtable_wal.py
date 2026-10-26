"""
=====================================================================
MEMBER 1 - THE WRITE PATH FRONT-END:  Skip List  ->  MemTable  ->  WAL
=====================================================================
Syllabus topics used : Skip Lists (Unit 2), Linked structures, Complexity analysis

Every write in an LSM tree does exactly two cheap things:
    1. APPEND the record to the Write-Ahead Log (sequential disk write, crash safety)
    2. INSERT the record into the MemTable (sorted, in RAM)

No random disk I/O ever happens on the write path - this is why LSM trees are
"write-optimised", which is exactly what we need for a flood of driver GPS pings.

Complexities
    SkipList.put / get : O(log n) expected      SkipList.items : O(n)
    WAL.append         : O(1) (sequential append)
"""
import json
import os
import random

from .common import entry_size


# --------------------------------------------------------------------------
# 1. SKIP LIST  (sorted in-memory map)
# --------------------------------------------------------------------------
class _Node:
    __slots__ = ("key", "seq", "value", "forward")

    def __init__(self, key, seq, value, level):
        self.key = key
        self.seq = seq
        self.value = value
        self.forward = [None] * level          # forward[i] = next node on express lane i


class SkipList:
    """
    Probabilistic sorted linked list with express lanes.
    Each node is promoted to the next lane with probability P=0.5, so lane i has
    ~n/2^i nodes and a search skips through the lanes top-down in O(log n).
    """
    MAX_LEVEL = 16
    P = 0.5

    def __init__(self, seed=None):
        self._rng = random.Random(seed)
        self._head = _Node(None, 0, None, self.MAX_LEVEL)
        self._level = 1                         # number of lanes currently in use
        self._count = 0

    def __len__(self):
        return self._count

    def _random_level(self):
        lvl = 1
        while lvl < self.MAX_LEVEL and self._rng.random() < self.P:
            lvl += 1
        return lvl

    def _descend(self, key):
        """Walk top lane -> bottom lane. Returns (update[], first node with node.key >= key)."""
        update = [self._head] * self.MAX_LEVEL
        x = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = x.forward[i]
            while nxt is not None and nxt.key < key:
                x = nxt
                nxt = x.forward[i]
            update[i] = x
        return update, x.forward[0]

    def put(self, key, seq, value):
        """Insert or overwrite. Returns the old (seq, value) if the key existed, else None."""
        update, cand = self._descend(key)
        if cand is not None and cand.key == key:
            old = (cand.seq, cand.value)
            cand.seq, cand.value = seq, value
            return old
        lvl = self._random_level()
        if lvl > self._level:
            self._level = lvl                   # update[] for new lanes already points at head
        node = _Node(key, seq, value, lvl)
        for i in range(lvl):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node
        self._count += 1
        return None

    def get(self, key):
        """Returns (seq, value) or None."""
        _, cand = self._descend(key)
        if cand is not None and cand.key == key:
            return (cand.seq, cand.value)
        return None

    def items(self, start=None):
        """Yield (key, seq, value) in sorted key order, beginning at the first key >= start."""
        if start is None:
            x = self._head.forward[0]
        else:
            _, x = self._descend(start)
        while x is not None:
            yield (x.key, x.seq, x.value)
            x = x.forward[0]


# --------------------------------------------------------------------------
# 2. MEMTABLE  (skip list + size accounting)
# --------------------------------------------------------------------------
class MemTable:
    """The in-RAM write buffer. When `bytes` reaches the limit, the engine flushes it to an SSTable."""

    def __init__(self, seed=None):
        self._sl = SkipList(seed)
        self.bytes = 0
        self.max_seq = 0

    def __len__(self):
        return len(self._sl)

    def put(self, key, seq, value):
        old = self._sl.put(key, seq, value)
        new_size = entry_size(key, value)
        if old is None:
            self.bytes += new_size
        else:
            self.bytes += new_size - entry_size(key, old[1])
        if seq > self.max_seq:
            self.max_seq = seq

    def get(self, key):
        """(seq, value) or None. value None inside a hit means 'deleted' (tombstone)."""
        return self._sl.get(key)

    def items(self, start=None):
        return self._sl.items(start)

    def is_full(self, max_bytes):
        return self.bytes >= max_bytes


# --------------------------------------------------------------------------
# 3. WRITE-AHEAD LOG
# --------------------------------------------------------------------------
class WAL:
    """
    Append-only log, one JSON array per line:  [key, seq, value]
    If the process crashes, the MemTable (RAM) is lost, but replaying the WAL rebuilds it.
    """

    def __init__(self, path, fsync=False):
        self.path = path
        self.fsync = fsync
        self._f = open(path, "a", encoding="utf-8")

    def append(self, key, seq, value):
        """Append one record. Returns the number of bytes written (for write-amplification stats)."""
        line = json.dumps([key, seq, value], separators=(",", ":")) + "\n"
        self._f.write(line)
        self._f.flush()
        if self.fsync:
            os.fsync(self._f.fileno())
        return len(line.encode("utf-8"))

    def close(self):
        if not self._f.closed:
            self._f.close()

    def delete(self):
        self.close()
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass

    def size(self):
        try:
            return os.path.getsize(self.path)
        except FileNotFoundError:
            return 0

    @staticmethod
    def replay(path):
        """Yield (key, seq, value) from a WAL file. A torn last line (crash mid-write) is ignored."""
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    key, seq, value = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    break
                yield (key, seq, value)
