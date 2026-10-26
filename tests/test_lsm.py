"""Run:  python -m unittest discover -s tests -v     (from the project root)"""
import os
import random
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsm.common import Config
from lsm.member1_memtable_wal import MemTable, SkipList, WAL
from lsm.member2_sstable_bloom import BloomFilter, SSTable, write_sstable
from lsm.member3_compaction import kway_merge
from lsm.member4_engine import LSMEngine
from lsm.member4_ride_service import ApiError, RideService


class TmpDirCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="lsmtest_")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TestMember1(TmpDirCase):
    def test_skiplist_sorted_and_overwrite(self):
        sl = SkipList(seed=1)
        keys = ["k%04d" % i for i in range(500)]
        shuffled = keys[:]
        random.Random(2).shuffle(shuffled)
        for i, k in enumerate(shuffled):
            self.assertIsNone(sl.put(k, i, {"v": i}))
        self.assertEqual([k for k, _, _ in sl.items()], keys)
        self.assertEqual(len(sl), 500)
        self.assertEqual(sl.put("k0007", 999, "new")[0], shuffled.index("k0007"))   # returns old
        self.assertEqual(sl.get("k0007"), (999, "new"))
        self.assertEqual(len(sl), 500)
        self.assertIsNone(sl.get("zzz"))
        self.assertEqual(next(sl.items("k0100"))[0], "k0100")
        self.assertEqual(next(sl.items("k0100x"))[0], "k0101")

    def test_memtable_bytes_accounting(self):
        m = MemTable()
        m.put("a", 1, {"x": 1})
        b1 = m.bytes
        m.put("a", 2, {"x": 1})             # same size overwrite
        self.assertEqual(m.bytes, b1)
        m.put("a", 3, None)                 # tombstone is smaller
        self.assertLess(m.bytes, b1)

    def test_wal_replay_and_torn_line(self):
        p = os.path.join(self.dir, "w.log")
        w = WAL(p)
        w.append("a", 1, {"v": 1})
        w.append("b", 2, None)
        w.close()
        with open(p, "a") as f:
            f.write('["c",3,{"v"')           # simulated crash mid-write
        self.assertEqual(list(WAL.replay(p)), [("a", 1, {"v": 1}), ("b", 2, None)])


class TestMember2(TmpDirCase):
    def test_bloom_no_false_negatives_and_low_fp(self):
        bf = BloomFilter(2000, 0.01)
        for i in range(2000):
            bf.add("in%d" % i)
        self.assertTrue(all(bf.might_contain("in%d" % i) for i in range(2000)))
        fp = sum(bf.might_contain("out%d" % i) for i in range(20000)) / 20000
        self.assertLess(fp, 0.03)
        again = BloomFilter.from_dict(bf.to_dict())
        self.assertTrue(all(again.might_contain("in%d" % i) for i in range(2000)))

    def test_sstable_get_scan_reload(self):
        cfg = Config(data_dir=self.dir, sparse_index_every=8)
        entries = [("k%04d" % i, i, None if i % 10 == 0 else {"v": i}) for i in range(1, 300)]
        t = write_sstable(self.dir, 1, 0, iter(entries), len(entries), cfg)
        t = SSTable.load(t.meta_path)       # prove it round-trips through disk
        for k, s, v in entries:
            self.assertEqual(t.get(k), (s, v))
        self.assertIsNone(t.get("k9999"))
        self.assertIsNone(t.get("a"))
        got = [k for k, _, _ in t.scan("k0050", "k0060")]
        self.assertEqual(got, ["k%04d" % i for i in range(50, 60)])
        self.assertEqual(len(list(t.scan())), 299)
        self.assertEqual(t.tombstones, 29)
        self.assertIsNone(write_sstable(self.dir, 2, 0, iter([]), 0, cfg))


class TestMember3(unittest.TestCase):
    def test_kway_merge_newest_wins_and_tombstones(self):
        a = iter([("a", 1, "a1"), ("c", 5, "c5"), ("e", 1, "e1")])
        b = iter([("a", 4, "a4"), ("b", 2, "b2"), ("e", 7, None)])
        c = iter([("c", 3, "c3"), ("d", 6, "d6")])
        out = list(kway_merge([a, b, c]))
        self.assertEqual(out, [("a", 4, "a4"), ("b", 2, "b2"), ("c", 5, "c5"), ("d", 6, "d6"), ("e", 7, None)])
        a = iter([("e", 1, "e1")])
        b = iter([("e", 7, None), ("f", 8, "f")])
        self.assertEqual(list(kway_merge([a, b], drop_tombstones=True)), [("f", 8, "f")])


class TestEngine(TmpDirCase):
    def cfg(self, **kw):
        base = dict(data_dir=self.dir, memtable_max_bytes=2000, sparse_index_every=4, compaction_trigger=3)
        base.update(kw)
        return Config(**base)

    def test_randomised_against_dict_model(self):
        eng = LSMEngine(self.cfg())
        model, rng = {}, random.Random(42)
        for i in range(6000):
            k = "key%03d" % rng.randint(0, 299)
            if rng.random() < 0.2:
                eng.delete(k)
                model.pop(k, None)
            else:
                v = {"n": i}
                eng.put(k, v)
                model[k] = v
            if i % 500 == 0:
                for kk in rng.sample(sorted(model) or ["x"], min(20, len(model))):
                    self.assertEqual(eng.get(kk), model[kk])
        st = eng.stats()
        self.assertGreater(st["counters"]["flushes"], 5)
        self.assertGreater(st["counters"]["compactions"], 0)
        for i in range(300):
            k = "key%03d" % i
            self.assertEqual(eng.get(k), model.get(k), k)
        self.assertEqual(eng.scan("key", "key￿"), sorted(model.items()))
        self.assertEqual(eng.scan("key100", "key110"), [(k, v) for k, v in sorted(model.items()) if "key100" <= k < "key110"])
        self.assertEqual(len(eng.scan("key", "key￿", limit=7)), 7)
        eng.close()

    def test_recovery_after_clean_restart_and_after_crash(self):
        eng = LSMEngine(self.cfg(memtable_max_bytes=10 ** 6))
        for i in range(50):
            eng.put("k%02d" % i, {"v": i})
        eng.delete("k03")
        eng.close()                                  # NOT flushed: everything only in the WAL
        eng2 = LSMEngine(self.cfg(memtable_max_bytes=10 ** 6))
        self.assertEqual(eng2.get("k10"), {"v": 10})
        self.assertIsNone(eng2.get("k03"))
        eng2.put("k99", {"v": 99})
        # simulate a crash: do not close(), just drop it, then reopen
        eng3 = LSMEngine(self.cfg(memtable_max_bytes=10 ** 6))
        self.assertEqual(eng3.get("k99"), {"v": 99})
        self.assertEqual(eng3.get("k49"), {"v": 49})
        self.assertEqual(eng3.seq, 52)
        eng3.close()

    def test_tombstones_dropped_in_last_level(self):
        eng = LSMEngine(self.cfg(max_levels=2, compaction_trigger=2, memtable_max_bytes=10 ** 6))
        for i in range(20):
            eng.put("k%02d" % i, {"v": i})
        eng.flush()
        for i in range(20):
            eng.delete("k%02d" % i)
        eng.flush()                                  # 2 tables at L0 -> compact into L1 (the last level)
        self.assertEqual(eng.stats()["tables"], 0)   # everything cancelled out, nothing left on disk
        self.assertEqual(eng.scan("k", "l"), [])
        eng.close()

    def test_bloom_and_trace(self):
        eng = LSMEngine(self.cfg(memtable_max_bytes=10 ** 6, compaction_trigger=50))
        for t in range(3):
            for i in range(30):
                eng.put("t%d-%02d" % (t, i), {"v": i})
            eng.flush()
        r = eng.get_traced("t0-05")
        self.assertTrue(r["found"])
        self.assertTrue(any("range" in s["result"] or "Bloom" in s["result"] for s in r["trace"]))
        self.assertFalse(eng.get_traced("nope")["found"])
        self.assertGreater(eng.stats()["counters"]["range_skips"] + eng.stats()["counters"]["bloom_negatives"], 0)
        eng.close()


class TestRideService(TmpDirCase):
    def test_flow(self):
        eng = LSMEngine(Config(data_dir=self.dir, memtable_max_bytes=3000))
        svc = RideService(eng)
        for i in range(30):                          # many pings -> forces flushes, overwrites of "latest"
            svc.update_location("D1", 19.05 + i * 1e-4, 72.85, 20)
        svc.update_location("D2", 19.0505, 72.8505, 10)
        self.assertAlmostEqual(svc.get_driver("D1")["lat"], 19.05 + 29e-4)
        hist = svc.driver_history("D1", 5)
        self.assertEqual(len(hist), 5)
        self.assertEqual([h["lat"] for h in hist], sorted([h["lat"] for h in hist], reverse=True))   # newest first
        near = svc.nearby(19.0501, 72.85, 1)
        self.assertEqual({d["driver_id"] for d in near}, {"D1", "D2"})
        trip = svc.request_trip("R1", {"lat": 19.0502, "lng": 72.8502}, {"lat": 19.1, "lng": 72.9})
        tid = trip["trip_id"]
        st = svc.auto_assign(tid)
        self.assertEqual(st["status"], "DRIVER_ASSIGNED")
        self.assertEqual(svc.get_driver(st["driver_id"])["status"], "BUSY")
        with self.assertRaises(ApiError):
            svc.add_trip_event(tid, "TRIP_COMPLETED")        # illegal jump
        for e in ("DRIVER_ARRIVED", "TRIP_STARTED", "TRIP_COMPLETED"):
            svc.add_trip_event(tid, e)
        full = svc.get_trip(tid)
        self.assertEqual([e["type"] for e in full["events"]],
                         ["REQUESTED", "DRIVER_ASSIGNED", "DRIVER_ARRIVED", "TRIP_STARTED", "TRIP_COMPLETED"])
        svc.driver_offline("D2")
        with self.assertRaises(ApiError):
            svc.get_driver("D2")
        with self.assertRaises(ApiError):
            svc.update_location("bad:id", 1, 1)               # key-injection guard
        with self.assertRaises(ApiError):
            svc.update_location("D3", 123, 1)
        eng.close()


if __name__ == "__main__":
    unittest.main()
