"""Start the whole project:   python run.py   then open http://127.0.0.1:8000"""
import argparse

from lsm.common import Config
from lsm.member4_engine import LSMEngine
from lsm.member4_server import serve

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="LSM-tree backed ride-hailing location store")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--data-dir", default="lsm_data")
    ap.add_argument("--memtable-kb", type=int, default=16, help="MemTable size before flush (small = visible flushes)")
    args = ap.parse_args()
    cfg = Config(data_dir=args.data_dir, memtable_max_bytes=args.memtable_kb * 1024)
    serve(LSMEngine(cfg), args.host, args.port)
