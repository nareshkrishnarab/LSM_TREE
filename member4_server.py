"""
=====================================================================
MEMBER 4 (part 3/3) - HTTP API SERVER  (serves the frontend too)
=====================================================================
Pure standard library (http.server). JSON in, JSON out. The frontend in ../frontend
talks to these endpoints; you can also test them with curl.
"""
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .member4_ride_service import ApiError, RideService, Simulator, run_benchmark

FRONTEND = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend", "index.html")


class App:
    def __init__(self, engine):
        self.engine = engine
        self.service = RideService(engine)
        self.sim = Simulator(self.service)
        self.bench_lock = threading.Lock()


def _q(qs, name, default=None):
    return qs.get(name, [default])[0]


def make_handler(app):
    svc, eng, sim = app.service, app.engine, app.sim

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass                                          # keep the console quiet

        # ---------- plumbing
        def _send(self, status, payload, ctype="application/json"):
            body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n > 1_000_000:
                raise ApiError(413, "body too large")
            if n == 0:
                return {}
            try:
                d = json.loads(self.rfile.read(n))
            except json.JSONDecodeError:
                raise ApiError(400, "invalid JSON body")
            if not isinstance(d, dict):
                raise ApiError(400, "JSON body must be an object")
            return d

        def _dispatch(self, method):
            u = urlparse(self.path)
            qs = parse_qs(u.query)
            path = u.path
            try:
                if method == "GET" and path in ("/", "/index.html"):
                    with open(FRONTEND, "rb") as f:
                        return self._send(200, f.read(), "text/html; charset=utf-8")
                if not path.startswith("/api/"):
                    raise ApiError(404, "not found")
                body = self._body() if method == "POST" else {}
                return self._send(200, self._route(method, path, qs, body))
            except ApiError as e:
                self._send(e.status, {"error": e.message})
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:                        # never kill the server thread
                self._send(500, {"error": "%s: %s" % (type(e).__name__, e)})

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        # ---------- routes
        def _route(self, m, path, qs, b):
            # ---- monitoring
            if m == "GET" and path == "/api/stats":
                return dict(eng.stats(), sim=sim.status())
            if m == "GET" and path == "/api/structure":
                return eng.structure()

            # ---- drivers
            if m == "POST" and path == "/api/driver/location":
                return svc.update_location(b.get("driver_id"), b.get("lat"), b.get("lng"), b.get("speed", 0),
                                           b.get("status", "AVAILABLE"), b.get("heading", 0))
            if m == "POST" and path == "/api/driver/offline":
                return svc.driver_offline(b.get("driver_id"))
            if m == "GET" and path == "/api/drivers":
                return svc.list_drivers(int(_q(qs, "limit", 1000)))
            if m == "GET" and path == "/api/drivers/nearby":
                return svc.nearby(_q(qs, "lat"), _q(qs, "lng"), _q(qs, "radius_km", 3), _q(qs, "limit", 10))
            r = re.fullmatch(r"/api/driver/([^/]+)/history", path)
            if m == "GET" and r:
                return svc.driver_history(r.group(1), _q(qs, "limit", 30))
            r = re.fullmatch(r"/api/driver/([^/]+)", path)
            if m == "GET" and r:
                return svc.get_driver(r.group(1))

            # ---- trips
            if m == "POST" and path == "/api/trip/request":
                st = svc.request_trip(b.get("rider_id"), b.get("pickup") or {}, b.get("drop") or {})
                if b.get("auto_assign"):
                    st = svc.auto_assign(st["trip_id"])
                return st
            if m == "POST" and path == "/api/trip/event":
                st = svc.add_trip_event(b.get("trip_id"), b.get("type"), b.get("driver_id"), b.get("data"))
                if st["status"] in ("TRIP_COMPLETED", "CANCELLED"):
                    svc.finish_trip_free_driver(st["trip_id"])
                return st
            if m == "POST" and path == "/api/trip/assign":
                return svc.auto_assign(b.get("trip_id"))
            if m == "GET" and path == "/api/trips":
                return svc.list_trips()
            r = re.fullmatch(r"/api/trip/([^/]+)", path)
            if m == "GET" and r:
                return svc.get_trip(r.group(1))

            # ---- raw key-value explorer
            if m == "GET" and path == "/api/kv/get":
                return eng.get_traced(_q(qs, "key", ""))
            if m == "GET" and path == "/api/kv/scan":
                rows = eng.scan_prefix(_q(qs, "prefix", ""), min(int(_q(qs, "limit", 20)), 200))
                return [{"key": k, "value": v} for k, v in rows]

            # ---- admin / demo controls
            if m == "POST" and path == "/api/admin/flush":
                eng.flush()
                return {"ok": True}
            if m == "POST" and path == "/api/admin/compact":
                return {"event": eng.compact()}
            if m == "POST" and path == "/api/admin/reset":
                sim.stop()
                sim.drivers.clear()
                sim.trips.clear()
                sim.pings_sent = sim.trips_created = 0
                eng.reset()
                return {"ok": True}
            if m == "POST" and path == "/api/sim/start":
                sim.start(b.get("drivers", 40), b.get("updates_per_sec", 40))
                return sim.status()
            if m == "POST" and path == "/api/sim/stop":
                sim.stop()
                return sim.status()
            if m == "POST" and path == "/api/bench":
                if not app.bench_lock.acquire(blocking=False):
                    raise ApiError(409, "a benchmark is already running")
                try:
                    return run_benchmark(svc, b.get("updates", 5000))
                finally:
                    app.bench_lock.release()
            raise ApiError(404, "unknown endpoint")

    return Handler


def serve(engine, host="127.0.0.1", port=8000):
    app = App(engine)
    httpd = ThreadingHTTPServer((host, port), make_handler(app))
    httpd.daemon_threads = True
    print("LSM ride-hailing server running at http://%s:%d   (Ctrl+C to stop)" % (host, port))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        app.sim.stop()
        engine.close()
        httpd.server_close()
