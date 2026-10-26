"""
=====================================================================
MEMBER 4 (part 2/3) - RIDE-HAILING DOMAIN LAYER  (+ traffic simulator)
=====================================================================
Turns ride-hailing concepts into keys in the LSM tree. The KEY DESIGN is the whole trick:
keys are sorted, so related records sit next to each other and a PREFIX SCAN fetches them.

    driver:latest:<driver_id>                  -> newest GPS ping       (overwritten constantly)
    driver:hist:<driver_id>:<REVERSED ts>      -> every GPS ping        (append-only history)
    tripstate:<trip_id>                        -> current trip status
    trip:<trip_id>:<event no, 4 digits>        -> trip event log        (REQUESTED, ASSIGNED, ...)

Trick: history keys use (10^13 - timestamp_ms), so the NEWEST ping sorts FIRST and
"last 20 locations of driver D" is just a prefix scan with limit=20.

Why an LSM tree fits: location updates are a firehose of tiny overwrites. Each one is a WAL
append + a skip-list insert; repeated overwrites of `driver:latest:*` are collapsed
later by compaction, and Bloom filters keep reads of recent keys cheap.
"""
import math
import random
import re
import threading
import time
import uuid

ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")
DRIVER_STATUSES = ("AVAILABLE", "BUSY", "OFFLINE")
TRIP_FLOW = {
    None: {"REQUESTED"},
    "REQUESTED": {"DRIVER_ASSIGNED", "CANCELLED"},
    "DRIVER_ASSIGNED": {"DRIVER_ARRIVED", "CANCELLED"},
    "DRIVER_ARRIVED": {"TRIP_STARTED", "CANCELLED"},
    "TRIP_STARTED": {"TRIP_COMPLETED"},
    "TRIP_COMPLETED": set(),
    "CANCELLED": set(),
}
TS_REVERSE = 10 ** 13


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _check_id(name, value):
    if not isinstance(value, str) or not ID_RE.match(value):
        raise ApiError(400, "%s must be 1-40 chars of letters, digits, '_' or '-'" % name)
    return value


def _num(name, value, lo, hi):
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ApiError(400, "%s must be a number" % name)
    if not (lo <= v <= hi) or math.isnan(v):
        raise ApiError(400, "%s must be between %s and %s" % (name, lo, hi))
    return v


def haversine_km(lat1, lng1, lat2, lng2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class RideService:
    def __init__(self, engine):
        self.db = engine
        self._lock = threading.RLock()
        self._ping_counter = 0

    # ---------------------------------------------------------------- drivers
    def update_location(self, driver_id, lat, lng, speed=0, status="AVAILABLE", heading=0,
                        keep_history=True, ts_ms=None):
        _check_id("driver_id", driver_id)
        lat = _num("lat", lat, -90, 90)
        lng = _num("lng", lng, -180, 180)
        speed = _num("speed", speed, 0, 400)
        heading = _num("heading", heading, 0, 360)
        if status not in DRIVER_STATUSES:
            raise ApiError(400, "status must be one of %s" % (DRIVER_STATUSES,))
        ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
        point = {"driver_id": driver_id, "lat": round(lat, 6), "lng": round(lng, 6),
                 "speed": speed, "heading": heading, "status": status, "ts": ts_ms}
        self.db.put("driver:latest:%s" % driver_id, point)
        if keep_history:
            with self._lock:
                self._ping_counter = (self._ping_counter + 1) % 1000
                n = self._ping_counter
            # suffix also counts DOWN so that pings in the same millisecond keep "newest first" order
            self.db.put("driver:hist:%s:%013d-%03d" % (driver_id, TS_REVERSE - ts_ms, 999 - n), point)
        return point

    def get_driver(self, driver_id):
        _check_id("driver_id", driver_id)
        p = self.db.get("driver:latest:%s" % driver_id)
        if p is None:
            raise ApiError(404, "driver not found (never seen, or went offline)")
        return p

    def driver_history(self, driver_id, limit=30):
        _check_id("driver_id", driver_id)
        rows = self.db.scan_prefix("driver:hist:%s:" % driver_id, limit=max(1, min(int(limit), 500)))
        return [v for _k, v in rows]                       # newest first

    def driver_offline(self, driver_id):
        _check_id("driver_id", driver_id)
        self.db.delete("driver:latest:%s" % driver_id)     # writes a TOMBSTONE
        return {"driver_id": driver_id, "status": "OFFLINE"}

    def list_drivers(self, limit=500):
        return [v for _k, v in self.db.scan_prefix("driver:latest:", limit=limit)]

    def nearby(self, lat, lng, radius_km=3.0, limit=10):
        lat = _num("lat", lat, -90, 90)
        lng = _num("lng", lng, -180, 180)
        radius_km = _num("radius_km", radius_km, 0.01, 500)
        found = []
        for d in self.list_drivers():
            if d["status"] != "AVAILABLE":
                continue
            dist = haversine_km(lat, lng, d["lat"], d["lng"])
            if dist <= radius_km:
                found.append(dict(d, distance_km=round(dist, 3)))
        found.sort(key=lambda d: d["distance_km"])
        return found[:max(1, int(limit))]

    # ---------------------------------------------------------------- trips
    def request_trip(self, rider_id, pickup, drop):
        _check_id("rider_id", rider_id)
        p = {"lat": _num("pickup.lat", pickup.get("lat"), -90, 90), "lng": _num("pickup.lng", pickup.get("lng"), -180, 180)}
        d = {"lat": _num("drop.lat", drop.get("lat"), -90, 90), "lng": _num("drop.lng", drop.get("lng"), -180, 180)}
        trip_id = "T" + uuid.uuid4().hex[:8]
        return self._event(trip_id, "REQUESTED", None, {"rider_id": rider_id, "pickup": p, "drop": d})

    def add_trip_event(self, trip_id, etype, driver_id=None, data=None):
        _check_id("trip_id", trip_id)
        if driver_id is not None:
            _check_id("driver_id", driver_id)
        return self._event(trip_id, etype, driver_id, data or {})

    def _event(self, trip_id, etype, driver_id, data):
        with self._lock:
            state = self.db.get("tripstate:%s" % trip_id)
            current = state["status"] if state else None
            if state is None and etype != "REQUESTED":
                raise ApiError(404, "unknown trip")
            if etype not in TRIP_FLOW.get(current, set()):
                raise ApiError(409, "illegal transition %s -> %s" % (current, etype))
            now = int(time.time() * 1000)
            n = (state["events"] if state else 0) + 1
            new_state = dict(state or {}, trip_id=trip_id, status=etype, events=n, updated=now)
            if state is None:
                new_state.update(rider_id=data["rider_id"], pickup=data["pickup"], drop=data["drop"],
                                 created=now, driver_id=None)
            if driver_id:
                new_state["driver_id"] = driver_id
            event = {"trip_id": trip_id, "n": n, "type": etype, "driver_id": new_state["driver_id"],
                     "ts": now, "data": data}
            self.db.put("trip:%s:%04d" % (trip_id, n), event)
            self.db.put("tripstate:%s" % trip_id, new_state)
            return new_state

    def auto_assign(self, trip_id):
        """Pick the nearest AVAILABLE driver to the pickup point and mark them BUSY."""
        state = self.get_trip(trip_id)["state"]
        pk = state["pickup"]
        for radius in (2, 5, 15, 60):
            near = self.nearby(pk["lat"], pk["lng"], radius, 1)
            if near:
                d = near[0]
                self.update_location(d["driver_id"], d["lat"], d["lng"], d["speed"], "BUSY", d["heading"])
                return self.add_trip_event(trip_id, "DRIVER_ASSIGNED", d["driver_id"],
                                           {"distance_km": d["distance_km"]})
        raise ApiError(409, "no available drivers nearby")

    def get_trip(self, trip_id):
        _check_id("trip_id", trip_id)
        state = self.db.get("tripstate:%s" % trip_id)
        if state is None:
            raise ApiError(404, "unknown trip")
        events = [v for _k, v in self.db.scan_prefix("trip:%s:" % trip_id)]
        return {"state": state, "events": events}

    def list_trips(self, limit=40):
        states = [v for _k, v in self.db.scan_prefix("tripstate:")]
        states.sort(key=lambda s: s["updated"], reverse=True)
        return states[:limit]

    def finish_trip_free_driver(self, trip_id):
        """After COMPLETED/CANCELLED make the driver AVAILABLE again."""
        state = self.db.get("tripstate:%s" % trip_id)
        if state and state.get("driver_id"):
            d = self.db.get("driver:latest:%s" % state["driver_id"])
            if d:
                self.update_location(d["driver_id"], d["lat"], d["lng"], 0, "AVAILABLE", d["heading"])


# ======================================================================
# Traffic simulator + benchmark
# ======================================================================
CITY = {"lat_min": 19.040, "lat_max": 19.120, "lng_min": 72.830, "lng_max": 72.920}   # a made-up "demo city" box


def random_point(rng=random):
    return {"lat": rng.uniform(CITY["lat_min"], CITY["lat_max"]),
            "lng": rng.uniform(CITY["lng_min"], CITY["lng_max"])}


class Simulator:
    """Background thread: N drivers do a random walk and ping their GPS; trips are born and progress."""

    def __init__(self, service):
        self.svc = service
        self._thread = None
        self._stop = threading.Event()
        self.drivers = {}
        self.trips = []                     # [trip_id, next_event]
        self.pings_sent = 0
        self.trips_created = 0
        self.ups = 0
        self.rng = random.Random()

    def status(self):
        return {"running": bool(self._thread and self._thread.is_alive()), "drivers": len(self.drivers),
                "updates_per_sec": self.ups, "pings_sent": self.pings_sent,
                "trips_created": self.trips_created, "active_trips": len(self.trips)}

    def start(self, n_drivers=40, updates_per_sec=40):
        self.stop()
        self._stop.clear()
        self.ups = max(1, min(int(updates_per_sec), 2000))
        n_drivers = max(1, min(int(n_drivers), 1000))
        for i in range(n_drivers):
            did = "D%03d" % i
            if did not in self.drivers:
                pt = random_point(self.rng)
                self.drivers[did] = {"lat": pt["lat"], "lng": pt["lng"], "heading": self.rng.uniform(0, 360),
                                     "speed": self.rng.uniform(10, 45), "status": "AVAILABLE"}
        for did in list(self.drivers):
            if int(did[1:]) >= n_drivers:
                del self.drivers[did]
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread and self._thread.is_alive():
            self._stop.set()
            self._thread.join(timeout=3)
        self._thread = None

    def _step_driver(self, did, d):
        d["heading"] = (d["heading"] + self.rng.uniform(-25, 25)) % 360
        d["speed"] = max(0, min(70, d["speed"] + self.rng.uniform(-5, 5)))
        step_deg = d["speed"] / 3600 * 1.5 / 111.0          # ~1.5 s of movement in degrees
        d["lat"] += math.cos(math.radians(d["heading"])) * step_deg
        d["lng"] += math.sin(math.radians(d["heading"])) * step_deg
        if not (CITY["lat_min"] < d["lat"] < CITY["lat_max"]):
            d["heading"] = (180 - d["heading"]) % 360
            d["lat"] = min(max(d["lat"], CITY["lat_min"]), CITY["lat_max"])
        if not (CITY["lng_min"] < d["lng"] < CITY["lng_max"]):
            d["heading"] = (-d["heading"]) % 360
            d["lng"] = min(max(d["lng"], CITY["lng_min"]), CITY["lng_max"])

    def _run(self):
        ids = list(self.drivers)
        tick = 0.1
        per_tick = max(1, self.ups * tick)
        carry = 0.0
        idx = 0
        while not self._stop.is_set():
            t0 = time.time()
            carry += per_tick
            n = int(carry)
            carry -= n
            for _ in range(n):
                did = ids[idx % len(ids)]
                idx += 1
                d = self.drivers.get(did)
                if d is None:
                    continue
                self._step_driver(did, d)
                known = self.svc.db.get("driver:latest:%s" % did)
                status = known["status"] if known else "AVAILABLE"
                try:
                    self.svc.update_location(did, d["lat"], d["lng"], round(d["speed"], 1), status, round(d["heading"], 1))
                    self.pings_sent += 1
                except ApiError:
                    pass
            self._trip_step()
            time.sleep(max(0.0, tick - (time.time() - t0)))

    def _trip_step(self):
        try:
            if self.rng.random() < 0.08 and len(self.trips) < 15:
                st = self.svc.request_trip("R%04d" % self.rng.randint(0, 9999), random_point(self.rng), random_point(self.rng))
                self.svc.auto_assign(st["trip_id"])
                self.trips.append([st["trip_id"], "DRIVER_ARRIVED"])
                self.trips_created += 1
            if self.trips and self.rng.random() < 0.35:
                item = self.rng.choice(self.trips)
                tid, nxt = item
                self.svc.add_trip_event(tid, nxt)
                if nxt == "DRIVER_ARRIVED":
                    item[1] = "TRIP_STARTED"
                elif nxt == "TRIP_STARTED":
                    item[1] = "TRIP_COMPLETED"
                else:
                    self.svc.finish_trip_free_driver(tid)
                    self.trips.remove(item)
        except ApiError:
            pass        # e.g. no free drivers right now - just skip this tick


def run_benchmark(service, n_updates):
    """Write n GPS updates as fast as possible (each = 2 LSM writes). Returns throughput numbers."""
    n_updates = max(1, min(int(n_updates), 200000))
    db = service.db
    before = dict(db.stats()["counters"])
    rng = random.Random(1)
    t0 = time.perf_counter()
    for i in range(n_updates):
        # Same two-write pattern as update_location(), but under a separate "bench:" prefix so the
        # benchmark never pollutes the live driver map.
        pt = random_point(rng)
        ts = int(time.time() * 1000)
        did = "B%04d" % (i % 500)
        point = {"driver_id": did, "lat": round(pt["lat"], 6), "lng": round(pt["lng"], 6),
                 "speed": 30, "heading": 0, "status": "AVAILABLE", "ts": ts}
        db.put("bench:latest:%s" % did, point)
        db.put("bench:hist:%s:%013d-%06d" % (did, TS_REVERSE - ts, 999999 - i), point)
    secs = time.perf_counter() - t0
    after = db.stats()["counters"]
    return {"updates": n_updates, "lsm_writes": n_updates * 2, "seconds": round(secs, 3),
            "updates_per_sec": round(n_updates / secs), "lsm_writes_per_sec": round(2 * n_updates / secs),
            "flushes": after["flushes"] - before["flushes"],
            "compactions": after["compactions"] - before["compactions"]}
