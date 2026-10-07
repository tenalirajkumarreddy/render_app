"""Web app for the old_eci collector.

Run:  python -m uvicorn app:app --host 127.0.0.1 --port 8008     (from work/old_eci)
"""
from __future__ import annotations

import csv
import io
import json
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import client
import db
import worker

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")


@asynccontextmanager
async def lifespan(_app):
    db.init()
    # Dashboard + fleet control plane only on Render (free web service sleeps
    # and wakes, so the always-on collection stays on the devices).
    worker.start_background()
    yield
    worker.STOP.set()


app = FastAPI(title="old_eci collector", version="1.0",
              description="Old-roll (SIR/2003) EPIC harvest for every state / AC / part",
              lifespan=lifespan)


def enqueue(kind, payload, mode="manual", priority=100):
    # Tag the job with THIS device (the web/PC node). Each device's worker claims
    # only its own jobs, so a job queued here is never run by a phone and vice
    # versa - each device owns its tasks. Jobs queued before that had no device
    # (NULL) and stay claimable by anyone.
    row = db.q("insert into jobs(kind, payload, mode, priority, device) "
               "values (%s,%s,%s,%s,%s) returning id, kind, status",
               (kind, json.dumps(payload), mode, priority, db._device_tag()),
               fetch="one")
    db.event("api", "queued #%s %s %s" % (row["id"], kind, json.dumps(payload)[:120]))
    return row


# ----------------------------------------------------------------------- pages

@app.get("/")
def index():
    return FileResponse(os.path.join(WEB, "index.html"))


# ------------------------------------------------------------------- dashboard

# The dashboard polls /api/summary every few seconds, so the endpoint is built
# from cheap queries plus two short-lived caches. `v_overall` is deliberately NOT
# used here: its `count(distinct cur_epic)` over the electors table measured 15s
# on 600k rows, and it grows with the table. Nothing about a KPI card needs to be
# exact to the second, so exact-but-expensive counts are cached and labelled.
_agg_cache = {"at": 0.0, "data": None}
AGG_TTL = 120.0         # electors / unique EPICs
_agg_lock = threading.Lock()     # single-flight: at most ONE heavy aggregate at a time
_bd_cache = {"at": 0.0, "data": None}
BD_TTL = 300.0                   # relation/gender/coverage breakdown
_bd_lock = threading.Lock()
_speed_cache = {"at": 0.0, "data": None}
SPEED_TTL = 5.0         # live speed is still live at 5s old


def heavy_counts():
    """Exact `electors` and distinct-EPIC counts, cached and single-flight.

    `count(distinct cur_epic)` over millions of rows spills >40 MB of temp files
    and takes a minute - with every poller refreshing its own copy at expiry the
    dashboard queries alone saturated the DB server (
    `BufFileRead`/`DataFileRead` waits, statement timeouts on the workers). A
    process-wide lock lets exactly one refresh run; everyone else serves the
    previous value and `stale` marks it. The 2-minute TTL bounds the staleness
    of a number that only feeds a KPI card.
    """
    now = time.time()
    if _agg_cache["data"] is None or now - _agg_cache["at"] > AGG_TTL:
        if _agg_lock.acquire(timeout=0.05):
            try:
                if _agg_cache["data"] is None or now - _agg_cache["at"] > AGG_TTL:
                    row = db.q("""select (select count(*) from electors) electors,
                             (select count(distinct cur_epic) from electors
                               where cur_epic is not null and cur_epic <> '')
                               unique_epics""", fetch="one") or {}
                    _agg_cache.update(at=time.time(), data=dict(row))
            except Exception:
                pass      # serve the previous value; the DB is having a bad day
            finally:
                _agg_lock.release()
    out = dict(_agg_cache["data"] or {})
    out["cached_secs"] = (round(now - _agg_cache["at"], 1)
                          if _agg_cache["data"] is not None else None)
    out["stale"] = bool(_agg_cache["data"] is not None
                        and out["cached_secs"] is not None
                        and out["cached_secs"] > AGG_TTL)
    out["ttl_secs"] = AGG_TTL
    return out


def speed_stats(window_min=15):
    """Live collection speed, in units per second.

    Two different rates, because they answer different questions:

    * `req_per_sec` - serials actually fetched, divided by the time those parts
      were *running*. This is the gateway-facing speed and excludes idle.
    * `records_per_sec` - elector rows landed, divided by the whole wall-clock
      window. This includes idle, so it is what the database actually gains.

    `current` is the in-flight part, timed from `started_at`. `collect_part`
    updates `last_serial` every 200 serials, so this is a real instantaneous
    rate rather than an average over finished work, and it carries an ETA.
    """
    row = db.q("""
        select count(*) parts,
               coalesce(sum(records),0) records,
               coalesce(sum(coalesce(roll_end, records)),0) serials,
               coalesce(sum(extract(epoch from
                 (coalesce(finished_at, now()) - started_at))),0) busy
        from old_parts
        where finished_at > now() - (%s * interval '1 minute')""",
        (window_min,), fetch="one") or {}
    busy = float(row.get("busy") or 0)
    wall = float(window_min) * 60
    parts = row.get("parts") or 0
    out = {
        "window_min": window_min,
        "parts": parts,
        "records": row.get("records") or 0,
        "serials": row.get("serials") or 0,
        "busy_secs": round(busy, 1),
        "parts_per_hour": round(parts / wall * 3600, 1),
        # Wall-clock rate includes idle, so on a quiet system it understates the
        # real speed; the *_busy rates divide by the time parts were actually
        # running, which is the number to compare against the gateway's limits.
        "records_per_sec": round((row.get("records") or 0) / wall, 2),
        "records_per_busy_sec": (round((row.get("records") or 0) / busy, 1)
                                 if busy else None),
        "req_per_sec": round((row.get("serials") or 0) / busy, 1) if busy else None,
        "current": None,
    }
    cur = db.q("""select state_cd, ac_no, part_no, last_serial, roll_end, records,
                         extract(epoch from now()-started_at) elapsed
                  from old_parts where status='running'
                  order by started_at desc limit 1""", fetch="one")
    if not cur:
        # Show the most recently finished part while the worker is between parts,
        # so the card does not flap to "idle" between every collection.
        cur = db.q("""select state_cd, ac_no, part_no, roll_end, records, roll_end last_serial,
                             extract(epoch from (finished_at - started_at)) elapsed
                      from old_parts where finished_at is not null
                      order by finished_at desc limit 1""", fetch="one")
        if cur:
            out["last_finished"] = True
    if cur and cur.get("elapsed"):
        el = max(float(cur["elapsed"]), 0.1)
        done = cur.get("last_serial") or 0
        rate = (done / el) if done else None
        roll_end = cur.get("roll_end")
        out["current"] = {
            "state_cd": cur["state_cd"], "ac_no": cur["ac_no"],
            "part_no": cur["part_no"], "serial": done, "roll_end": roll_end,
            "records": cur.get("records") or 0, "elapsed": round(el, 1),
            "req_per_sec": round(rate, 1) if rate else None,
            "records_per_sec": round((cur.get("records") or 0) / el, 2),
            "eta_secs": (round((roll_end - done) / rate)
                         if rate and roll_end and roll_end > done else None),
        }
    return out


@app.get("/api/summary")
def summary():
    # One pass over old_parts (a few thousand rows) for all the part counters,
    # instead of v_overall's subquery-per-count.
    parts = db.q("""
        select count(*) old_parts,
               count(*) filter (where status='done')    done_parts,
               count(*) filter (where status='pending') pending_parts,
               count(*) filter (where status='running') running_parts,
               count(*) filter (where status='error')   error_parts,
               coalesce(sum(records),0) records,
               coalesce(sum(epics),0)   epics
        from old_parts""", fetch="one") or {}
    overall = dict(parts)
    overall["states"] = db.q("select count(*) c from states", fetch="one")["c"]
    overall["acs"] = db.q("select count(*) c from acs", fetch="one")["c"]
    overall["epic_lookups"] = db.q("select count(*) c from epic_lookups",
                                  fetch="one")["c"]
    overall.update(heavy_counts())
    # Free = unprocessed, unclaimed AND unreserved: running parts already belong
    # to a device and reserved ones are held for a device's next few sweeps, so
    # what is left is exactly the pool another device's picker may scan.
    overall["free_parts"] = ((overall.get("pending_parts") or 0)
                             + (overall.get("error_parts") or 0))
    # Reservations (see worker.reserve_parts): the upcoming parts each device is
    # holding so it never re-competes for its next one. Live holds only - an
    # expired hold is just a row waiting to be reclaimed, not a reserved part.
    ttl = float(db.setting("part_reserve_ttl", 900) or 900)
    holds = db.q("""select reserved_by d, count(*) c from old_parts
                    where reserved_by is not null
                      and status in ('pending','error')
                      and reserved_at >= now() - make_interval(secs => %s)
                    group by reserved_by order by 2 desc, 1""", (ttl,))
    overall["reserved_parts"] = sum(h["c"] for h in holds)
    overall["free_parts"] = max(0, overall["free_parts"] - overall["reserved_parts"])

    # This device's own tasks (plus legacy untagged ones), not the whole fleet's.
    jobs = db.q("""select id, kind, status, mode, device, progress, result, error,
                          created_at, started_at, finished_at from jobs
                   where (device is null or device = %s)
                     and status in ('queued','running')
                   order by id desc limit 8""", (db._device_tag(),))
    # Aggregated per state in one scan each, rather than three correlated
    # subqueries per state (which cost a round trip apiece and measured 0.85s).
    states = db.q("""
        select s.state_cd, s.name, s.has_old_data,
               coalesce(a.acs,0) acs, coalesce(p.parts,0) parts, coalesce(p.done,0) done
        from states s
        left join (select state_cd, count(*) acs from acs group by 1) a
               on a.state_cd = s.state_cd
        left join (select state_cd, count(*) parts,
                          count(*) filter (where status='done') done
                   from old_parts group by 1) p
               on p.state_cd = s.state_cd
        order by s.state_cd limit 60""")

    now = time.time()
    if _speed_cache["data"] is None or now - _speed_cache["at"] > SPEED_TTL:
        _speed_cache.update(at=now, data=speed_stats())
    speed = dict(_speed_cache["data"] or {})
    speed["cached_secs"] = round(now - _speed_cache["at"], 1)
    speed["ttl_secs"] = SPEED_TTL

    # Per-device knobs: the summary shows THIS server's tag's values plus the
    # fleet defaults, every claimant's kind so the operator sees who holds
    # what, and any device that opted itself OUT of the fleet auto default -
    # the phone's switch writes 'auto_enabled@android-<model>', the dashboard
    # here writes the global row everyone else falls back to.
    tag = os.environ.get("ECI_DEVICE_TAG", "")
    settings = {
        "auto_enabled": db.setting("auto_enabled", tag=""),  # fleet default
        "calibrate_offset": db.setting("calibrate_offset", tag=""),
        "workers": (db.setting("workers", ("workers", 6), tag=tag)
                    if tag else db.setting("workers")),
        "discover_max_part": (db.setting("discover_max_part",
                                         ("discover_max_part", 400), tag=tag)
                              if tag else db.setting("discover_max_part")),
        "device_tag": tag or None,
    }
    settings["auto_overrides"] = db.q(
        "select key, value from settings where key like 'auto_enabled@%%' "
        "order by key")
    claimants = db.q("""select split_part(claimed_by, '-', 1) as kind,
                               min(claimed_by) example, count(*)
                        from old_parts where status='running'
                          and claimed_by is not null group by 1""")
    return {
        "overall": overall,
        "jobs": jobs,
        "states": states,
        "worker": worker.worker_status(),
        "speed": speed,
        "settings": settings,
        "claimants": claimants,
        "holds": holds,
    }


@app.get("/api/events")
def events(limit: int = 50, device: str = None, all: int = 0):
    """This device's own log stream by default (`all=1` or `device=*` for the
    whole fleet). Each device shows its own logs - the web page no longer mirrors
    the phones' feed."""
    limit = min(limit, 500)
    if all or device == "*":
        return db.q("select id, ts, level, source, message, device from events "
                    "order by id desc limit %s", (limit,))
    dev = device if device is not None else db._device_tag()
    return db.q("select id, ts, level, source, message, device from events "
                "where device = %s order by id desc limit %s", (dev, limit))


# --------------------------------------------------------------------- catalog

@app.get("/api/states")
def states():
    return db.q("""select s.state_cd, s.name, s.has_old_data,
                          (select count(*) from acs a where a.state_cd=s.state_cd) acs,
                          (select count(*) from old_parts p where p.state_cd=s.state_cd) parts,
                          (select count(*) from old_parts p where p.state_cd=s.state_cd
                            and p.status='done') done
                   from states s order by s.state_cd""")


@app.post("/api/states/seed")
def seed_states():
    return enqueue("seed_states", {})


@app.get("/api/acs")
def acs(state: str):
    return db.q("""select a.state_cd, a.ac_no, a.name, a.ac_type, a.district_cd, a.discover_status,
                          a.old_parts_found, a.discover_max, a.discovered_at, a.last_error,
                          (a.discover_max is not null
                            and a.old_parts_found >= a.discover_max) maybe_truncated,
                          (select count(*) from old_parts p
                            where p.state_cd=a.state_cd and p.ac_no=a.ac_no) parts,
                          (select count(*) from old_parts p
                            where p.state_cd=a.state_cd and p.ac_no=a.ac_no
                              and p.status='done') done
                   from acs a where a.state_cd=%s order by a.ac_no""", (state,))


@app.post("/api/acs/seed")
def seed_acs(payload: dict):
    return enqueue("seed_acs", {"state_cd": payload["state_cd"]})


@app.post("/api/acs/seed_all")
def seed_acs_all():
    """Log every AC of every state at once (one batched write, no ECI traffic)."""
    return enqueue("seed_acs_all", {})


@app.post("/api/acs/discover")
def discover(payload: dict):
    return enqueue("discover_parts", {
        "state_cd": payload["state_cd"], "ac_no": int(payload["ac_no"]),
        "max_part": int(payload.get("max_part") or db.setting("discover_max_part", 400))})


@app.get("/api/parts")
def parts(state: str, ac: int, status: str | None = None,
          q: str | None = None, limit: int = 300, offset: int = 0):
    where = ["p.state_cd=%s", "p.ac_no=%s"]
    params = [state, ac]
    if status:
        where.append("p.status=%s")
        params.append(status)
    if q:
        where.append("(p.name ilike %s or p.part_no::text = %s)")
        params += ["%" + q + "%", q]
    limit = min(limit, 2000)
    rows = db.q("""select p.*, p.mapping_offset as "offset",
                          cp.part_name as cur_part_name, cp.part_name_l1,
                          (select count(*) from electors e
                            where e.state_cd=p.state_cd and e.ac_no=p.ac_no
                              and e.part_no=p.part_no) electors,
                          (select count(distinct e.cur_epic) from electors e
                            where e.state_cd=p.state_cd and e.ac_no=p.ac_no
                              and e.part_no=p.part_no) unique_epics
                   from old_parts p
                   left join current_parts cp
                     on cp.state_cd=p.state_cd and cp.ac_no=p.ac_no
                    and cp.part_no=p.cur_part_mode
                   where %s order by p.part_no limit %s offset %s"""
                % (" and ".join(where), limit, offset), params)
    total = db.q("select count(*) c from old_parts p where %s" % " and ".join(where),
                 params, fetch="one")["c"]
    return {"rows": rows, "total": total}


@app.get("/api/current_parts")
def current_parts(state: str, ac: int, refresh: bool = False):
    """Current-roll parts of an AC (names/ids), cached in the DB."""
    cached = db.q("select count(*) c from current_parts where state_cd=%s and ac_no=%s",
                  (state, ac), fetch="one")["c"]
    if refresh or not cached:
        rows = client.current_parts(state, ac)
        for r in rows:
            db.q("""insert into current_parts(state_cd, ac_no, part_no, part_name,
                        part_name_l1, part_id, district_cd, ps_type, ps_caty,
                        old_pdf_url, fetched_at)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                    on conflict (state_cd, ac_no, part_no) do update set
                      part_name=excluded.part_name, part_name_l1=excluded.part_name_l1,
                      part_id=excluded.part_id, district_cd=excluded.district_cd,
                      ps_type=excluded.ps_type, ps_caty=excluded.ps_caty,
                      old_pdf_url=excluded.old_pdf_url,
                      fetched_at=now()""",
                 (state, ac, r.get("partNumber"), r.get("partName"), r.get("partNameL1"),
                  r.get("partId"), r.get("districtCd"), r.get("psType"), r.get("psCaty"),
                  r.get("oldPdfUrl")), fetch=None)
    return db.q("select * from current_parts where state_cd=%s and ac_no=%s order by part_no",
                (state, ac))


@app.get("/api/part")
def part_detail(state: str, ac: int, part: int):
    row = db.q("""select *, mapping_offset as "offset" from old_parts
                   where state_cd=%s and ac_no=%s and part_no=%s""",
               (state, ac, part), fetch="one")
    if not row:
        raise HTTPException(404, "part not collected/discovered")
    by_cur = db.q("""select cur_state_cd, cur_ac_no, cur_part_no, count(*) n,
                            count(distinct cur_epic) epics
                     from electors where state_cd=%s and ac_no=%s and part_no=%s
                     group by 1,2,3 order by n desc""", (state, ac, part))
    sample = db.q("""select serial_no, full_name, full_name_l1, relative_name,
                            relative_name_l1, relation_type, gender, age_snapshot,
                            epic_2003, cur_epic, cur_ac_no, cur_part_no, marked_by_blo
                     from electors where state_cd=%s and ac_no=%s and part_no=%s
                     order by serial_no limit 30""", (state, ac, part))
    for r in sample:
        r["relation_label"] = client.relation_label(r["relation_type"])
        r["gender_label"] = client.gender_label(r["gender"])
    by_relation = db.q("""select relation_type, count(*) n,
                                 count(*) filter (where cur_epic is not null
                                                  and cur_epic <> '') mapped
                          from electors where state_cd=%s and ac_no=%s and part_no=%s
                          group by 1 order by n desc""", (state, ac, part))
    for r in by_relation:
        r["label"] = client.relation_label(r["relation_type"])
    return {"part": row, "by_current_part": by_cur, "by_relation": by_relation,
            "sample": sample}


# ------------------------------------------------------------------ collection

@app.post("/api/collect")
def collect(payload: dict):
    return enqueue("collect_part", {
        "state_cd": payload["state_cd"], "ac_no": int(payload["ac_no"]),
        "part_no": int(payload["part_no"]), "force": bool(payload.get("force"))},
        priority=int(payload.get("priority", 100)))


@app.post("/api/collect_auto")
def collect_auto(payload: dict):
    return enqueue("collect_auto", {
        "state_cd": payload.get("state_cd"), "ac_no": payload.get("ac_no"),
        "max_parts": int(payload.get("max_parts", 25)),
        "force": bool(payload.get("force"))}, mode="auto")


@app.post("/api/auto")
def auto(payload: dict):
    # Dashboard = fleet control: writes the global row, which every device
    # without its own '@tag' override follows. The phone's own switch writes its
    # scoped row instead, so the phone can opt out without stopping the PC.
    db.set_setting("auto_enabled", bool(payload.get("enabled")))
    db.event("api", "auto mode (fleet default) %s"
             % ("on" if payload.get("enabled") else "off"))
    return {"auto_enabled": db.setting("auto_enabled", tag="")}


@app.post("/api/settings")
def set_settings(payload: dict):
    # session?device= scopes numbers to one device; without it they stay global
    # (the shared defaults every other device falls back to).
    tag = (payload or {}).get("device_tag") or None
    for k, v in (payload or {}).items():
        if k in ("device_tag",):
            continue
        if k in ("workers", "discover_max_part", "collect_serial_cap",
                 "request_pause_ms"):
            db.set_setting(k, v, tag=tag)
    out_keys = ("workers", "discover_max_part", "collect_serial_cap",
                "request_pause_ms")
    return {k: db.setting(k, tag=tag) for k in out_keys}


@app.get("/api/jobs")
def jobs(status: str | None = None, limit: int = 50):
    if status:
        return db.q("""select * from jobs where status=%s order by id desc limit %s""",
                    (status, min(limit, 500)))
    return db.q("select * from jobs order by id desc limit %s", (min(limit, 500),))


@app.post("/api/jobs/{job_id}/cancel")
def cancel(job_id: int):
    db.q("update jobs set cancel=true where id=%s and status in ('queued','running')",
         (job_id,), fetch=None)
    db.event("api", "cancel requested for job #%s" % job_id)
    return {"cancelled": job_id}


# ------------------------------------------------------------------ data + EPIC

@app.get("/api/electors")
def electors(state: str | None = None, ac: int | None = None, part: int | None = None,
             cur_part: int | None = None, epic: str | None = None, q: str | None = None,
             limit: int = 100, offset: int = 0):
    where, params = ["true"], []
    for col, val in (("state_cd", state), ("ac_no", ac), ("part_no", part),
                     ("cur_part_no", cur_part)):
        if val is not None:
            where.append("%s=%%s" % col)
            params.append(val)
    if epic:
        where.append("cur_epic ilike %s")
        params.append("%" + epic + "%")
    if q:
        where.append("(full_name ilike %s or relative_name ilike %s)")
        params += ["%" + q + "%", "%" + q + "%"]
    where_sql = " and ".join(where)
    rows = db.q("""select source_id, state_cd, ac_no, part_no, serial_no, full_name,
                          full_name_l1, relative_name, relative_name_l1, relation_type,
                          gender, age_snapshot, epic_2003, marked_by_blo, cur_state_cd,
                          cur_ac_no, cur_part_no, cur_epic, last_seen
                   from electors where %s order by state_cd, ac_no, part_no, serial_no
                   limit %s offset %s""" % (where_sql, min(limit, 1000), offset), params)
    # The route stores single-letter relation/gender codes; decode for display.
    for r in rows:
        r["relation_label"] = client.relation_label(r["relation_type"])
        r["gender_label"] = client.gender_label(r["gender"])
    total = db.q("select count(*) c from electors where %s" % where_sql, params,
                 fetch="one")["c"]
    return {"rows": rows, "total": total}


@app.get("/api/breakdown")
def breakdown(state: str | None = None, ac: int | None = None):
    """Value domains actually present in the harvest.

    This is the answer to "are we missing any relation type": every code the route
    returned is listed with its decoded label, so an unrecognised code shows up as
    itself instead of vanishing.
    """
    where, params = ["true"], []
    if state:
        where.append("state_cd=%s")
        params.append(state)
    if ac is not None:
        where.append("ac_no=%s")
        params.append(ac)
    w = " and ".join(where)
    # Cached + single-flight: the dashboard used to re-run these three
    # full-table aggregates every 3s poll, and with electors in the millions
    # their BufferMapping waits starved the collectors (statement timeouts on
    # the PC, IO errors on the phone). A slow-changing value domain does not
    # need per-poll freshness. (Server-side, so every device/browser shares
    # one cache.)
    cur = time.time()
    if _bd_cache["data"] is None or cur - _bd_cache["at"] > BD_TTL:
        if _bd_lock.acquire(timeout=0.05):
            try:
                if _bd_cache["data"] is None or cur - _bd_cache["at"] > BD_TTL:
                    rel = db.q("""select relation_type code, count(*) n,
                         count(*) filter (where cur_epic is not null
                                          and cur_epic <> '') mapped
                  from electors where %s group by 1 order by n desc""" % w, params)
                    gen = db.q("""select gender code, count(*) n,
                         count(*) filter (where cur_epic is not null
                                          and cur_epic <> '') mapped
                  from electors where %s group by 1 order by n desc""" % w, params)
                    cov = db.q("""select count(*) total,
                        count(*) filter (where cur_epic is not null
                                         and cur_epic <> '') with_epic,
                        count(*) filter (where relative_name is null
                                         or relative_name = '') no_relative,
                        count(*) filter (where full_name is null
                                         or full_name = '') no_name
                 from electors where %s""" % w, params, fetch="one")
                    if rel or gen or cov:
                        _bd_cache.update(at=time.time(), data={"rel": rel,
                                                               "gen": gen,
                                                               "cov": dict(cov or {})})
            except Exception:
                pass    # serve the previous cache; the DB is having a bad day
            finally:
                _bd_lock.release()
    if _bd_cache["data"] is not None:
        data = _bd_cache["data"]
    else:
        # Nothing cached yet (or the refresh itself failed): one uncached shot,
        # so the table is never empty on first use.
        data = {"rel": rel if "rel" in dir() else [],
                "gen": gen if "gen" in dir() else [],
                "cov": dict(cov) if "cov" in dir() else {}}
    return {
        "relation": [dict(r, label=client.relation_label(r["code"]),
                          short=client.relation_label(r["code"], short=True))
                     for r in data["rel"]],
        "gender": [dict(r, label=client.gender_label(r["code"])) for r in data["gen"]],
        "coverage": data["cov"],
        "cached_secs": round(cur - _bd_cache["at"], 1) if _bd_cache["data"] else None,
        "ttl_secs": BD_TTL,
    }


@app.get("/api/export.csv")
def export_csv(state: str, ac: int, part: int | None = None, cur_part: int | None = None,
               epics_only: bool = True):
    where, params = ["state_cd=%s", "ac_no=%s"], [state, ac]
    if part is not None:
        where.append("part_no=%s")
        params.append(part)
    if cur_part is not None:
        where.append("cur_part_no=%s")
        params.append(cur_part)
    if epics_only:
        where.append("cur_epic is not null and cur_epic <> ''")
    rows = db.q("""select cur_epic, full_name, full_name_l1, relative_name,
                          relative_name_l1, relation_type, gender, age_snapshot,
                          epic_2003, serial_no, part_no, cur_ac_no, cur_part_no
                   from electors where %s
                   order by cur_ac_no, cur_part_no, cur_epic""" % " and ".join(where),
                params)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["epic", "name", "name_local", "relation_type", "relation",
                "relation_local", "gender", "age_2003", "epic_2003", "old_serial",
                "old_part", "cur_ac", "cur_part"])
    for r in rows:
        w.writerow([r["cur_epic"], r["full_name"], r["full_name_l1"],
                    client.relation_label(r["relation_type"]), r["relative_name"],
                    r["relative_name_l1"], client.gender_label(r["gender"]),
                    r["age_snapshot"], r["epic_2003"], r["serial_no"],
                    r["part_no"], r["cur_ac_no"], r["cur_part_no"]])
    name = "epics_%s_AC%s%s.csv" % (state, ac, ("_P%s" % part) if part else "")
    return StreamingResponse(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                             media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="%s"' % name})


@app.post("/api/epic")
def epic_run(payload: dict):
    epic = "".join(str(payload["epic"]).split()).upper()
    if not epic:
        raise HTTPException(400, "epic required")
    cached = db.q("select * from epic_lookups where epic=%s", (epic,), fetch="one")
    if cached and not payload.get("refresh"):
        return {"cached": True, "row": cached}
    out = worker.epic_lookup_job(db.connect(), None, epic)
    row = db.q("select * from epic_lookups where epic=%s", (epic,), fetch="one")
    return {"cached": False, "result": out, "row": row}


@app.get("/api/epic/{epic}")
def epic_get(epic: str):
    row = db.q("select * from epic_lookups where epic=%s", (epic.strip().upper(),),
               fetch="one")
    if not row:
        raise HTTPException(404, "not looked up yet")
    return row


@app.get("/api/epics")
def epics(limit: int = 50):
    return db.q("""select epic, found, http_status, name, part_no, part_name, ac_name,
                          serial_no, fetched_at from epic_lookups
                   order by fetched_at desc limit %s""", (min(limit, 500),))


@app.post("/api/worker/restart")
def restart_worker():
    """Revive the worker thread when it is missing or stalled.

    A stuck remote connection used to leave the worker looking healthy while
    queued jobs went nowhere; this is the recovery path that does not need an
    app restart.
    """
    return worker.ensure_worker()


@app.get("/api/health")
def health():
    st = worker.worker_status()
    return {"ok": True, "schema": db.SCHEMA, "db": db.DSN.split("@")[-1],
            "auto": db.setting("auto_enabled", tag=""),  # fleet default
            "worker_alive": st["alive"],
            "worker_tick_age": st["tick_age"], "worker_error": st["error"]}


if __name__ == "__main__":
    import uvicorn
    db.init()
    print("serving on http://127.0.0.1:8008")
    uvicorn.run(app, host="127.0.0.1", port=8008, log_level="info")
