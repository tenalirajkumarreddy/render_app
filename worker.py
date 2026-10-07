"""Background worker for the old_eci collector.

One thread owns a connection and pulls jobs from old_eci.jobs
(FOR UPDATE SKIP LOCKED). Job kinds:

  seed_states     copy the state list (from the database's public.states if
                  present, else the live anonymous API)
  seed_acs        same for one state's assembly constituencies
  discover_parts  walk candidate old part numbers of one AC and record the ones
                  that exist in the old roll (name discovered from the route)
  collect_part    full serial sweep of one old part -> electors rows; marks the
                  part done so it is never repeated (unless force)
  collect_auto    keep picking the best pending part and collecting it
  epic_lookup     run one EPIC through the national search and store the record

Auto mode (settings.auto_enabled) turns idle time into collection: the worker
picks the best pending part by itself until nothing is left or it is switched
off.
"""
from __future__ import annotations

import json
import os
import random
import socket
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import client
import db

# Identity written into old_parts.claimed_by when this process claims a part,
# so the dashboard can see which PC process, phone or Colab holds what.
HOST_TAG = socket.gethostname()
WORKER_ID = "pc-%s-%s" % (HOST_TAG, os.getpid())
# Per-device settings key: 'key@inspiron' overrides the shared 'key' default.
# Each device runs its own concurrency - the phone 8 workers while the PC runs
# 2 - without either flip-flopping the other between polls.
DEVICE_TAG = os.environ.get("ECI_DEVICE_TAG", HOST_TAG)

STOP = threading.Event()
# `tick` is bumped every loop turn so a stalled worker is visible instead of
# looking healthy. `thread` lets /api/worker/restart revive one that died.
STATE = {"running": False, "job_id": None, "note": "", "tick": 0.0,
         "started": 0.0, "error": None, "thread": None, "reserved": 0}

# A worker that has not turned its loop in this long is stuck (see db.CONN_KWARGS
# for what stops a dropped connection from hanging forever). A single old part can
# take up to ~60s, and collect_part bumps the heartbeat as it sweeps, so this only
# trips on a genuine stall.
STALE_AFTER = 90.0

# ------------------------------------------------ per-device reservations
# Two devices that rank the same part both pay a pick and one loses the claim:
# the loser wasted the pick (a ~5s scan of every pending part now that the
# backlog is 30k+) and a sweep's worth of attention. A reservation moves that
# competition off the part and onto a cheap hold: `old_parts.reserved_by` hides
# an upcoming part from every other picker, so the device's next part is already
# decided and taking it is one indexed point update. See reserve_parts() for the
# full contract - the reservation is advisory, the atomic claim is still the
# only thing that guarantees a part is swept once.
RESERVE_N = 5        # upcoming parts held per device
# How long an unused hold survives. Must comfortably exceed N x part duration
# (a part is ~5-60s, so the tail of a 5-deep queue is read ~5 minutes after it
# was held) - below that a device would lose the back of its own queue. Expiry
# is a time predicate in the SQL, so a crashed or stopped device's holds free
# themselves even if no reaper is alive.
RESERVE_TTL = 900.0

_TTL_CACHE = {"value": None, "at": 0.0}


def worker_status():
    age = time.time() - STATE["tick"] if STATE["tick"] else None
    return {"running": STATE["running"], "job_id": STATE["job_id"],
            "note": STATE["note"], "error": STATE["error"],
            "reserved": STATE.get("reserved", 0),
            "tick_age": None if age is None else round(age, 1),
            "alive": bool(STATE["running"] and age is not None and age < STALE_AFTER)}


def reserve_ttl():
    """Seconds an unused hold survives (per-device setting).

    Cached briefly because the picker, the claim, the reaper and the expiry
    sweep all test the same boundary: they must agree on one number within a
    turn, or a part could be 'expired' for the claim and 'live' for the reaper.
    """
    now = time.time()
    if _TTL_CACHE["value"] is None or now - _TTL_CACHE["at"] > 30.0:
        try:
            v = float(db.setting("part_reserve_ttl", RESERVE_TTL,
                                 tag=DEVICE_TAG))
        except Exception:  # noqa: BLE001 - any read failure keeps the default
            v = RESERVE_TTL
        # Floored at a minute: a tiny TTL would expire holds faster than a
        # single part takes and turn the queue into pure churn.
        _TTL_CACHE.update(value=max(60.0, v), at=now)
    return _TTL_CACHE["value"]


def reserve_n():
    """How many upcoming parts this device holds (per-device setting)."""
    try:
        return max(0, int(db.setting("part_reserve_n", RESERVE_N,
                                     tag=DEVICE_TAG)))
    except Exception:  # noqa: BLE001
        return RESERVE_N


# ------------------------------------------------------------------ job queue

def claim_job(conn):
    """Claim the next queued job THIS device owns. Jobs carry the tag of the
    device that queued them, so a phone's manual job is never run by the web
    worker (and vice versa) - each device has its own tasks. Legacy rows with no
    device stay claimable by anyone."""
    with conn.cursor() as cur:
        cur.execute("""
            update jobs set status='running', started_at=now()
            where id = (select id from jobs
                        where status='queued'
                          and (device is null or device = %s)
                        order by priority desc, id
                        limit 1 for update skip locked)
            returning *""", (DEVICE_TAG,))
        return cur.fetchone()


def job_cancelled(conn, job_id):
    if job_id is None:
        return False
    row = db.q("select cancel from jobs where id=%s", (job_id,), fetch="one")
    return bool(row and row["cancel"])


def finish(conn, job_id, status, result=None, error=None):
    if job_id is None:
        return
    db.q("update jobs set status=%s, finished_at=now(), result=%s, error=%s "
         "where id=%s", (status, json.dumps(result) if result is not None else None,
                         error, job_id), fetch=None)


def progress(conn, job_id, prog):
    if job_id is None:
        return
    db.q("update jobs set progress=%s where id=%s",
         (json.dumps(prog), job_id), fetch=None)


def recover_orphans(conn=None):
    """Requeue work whose holder is gone. `conn` is accepted for call-site
    symmetry; every query goes through `db.q`.

    Multi-device safe: only STALE rows are touched, never a part that a live PC
    process, phone or Colab is sweeping this minute. A part being collected
    touches its row (batch flush + last_serial) every 200 serials, so 'running
    and not updated for 10 minutes' means the holder is gone - crash, killed
    window, dead emulator. Runs at startup AND every ~60s from the loop, so any
    surviving device picks up a dead device's parts live. Re-collecting is safe:
    the elector upsert is keyed on `source_id`, so a second sweep overwrites
    rows instead of duplicating them.
    """
    parts = db.q("""update old_parts set status='pending', last_serial=0,
                          claimed_by=null, updated_at=now()
                   where status='running'
                     and updated_at < now() - interval '10 minutes'
                   returning state_cd, ac_no, part_no""", fetch="all")
    # Expired holds: a device that was killed while holding a queue. The picker
    # already ignores an expired hold, so this is hygiene (it keeps the partial
    # index tiny and makes the release visible in the log) rather than the thing
    # that frees the part.
    # `returning s.reserved_by` (the CTE, not the updated row) is what makes the
    # freed holds still name their owner - the update has already nulled it.
    holds = db.q("""with stale as (
                     select state_cd, ac_no, part_no, reserved_by from old_parts
                      where reserved_by is not null and reserved_by <> %s
                        and status in ('pending','error')
                        and reserved_at < now() - make_interval(secs => %s))
                   update old_parts p set reserved_by=null, reserved_at=null
                     from stale s
                    where (p.state_cd, p.ac_no, p.part_no) =
                          (s.state_cd, s.ac_no, s.part_no)
                   returning s.reserved_by""",
                 (DEVICE_TAG, reserve_ttl()), fetch="all")
    jobs = db.q("""update jobs set status='error', finished_at=now(),
                          error='interrupted'
                   where status='running'
                     and started_at < now() - interval '30 minutes'
                   returning id""", fetch="all")
    acs = db.q("""update acs set discover_status='pending'
                  where discover_status='running'
                    and (discover_started_at is null or
                         discover_started_at < now() - interval '30 minutes')
                  returning state_cd, ac_no""", fetch="all")
    if parts:
        db.event("worker", "requeued %d stale part(s): %s"
                 % (len(parts), ", ".join("%s AC%s P%s" % (p["state_cd"], p["ac_no"],
                                                            p["part_no"])
                                          for p in parts)))
    if jobs:
        db.event("worker", "marked %d interrupted job(s) as error: %s"
                 % (len(jobs), ", ".join("#%s" % j["id"] for j in jobs)), level="warn")
    if acs:
        db.event("worker", "requeued %d stale AC discovery/discoveries: %s"
                 % (len(acs), ", ".join("%s AC%s" % (a["state_cd"], a["ac_no"])
                                        for a in acs)))
    if holds:
        db.event("worker", "freed %d expired part reservation(s) (%s)"
                 % (len(holds), ", ".join(sorted({h["reserved_by"]
                                                  for h in holds}))),
                 level="warn")
    return {"parts": len(parts or []), "jobs": len(jobs or []),
            "acs": len(acs or []), "holds": len(holds or [])}


# ----------------------------------------------------------------- the best part

BEST_PART_SQL = """
select p.state_cd, p.ac_no, p.part_no, p.name,
       (select count(*) from old_parts d
         where d.state_cd=p.state_cd and d.ac_no=p.ac_no and d.status='done'
           and abs(d.part_no - p.part_no) <= 2)                      as neighbours_done,
       coalesce((select avg(d.epics) from old_parts d
         where d.state_cd=p.state_cd and d.ac_no=p.ac_no and d.status='done'
           and abs(d.part_no - p.part_no) <= 2), 0)                  as neighbour_yield
from old_parts p
where p.status in ('pending','error') and coalesce(p.exists_, true)
  -- Per-device reservations (see reserve_parts): a part another device holds
  -- is invisible here, which is what stops two devices from ranking the same
  -- part at all. An expired hold is free again, and this device's own holds
  -- stay visible so its queue can be drained.
  and (p.reserved_by is null
       or p.reserved_by = %s
       or p.reserved_at < now() - make_interval(secs => %s))
  {filters}
-- The last sort key is a per-DEVICE hash of the part's identity, not its
-- number. Before this the ranking was fully deterministic, so every device
-- picked the very same top part in the same second and all but one lost the
-- claim ('both doing the same thing'). Hashing with the device tag spreads the
-- equally-good frontier parts across devices while the real ranking keys
-- (finished neighbours first, then yield) still dominate - so each device
-- works its own slice of the same AC and claims stop colliding.
order by neighbours_done desc, neighbour_yield desc,
         md5(p.state_cd || ':' || p.ac_no::text || ':' || p.part_no::text || %s),
         p.state_cd, p.ac_no, p.part_no
limit {limit}
"""


def best_pending_part(conn, state_cd=None, ac_no=None):
    filters, params = "", []
    if state_cd:
        filters += " and p.state_cd = %s"
        params.append(state_cd)
    if ac_no is not None:
        filters += " and p.ac_no = %s"
        params.append(ac_no)
    # Bind order follows the SQL text: reservation predicate, filters, md5.
    return db.q(BEST_PART_SQL.format(filters=filters, limit=1),
                [DEVICE_TAG, reserve_ttl()] + params + [DEVICE_TAG], fetch="one")


def best_pending_parts(conn, state_cd=None, ac_no=None, limit=1, fresh=False):
    """Best `limit` collectable parts. Picking is advisory - the atomic claim
    (claim_part) is what actually reserves a part, so overlapping picks across
    devices resolve to visible skips, never duplicate sweeps.

    `fresh=True` also skips THIS device's own live holds. The reservation
    top-up needs that: its own holds rank top for its own tag (they were chosen
    by this same ranking a moment ago), so without it the candidate list would
    be filled with parts it already has and the queue would starve. Expired
    holds - anyone's - are still candidates, which is how a device recovers a
    part it let lapse.
    """
    filters, params = "", []
    if state_cd:
        filters += " and p.state_cd = %s"
        params.append(state_cd)
    if ac_no is not None:
        filters += " and p.ac_no = %s"
        params.append(ac_no)
    if fresh:
        filters += (" and (p.reserved_by is null or p.reserved_at < now()"
                    " - make_interval(secs => %s))")
        params.append(reserve_ttl())
    return db.q(BEST_PART_SQL.format(filters=filters, limit="%s"),
                [DEVICE_TAG, reserve_ttl()] + params + [DEVICE_TAG, int(limit)],
                fetch="all")


def claim_part(state_cd, ac_no, part_no):
    """Atomically claim a part for collection; False when someone else has it.

    The `where status <> 'running'` guard makes the claim a single atomic
    statement: with the web app's 9 worker processes, phones and Colab racing on
    one database, exactly one claimer wins and every loser sees a visible skip
    instead of double-sweeping the same part through the gateway.

    A live reservation held by ANOTHER device also blocks the claim, so a pick
    made just before that device reserved the part cannot steal it - the hold is
    a real hold, not a hint. This device's own hold always passes, which is how
    the reservation queue is drained (see take_reserved). Winning clears the
    hold: `reserved_by` is only ever set on a pending/error row waiting its turn.
    """
    row = db.q("""insert into old_parts(state_cd, ac_no, part_no, status,
                   started_at, attempts, claimed_by)
            values (%s,%s,%s,'running', now(), 1, %s)
            on conflict (state_cd, ac_no, part_no) do update set
              status='running', started_at=now(),
              attempts=old_parts.attempts+1, last_error=null,
              claimed_by=excluded.claimed_by,
              reserved_by=null, reserved_at=null, updated_at=now()
            where old_parts.status <> 'running'
              and (old_parts.reserved_by is null
                   or old_parts.reserved_by = %s
                   or old_parts.reserved_at < now() -
                      make_interval(secs => %s))
            returning state_cd""",
         (state_cd, ac_no, part_no, WORKER_ID, DEVICE_TAG, reserve_ttl()),
         fetch="one")
    return row is not None


def hold_reason(state_cd, ac_no, part_no):
    """Why a claim was refused, for the visible skip line. Runs only on a lost
    claim, so the normal path still costs exactly one round trip."""
    row = db.q("""select status, claimed_by, reserved_by,
                        (reserved_by is not null and reserved_at is not null
                         and reserved_at >= now() - make_interval(secs => %s))
                          as held
                 from old_parts where state_cd=%s and ac_no=%s and part_no=%s""",
               (reserve_ttl(), state_cd, ac_no, part_no), fetch="one")
    if row is None:
        return "not in catalogue"
    if row["status"] == "running":
        return "already running on %s" % (row["claimed_by"] or "another device")
    if row["held"]:
        return "reserved by %s" % row["reserved_by"]
    return "status changed to %s" % row["status"]


# ------------------------------------------------------- reservation queue
def reserve_count():
    """How many usable parts this device is holding right now.

    Live holds only: an expired one is no longer really held (any device may
    take it, and take_reserved still offers it to us as a bonus) so it must not
    count towards the queue depth - otherwise a stalled device would think its
    queue was full and never top up again.
    """
    row = db.q("""select count(*) c from old_parts
                 where reserved_by=%s and status in ('pending','error')
                   and reserved_at >= now() - make_interval(secs => %s)""",
               (DEVICE_TAG, reserve_ttl()), fetch="one")
    return row["c"] if row else 0


def reserve_parts(conn=None, state_cd=None, ac_no=None, n=None):
    """Hold up to `n` upcoming parts for this device, cheapest way possible.

    Two statements: the existing picker chooses the candidates - it already
    skips every live hold on the fleet, so the candidates are parts nobody else
    is holding - then ONE batch update stamps them. The stamp is the atomic
    step: a candidate another device reserved in the meantime simply does not
    match, so a lost race costs a missing row, not a wasted sweep, and the
    caller's next turn tops up again.

    Re-stamping this device's own live hold keeps its original `reserved_at`, so
    re-selecting it cannot push it to the back of the queue; an expired hold of
    this device is refreshed to now() (it genuinely is the newest).
    """
    if n is None:
        n = reserve_n()
    if n <= 0:
        return []
    need = n - reserve_count()
    if need <= 0:
        return []
    ttl = reserve_ttl()
    # Two spare candidates: losing one to a device that stamped it first is
    # normal, and the next top-up picks up the difference.
    cands = best_pending_parts(conn, state_cd, ac_no, limit=need + 2, fresh=True)
    keys = [(c["state_cd"], c["ac_no"], c["part_no"]) for c in cands][:need]
    if not keys:
        return []
    rows_txt = ",".join(["(%s,%s,%s)"] * len(keys))
    params = []
    for k in keys:
        params.extend(k)
    # Every driver placeholder is doubled: the candidate list is spliced in with
    # a `%` formatting step, which would otherwise eat the psycopg `%s`s. The
    # stamp takes a part that is free or whose hold has lapsed - including this
    # device's own lapsed hold, which is refreshed to now() (it genuinely is the
    # newest) - so a live hold is never stolen and never re-timestamped.
    return db.q("""update old_parts set
                    reserved_by = %%s, reserved_at = now()
                 where (state_cd, ac_no, part_no) in (%s)
                   and status in ('pending','error')
                   and (reserved_by is null
                        or reserved_at < now() - make_interval(secs => %%s))
                 returning state_cd, ac_no, part_no, name""" % rows_txt,
                 (DEVICE_TAG,) + tuple(params) + (ttl,), fetch="all")


def take_reserved(n=1, conn=None):
    """Move up to `n` of this device's held parts into `running`, oldest first.

    This is the whole point of the reservation: the sub-select reads only this
    device's handful of holds (partial index, a few rows) and the update is a
    point write, so the next part costs about a millisecond instead of re-ranking
    33k pending rows and racing the fleet for the same top one. FIFO on
    `reserved_at` means a hold is never held past its turn, so the queue cannot
    starve behind a part that keeps getting re-ranked to the front.

    The returned parts are already claimed, so collect_part must be told
    (`preclaimed=True`) or it would lose the race against its own take.
    """
    if n <= 0:
        return []
    return db.q("""update old_parts p set
                    status='running', started_at=now(),
                    attempts=p.attempts+1, last_error=null,
                    claimed_by=%s, reserved_by=null, reserved_at=null,
                    updated_at=now()
                 where (p.state_cd, p.ac_no, p.part_no) in (
                     select r.state_cd, r.ac_no, r.part_no from old_parts r
                      where r.reserved_by=%s and r.status in ('pending','error')
                      order by r.reserved_at, r.state_cd, r.ac_no, r.part_no
                      limit %s)
                   and p.status in ('pending','error') and p.reserved_by=%s
                 returning p.state_cd, p.ac_no, p.part_no, p.name""",
                 (WORKER_ID, DEVICE_TAG, int(n), DEVICE_TAG), fetch="all")


def release_reservations():
    """Give back every unused hold of this device; returns how many were freed.

    Called on a clean stop and when a device stops working on parts, so a
    stopped collector does not hide five parts from the fleet for the whole TTL.
    The expiry predicate is what covers a crash - this is the polite path.
    """
    rows = db.q("""update old_parts set reserved_by=null, reserved_at=null
                   where reserved_by=%s and status in ('pending','error')
                   returning state_cd""", (DEVICE_TAG,), fetch="all")
    return len(rows or [])


# --------------------------------------------------------------------- catalog

def seed_states(conn):
    rows = []
    pub = db.geo_q("select state_cd, name from public.states")
    if pub:
        rows = [{"state_cd": r["state_cd"], "name": r["name"], "source": "db"} for r in pub]
    if not rows:
        rows = [{"state_cd": r["state_cd"], "name": r["name"], "source": "api"}
                for r in client.states_live()]
    for r in rows:
        db.q("insert into states(state_cd,name,source) values (%s,%s,%s) "
             "on conflict (state_cd) do update set name=coalesce(excluded.name, states.name)",
             (r["state_cd"], r["name"], r["source"]), fetch=None)
    return {"states": len(rows)}


def seed_acs(conn, state_cd):
    """One state's ACs.

    Delegates to `seed_acs_all` so both paths behave identically. They used to
differ: this one trusted the legacy catalogue whenever it had any rows, so it
    never picked up the 12 ACs the catalogue omits for S01 and never stored
    `ac_type` - the same job giving different answers depending on the button.
    """
    return seed_acs_all(conn, state_cd=state_cd)


def seed_acs_all(conn, live=True, state_cd=None):
    """Seed ACs, from the legacy catalogue merged with live (all states, or one).

    The per-state `seed_acs` round-trips once per row, which is fine for one
    state but means minutes across the ~4k ACs of the whole country. This pulls
    them all with a single write.

    The legacy catalogue alone is NOT complete: it lists 175 ACs for S01 while
    `citizen/sir/getAsmbly` serves 187, and every one of the 12 extra ACs has
    old-roll data. Taking the catalogue at face value silently skipped them, so
    the live list is merged on top by (state_cd, ac_no).
    """
    merged = {}
    geo_sql = ("select state_cd, ac_number, ac_name, district_cd from public.acs "
               + ("where state_cd=%s " if state_cd else "")
               + "order by state_cd, ac_number")
    for r in db.geo_q(geo_sql, (state_cd,) if state_cd else None):
        merged[(r["state_cd"], int(r["ac_number"]))] = {
            "name": r.get("ac_name"), "name_l1": None,
            "district_cd": r.get("district_cd")}
    from_legacy = len(merged)

    live_only = 0
    if live:
        state_rows = ([{"state_cd": state_cd}] if state_cd
                      else db.q("select state_cd from states order by state_cd"))
        for s in state_rows:
            sc = s["state_cd"]
            try:
                rows = client.acs_live(sc)
            except Exception as exc:  # noqa: BLE001 - catalogue is best effort
                db.event("worker", "live AC list failed for %s: %s" % (sc, exc),
                         level="warn")
                continue
            for r in rows:
                key = (sc, int(r["ac_no"]))
                if key not in merged:
                    merged[key] = {}
                    live_only += 1
                cur_ = merged[key]
                cur_["name"] = r.get("name") or cur_.get("name")
                cur_["name_l1"] = r.get("name_l1") or cur_.get("name_l1")
                cur_["ac_type"] = r.get("ac_type") or cur_.get("ac_type")
                cur_["district_cd"] = r.get("district_cd") or cur_.get("district_cd")

    if not merged:
        return {"acs": 0, "source": "none"}
    with conn.cursor() as cur:
        cur.executemany(
            """insert into acs(state_cd, ac_no, name, name_l1, district_cd, ac_type)
               values (%s,%s,%s,%s,%s,%s)
               on conflict (state_cd, ac_no) do update set
                 name=coalesce(excluded.name, acs.name),
                 name_l1=coalesce(excluded.name_l1, acs.name_l1),
                 ac_type=coalesce(excluded.ac_type, acs.ac_type),
                 district_cd=coalesce(excluded.district_cd, acs.district_cd)""",
            [(sc, ac, v.get("name"), v.get("name_l1"), v.get("district_cd"),
              v.get("ac_type"))
             for (sc, ac), v in sorted(merged.items())])
    states = len({sc for sc, _ in merged})
    db.event("worker", "seeded %d ACs across %d states (%d from the legacy catalogue, "
             "%d only in the live list)" % (len(merged), states, from_legacy, live_only))
    return {"acs": len(merged), "states": states, "from_legacy": from_legacy,
            "live_only": live_only}


# Part numbers are probed in chunks and discovery stops only when a whole chunk
# comes back empty, so the cap is a floor rather than a limit.
PART_CHUNK = 200
PART_HARD_CAP = 3000


def discover_parts(conn, job_id, state_cd, ac_no, max_part=None):
    """Probe part numbers upward until a whole chunk is empty.

    `max_part` (`discover_max_part`) is a FLOOR, not a limit. Stopping exactly at
    a cap truncates silently: S01 AC 1 was first discovered with a cap of 12 and
    recorded as "12 parts, done", which hid 141 real parts until a cross-check
    against the live part list caught it. So probing continues while the top of
    each chunk is live, up to PART_HARD_CAP, and the result reports whether it
    hit that ceiling.
    """
    floor = int(db.setting("discover_max_part", max_part or 400,
                           tag=DEVICE_TAG))
    db.q("update acs set discover_status='running', discover_max=%s, last_error=null, "
         "discover_started_at=now() where state_cd=%s and ac_no=%s",
         (floor, state_cd, ac_no), fetch=None)
    workers = int(db.setting("workers", ("workers", 6), tag=DEVICE_TAG))

    def probe(n):
        status, payload = client.fetch_window(state_cd, ac_no, n)
        name = None
        for rec in payload or []:
            if rec.get("oldPartName"):
                name = rec["oldPartName"]
                break
        exists = bool(payload) or status == 200
        if exists and not payload:
            for serial in (1, 25, 100):
                st, pl = client.fetch_serial(state_cd, ac_no, n, serial)
                if pl:
                    return n, True, pl[0].get("oldPartName")
            return n, False, None
        return n, exists, name

    found = {}
    probed_to = 0
    start = 1
    while start <= PART_HARD_CAP:
        end = min(start + PART_CHUNK - 1, PART_HARD_CAP)
        if probed_to < floor:
            end = min(max(end, floor), PART_HARD_CAP)
        hits = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for n, exists, name in pool.map(probe, range(start, end + 1)):
                if exists:
                    found[n] = name
                    hits += 1
        probed_to = end
        progress(conn, job_id, {"phase": "discover", "probed_to": end,
                                "found": len(found)})
        # A whole empty chunk past the floor means there is nothing above.
        if hits == 0 and probed_to >= floor:
            break
        start = end + 1

    truncated = bool(found.get(probed_to)) and probed_to >= PART_HARD_CAP
    with conn.cursor() as cur:
        cur.executemany("""insert into old_parts(state_cd, ac_no, part_no, name,
                                                  exists_, status)
                           values (%s,%s,%s,%s,true,'pending')
                           on conflict (state_cd, ac_no, part_no)
                           do update set name=coalesce(excluded.name, old_parts.name),
                                         exists_=true, updated_at=now()""",
                        [(state_cd, ac_no, n, name) for n, name in sorted(found.items())])
    db.q("update states set has_old_data=true, last_checked=now() where state_cd=%s",
         (state_cd,), fetch=None)
    # discover_max records how far probing actually reached, so an AC whose
    # old_parts_found ever equals it can be spotted as possibly truncated.
    db.q("update acs set discover_status='done', old_parts_found=%s, discover_max=%s, "
         "discovered_at=now() where state_cd=%s and ac_no=%s",
         (len(found), probed_to, state_cd, ac_no), fetch=None)
    db.event("catalog", "discover %s AC %s: %d old parts (probed 1..%s%s)"
             % (state_cd, ac_no, len(found), probed_to,
                " - TRUNCATED at the hard cap" if truncated else ""))
    return {"found": len(found), "probed_to": probed_to, "truncated": truncated}


# ------------------------------------------------------------------ collection

ELECTOR_UPSERT = """
insert into electors(source_id, state_cd, ac_no, part_no, serial_no,
        full_name, full_name_l1, relative_name, relative_name_l1, relation_type,
        gender, age_snapshot, epic_2003, marked_by_blo,
        cur_state_cd, cur_ac_no, cur_part_no, cur_epic, last_seen)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
on conflict (source_id) do update set
  serial_no=excluded.serial_no, full_name=excluded.full_name,
  full_name_l1=excluded.full_name_l1, relative_name=excluded.relative_name,
  relative_name_l1=excluded.relative_name_l1, relation_type=excluded.relation_type,
  gender=excluded.gender, age_snapshot=excluded.age_snapshot,
  epic_2003=excluded.epic_2003, marked_by_blo=excluded.marked_by_blo,
  cur_state_cd=excluded.cur_state_cd, cur_ac_no=excluded.cur_ac_no,
  cur_part_no=excluded.cur_part_no, cur_epic=excluded.cur_epic,
  last_seen=now()
"""


def _row_tuple(rec, state, ac, part):
    def age(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    return (rec.get("id"), state, ac, part,
            age(rec.get("oldPartSerialNo")),
            rec.get("oldFullName") or rec.get("firstName"),
            rec.get("oldFullNameL1"),
            rec.get("oldRelativeFullName") or rec.get("relativeFName"),
            rec.get("oldRelativeFullNameL1"),
            rec.get("relationType"), rec.get("gender"), age(rec.get("age")),
            rec.get("epicNumber"), rec.get("markedByBlo"),
            rec.get("bloMappedStateCd"), age(rec.get("bloMappedAcNo")),
            age(rec.get("bloMappedPartNo")), rec.get("bloMappedEpicNo"))


def collect_part(conn, job_id, state_cd, ac_no, part_no, force=False, width=1,
                 preclaimed=False):
    """Claim + sweep + finalise, with the failure hand-back in one place.

    `width` parts are being collected in parallel by this worker; the shared
    `workers` budget is divided across them so the gateway sees the same total
    concurrency as a sequential sweep - the parallel win is phase overlap, not
    more requests. conn=None runs every statement through the shared pool (the
    pipeline calls it this way; psycopg connections are not thread-safe).

    `preclaimed=True` means the caller already took the part out of this
    device's reservation queue (take_reserved), so the part is already running
    under this device - claiming again would fail against our own claim and
    skip every queued part.
    """
    row = db.q("select * from old_parts where state_cd=%s and ac_no=%s and part_no=%s",
               (state_cd, ac_no, part_no), fetch="one")
    if row and row["status"] == "done" and not force:
        return {"skipped": True, "reason": "already done",
                "records": row["records"], "epics": row["epics"]}

    if not preclaimed and not claim_part(state_cd, ac_no, part_no):
        # Another PC process, phone or Colab holds this part right now (or holds
        # a reservation on it): visible skip instead of a silent duplicate sweep.
        return {"skipped": True, "reason": hold_reason(state_cd, ac_no, part_no)}

    try:
        return _collect_part_impl(conn, job_id, state_cd, ac_no, part_no,
                                  width=width)
    except BaseException as exc:  # noqa: BLE001 - hand the part back, then raise
        # A part must never stay `running` after a failure: the picker only
        # looks at pending/error, so a stranded part is invisible to every
        # device until restart. Hand it back with the reason attached.
        try:
            db.q("""update old_parts set status='pending', last_error=%s,
                        claimed_by=null, updated_at=now()
                    where state_cd=%s and ac_no=%s and part_no=%s
                      and status='running'""",
                 ("%s: %s" % (type(exc).__name__, exc), state_cd, ac_no, part_no),
                 fetch=None)
        except Exception:
            pass
        raise


def _collect_part_impl(conn, job_id, state_cd, ac_no, part_no, width=1):
    t0 = time.time()
    workers = max(1, int(db.setting("workers", ("workers", 6), tag=DEVICE_TAG))
                  // max(1, width))
    cap = int(db.setting("collect_serial_cap", ("collect_serial_cap", 3000),
                         tag=DEVICE_TAG))
    # Seed the roll-end probe from finished neighbours of the same AC: their
    # roll lengths are similar, so one DB read usually replaces ~10 of the ~13
    # sequential probe requests. probe_roll_end falls back to the full probe
    # when the hint misses, so the answer matches an unhinted probe.
    hint = 0
    nb = db.q("""select max(roll_end) as r from old_parts
                 where state_cd=%s and ac_no=%s and status='done'
                   and roll_end is not null and abs(part_no - %s) <= 3""",
              (state_cd, ac_no, part_no), fetch="one")
    if nb and nb["r"]:
        hint = int(nb["r"])
    roll_end = client.probe_roll_end(state_cd, ac_no, part_no, hard_cap=cap,
                                     hint=hint)
    # Publish roll_end before sweeping: it is the denominator the dashboard uses
    # for live speed and ETA, and `last_serial` is only written every 200 serials,
    # so without this the live card is blank for the first seconds of a part.
    db.q("""update old_parts set roll_end=%s, updated_at=now()
            where state_cd=%s and ac_no=%s and part_no=%s""",
         (roll_end, state_cd, ac_no, part_no), fetch=None)
    stats = {"hits": 0, "misses": 0, "errors": 0, "records": 0, "epics": 0}
    seen_ids = set()

    def one(serial):
        st, payload = client.fetch_serial(state_cd, ac_no, part_no, serial)
        return st, payload

    pending_rows = []
    # Old-location descriptor the route echoes on every record; constant per part,
    # so it is stored once on old_parts rather than repeated on every elector row.
    meta = {}

    def flush():
        if not pending_rows:
            return
        if conn is None:      # pipeline mode: borrow from the shared pool
            with db.pool().connection() as c, c.cursor() as cur:
                cur.executemany(ELECTOR_UPSERT, pending_rows)
        else:
            with conn.cursor() as cur:
                cur.executemany(ELECTOR_UPSERT, pending_rows)
        pending_rows.clear()

    cancelled = False
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, (st, payload) in enumerate(
                pool.map(one, range(1, roll_end + 1)), start=1):
            if st == 200 and payload:
                stats["hits"] += 1
                for rec in payload:
                    if rec.get("id") in seen_ids:
                        continue
                    seen_ids.add(rec["id"])
                    stats["records"] += 1
                    if rec.get("bloMappedEpicNo"):
                        stats["epics"] += 1
                    if not meta:
                        meta.update(
                            old_state_name=rec.get("oldStateName"),
                            old_dist_no=str(rec.get("oldDistNo") or "") or None,
                            old_dist_name=rec.get("oldDistName"),
                            old_ac_name=rec.get("oldAcName"))
                    pending_rows.append(_row_tuple(rec, state_cd, ac_no, part_no))
            elif st == 404:
                stats["misses"] += 1
            else:
                stats["errors"] += 1
            if i % 200 == 0:
                flush()
                # A part can outlast STALE_AFTER; sweeping counts as progress.
                STATE["tick"] = time.time()
                db.q("""update old_parts set last_serial=%s, records=%s, epics=%s,
                        old_state_name=coalesce(%s, old_state_name),
                        old_dist_no=coalesce(%s, old_dist_no),
                        old_dist_name=coalesce(%s, old_dist_name),
                        old_ac_name=coalesce(%s, old_ac_name),
                        exists_=true, updated_at=now()
                        where state_cd=%s and ac_no=%s and part_no=%s""",
                     (i, stats["records"], stats["epics"], meta.get("old_state_name"),
                      meta.get("old_dist_no"), meta.get("old_dist_name"),
                      meta.get("old_ac_name"), state_cd, ac_no, part_no),
                     fetch=None)
                progress(conn, job_id, {"phase": "collect", "serial": i,
                                        "roll_end": roll_end, "records": stats["records"],
                                        "epics": stats["epics"]})
                if job_cancelled(conn, job_id):
                    cancelled = True
                    break
        flush()

    # ---- calibration: how far does the mapping's numbering lag the live roll?
    offset = cur_part_mode = None
    if db.setting("calibrate_offset", True):  # global-only knob (device-agnostic)
        sample = db.q("""select cur_epic, cur_part_no from electors
                         where state_cd=%s and ac_no=%s and part_no=%s
                           and cur_epic is not null and cur_epic <> ''
                         order by random() limit 3""",
                      (state_cd, ac_no, part_no))
        deltas, parts = [], []
        for s in sample or []:
            res = client.epic_lookup(s["cur_epic"])
            content = res.get("content") or {}
            live_part = content.get("partNumber")
            if live_part is not None and s["cur_part_no"] is not None:
                deltas.append(int(live_part) - int(s["cur_part_no"]))
            if s["cur_part_no"] is not None:
                parts.append(int(s["cur_part_no"]))
        if deltas:
            offset = int(statistics.median(deltas))
        if parts:
            cur_part_mode = Counter(parts).most_common(1)[0][0]

    status = "pending" if cancelled else "done"
    last_error = None
    if status == "done" and stats["records"] == 0 and stats["errors"] > 0:
        # WAF challenges and rate-limit storms answer 200 with no parsable rows,
        # so a sweep can "succeed" with 0 records and the part would be lost as
        # done forever (the live DB had 63 such parts, all with the degraded
        # roll_end=30 fingerprint). Fetch errors are evidence of transport
        # trouble, so hand the part back to the retry path; a genuinely empty
        # part misses cleanly (errors == 0) and stays done in one pass.
        status = "error"
        last_error = "suspicious: 0 records with %d fetch errors" % stats["errors"]
    db.q("""update old_parts set status=%s, finished_at=now(), claimed_by=null,
            reserved_by=null, reserved_at=null,
            records=%s, epics=%s, last_error=%s,
            unmapped=%s, roll_end=%s, mapping_offset=%s, cur_part_mode=%s,
            old_state_name=coalesce(%s, old_state_name),
            old_dist_no=coalesce(%s, old_dist_no),
            old_dist_name=coalesce(%s, old_dist_name),
            old_ac_name=coalesce(%s, old_ac_name), updated_at=now()
            where state_cd=%s and ac_no=%s and part_no=%s""",
         (status, stats["records"], stats["epics"], last_error,
          stats["records"] - stats["epics"], roll_end, offset, cur_part_mode,
          meta.get("old_state_name"), meta.get("old_dist_no"),
          meta.get("old_dist_name"), meta.get("old_ac_name"),
          state_cd, ac_no, part_no), fetch=None)
    result = dict(stats, roll_end=roll_end, offset=offset,
                  cur_part_mode=cur_part_mode, seconds=round(time.time() - t0, 1),
                  cancelled=cancelled)
    if not cancelled:
        db.event("collect", "%s AC %s part %s: %d records, %d EPICs (%ss)"
                 % (state_cd, ac_no, part_no, stats["records"], stats["epics"],
                    result["seconds"]))
    return result


def _pipeline_one(pick, force):
    """Body of one parallel part-worker: every statement goes through the shared
    pool (the loop's conn belongs to the caller's thread; psycopg connections
    are not thread-safe). Failures surface via the future - collect_part hands
    the part back to the queue either way."""
    return collect_part(None, None, pick["state_cd"], pick["ac_no"],
                        pick["part_no"], force=force,
                        preclaimed=bool(pick.get("preclaimed")))


def collect_pipeline(conn, job_id, state_cd=None, ac_no=None, width=None,
                     max_parts=None, force=False):
    """Collect parts concurrently, replenishing each slot as it frees.

    The win is phase overlap: while one part sits in its roll-end probe head,
    its calibration tail or its final DB flush, another is already sweeping.
    Each part still runs `workers // width` serial threads, so the gateway sees
    the same total concurrency as before, just never idle.

    A freed slot is refilled from THIS device's reservation queue first (one
    indexed point update, see reserve_parts/take_reserved) and only falls back
    to the shared picker when the queue is short. So the ~5s full-backlog
    ranking runs once per (n_res - low) parts instead of once per part, the
    per-part decision is made ahead of time, and two devices no longer rank the
    same part. Claims stay atomic, so overlapping with other devices is safe
    either way: losers get a visible skip.
    """
    if width is None:
        width = max(1, int(db.setting("parts_parallel", ("parts_parallel", 2),
                                      tag=DEVICE_TAG)))
    # Force mode walks already-done parts, which a reservation can never cover.
    n_res = 0 if force else reserve_n()
    # Low-water mark: keep at least one spare per slot, so a refill lagging
    # behind the sweeps can never leave a slot without a queued part. Both ends
    # shrink for a small queue (n_res=1 works out to 'top up whenever empty').
    low = max(0, n_res - max(1, min(width, n_res)) - 1) if n_res else 0

    done = []
    yielded = False

    def record(pick, res):
        done.append({"state_cd": pick["state_cd"], "ac_no": pick["ac_no"],
                     "part_no": pick["part_no"], "result": res,
                     "cur_part_mode": (res or {}).get("cur_part_mode")})
        progress(conn, job_id, {"phase": "auto", "finished": len(done),
                                "last": done[-1]})

    def top_up():
        """Refill this device's queue when it runs low. One candidate query
        plus one batch stamp; a stamp lost to another device is normal and just
        means the next turn tops up again."""
        if not n_res:
            return 0
        held = reserve_count()
        STATE["reserved"] = held
        if held > low:
            return held
        got = reserve_parts(conn, state_cd, ac_no, n=n_res)
        STATE["reserved"] = held + len(got or [])
        return STATE["reserved"]

    def take(room):
        """Up to `room` parts out of this device's own queue, oldest first."""
        if not (n_res and room > 0):
            return []
        picks = take_reserved(room)
        if picks:
            STATE["reserved"] = max(0, STATE.get("reserved", 0) - len(picks))
        return picks

    def give_back():
        """Hand back unused holds when this device stops working on parts -
        otherwise a phone yielding to a job would hide five parts from the
        fleet for the whole TTL."""
        if not n_res:
            return 0
        n = release_reservations()
        STATE["reserved"] = 0
        if n:
            db.event("worker", "released %d unused part reservation(s)" % n)
        return n

    if width <= 1:
        while max_parts is None or len(done) < max_parts:
            if job_id is not None and job_cancelled(conn, job_id):
                break
            top_up()
            held = take(1)
            if held:
                pick = {"state_cd": held[0]["state_cd"],
                        "ac_no": held[0]["ac_no"],
                        "part_no": held[0]["part_no"], "preclaimed": True}
            else:
                nxt = best_pending_part(conn, state_cd, ac_no)
                if not nxt:
                    break
                pick = {"state_cd": nxt["state_cd"], "ac_no": nxt["ac_no"],
                        "part_no": nxt["part_no"]}
            record(pick, collect_part(conn, job_id, pick["state_cd"],
                                      pick["ac_no"], pick["part_no"],
                                      force=force,
                                      preclaimed=bool(pick.get("preclaimed"))))
        if not done:
            give_back()
        return {"parts": len(done), "done": done}

    attempted = set()
    inflight = {}   # future -> pick
    with ThreadPoolExecutor(max_workers=width) as ex:
        while not STOP.is_set():
            if job_id is not None and job_cancelled(conn, job_id):
                break
            if max_parts is not None and len(done) >= max_parts:
                break
            # An open-ended auto pipeline (job_id=None) must yield promptly when
            # a queued job appears - otherwise the endless sweep starves the
            # jobs queue (observed: discover buttons sat unclaimed for half an
            # hour while auto collected). run_forever claims it on the next pass.
            if job_id is None:
                queued = db.q("select 1 from jobs where status='queued' "
                              "and (device is null or device = %s) limit 1",
                              (DEVICE_TAG,), fetch="one")
                if queued:
                    yielded = True
                    break
            free = width - len(inflight)
            if free > 0 and (max_parts is None
                             or len(done) + len(inflight) < max_parts):
                room = free if max_parts is None else min(
                    free, max_parts - len(done) - len(inflight))
                # 1) This device's own queue first: one indexed point update,
                #    oldest hold first, and the parts come back already claimed.
                for nxt in take(room):
                    pick = {"state_cd": nxt["state_cd"], "ac_no": nxt["ac_no"],
                            "part_no": nxt["part_no"], "preclaimed": True}
                    key = (pick["state_cd"], pick["ac_no"], pick["part_no"])
                    if key in attempted:   # unreachable: taking clears the hold
                        continue
                    attempted.add(key)
                    inflight[ex.submit(_pipeline_one, pick, force)] = pick
                    room -= 1
                # 2) Queue could not fill every slot (cold start, a device that
                #    just lost stamps, or n_res=0): fall back to the shared
                #    picker, which skips live holds so a lost pick is rare
                #    rather than the norm. The claim still decides.
                if room > 0:
                    picks = best_pending_parts(conn, state_cd, ac_no,
                                               limit=room + width)
                    for nxt in picks:
                        if room <= 0:
                            break
                        if max_parts is not None and \
                                len(done) + len(inflight) >= max_parts:
                            break
                        pick = {"state_cd": nxt["state_cd"],
                                "ac_no": nxt["ac_no"],
                                "part_no": nxt["part_no"]}
                        key = (pick["state_cd"], pick["ac_no"], pick["part_no"])
                        if key in attempted:
                            continue
                        attempted.add(key)
                        inflight[ex.submit(_pipeline_one, pick, force)] = pick
                        room -= 1
            # Refill AFTER the slots are dispatched, so the picker's ranking
            # overlaps the sweeps instead of delaying them.
            top_up()
            if not inflight:
                break   # nothing pending left (or everything is held elsewhere)
            finished, _ = wait(list(inflight), timeout=5,
                               return_when=FIRST_COMPLETED)
            for fut in finished:
                pick = inflight.pop(fut)
                try:
                    res = fut.result()
                except Exception as exc:  # noqa: BLE001 - recorded, loop goes on
                    res = {"error": "%s: %s" % (type(exc).__name__, exc)}
                record(pick, res)
    if yielded or not done:
        give_back()
    return {"parts": len(done), "done": done}


def collect_auto(conn, job_id, state_cd=None, ac_no=None, max_parts=50, force=False):
    return collect_pipeline(conn, job_id, state_cd, ac_no,
                            max_parts=max_parts or None, force=force)


def epic_lookup_job(conn, job_id, epic):
    res = client.epic_lookup(epic)
    content = res.get("content") or {}
    prof = client.profile_from_content(content)
    db.q("""insert into epic_lookups(epic, found, http_status, hits, name, name_local,
              relation, relation_local, relation_type, age, gender, state_cd, state_name,
              district, ac_no, ac_name, part_no, part_name, part_name_l1, part_id,
              serial_no, section_no, ps_building, ps_building_l1, record_id, raw)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)
            on conflict (epic) do update set
              found=excluded.found, http_status=excluded.http_status, hits=excluded.hits,
              name=excluded.name, name_local=excluded.name_local,
              relation=excluded.relation, relation_local=excluded.relation_local,
              relation_type=excluded.relation_type, age=excluded.age,
              gender=excluded.gender, state_cd=excluded.state_cd,
              state_name=excluded.state_name, district=excluded.district,
              ac_no=excluded.ac_no, ac_name=excluded.ac_name, part_no=excluded.part_no,
              part_name=excluded.part_name, part_name_l1=excluded.part_name_l1,
              part_id=excluded.part_id, serial_no=excluded.serial_no,
              section_no=excluded.section_no, ps_building=excluded.ps_building,
              ps_building_l1=excluded.ps_building_l1, record_id=excluded.record_id,
              raw=excluded.raw, fetched_at=now()""",
         (res.get("epic"), bool(res.get("hits")), res.get("status"), res.get("hits"),
          prof["name"], prof["name_local"], prof["relation"], prof["relation_local"],
          prof["relation_type"], _int(prof["age"]), prof["gender"], prof["state_cd"],
          prof["state_name"], prof["district"], _int(prof["ac_no"]), prof["ac_name"],
          _int(prof["part_no"]), prof["part_name"], prof["part_name_l1"],
          _int(prof["part_id"]), _int(prof["serial_no"]), _int(prof["section_no"]),
          prof["ps_building"], prof["ps_building_l1"], prof["record_id"],
          json.dumps(content)), fetch=None)
    db.event("epic", "lookup %s: %s" % (res.get("epic"),
             "found" if res.get("hits") else "no record (%s)" % res.get("status")))
    return {"epic": res.get("epic"), "found": bool(res.get("hits")),
            "status": res.get("status"), "profile": prof}


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ------------------------------------------------------------------ job runner

def run_job(conn, job):
    kind = job["kind"]
    payload = job["payload"] or {}
    STATE["job_id"] = job["id"]
    STATE["note"] = "%s %s" % (kind, json.dumps(payload)[:80])
    db.event("worker", "job #%s %s %s" % (job["id"], kind, json.dumps(payload)[:120]))
    try:
        if kind == "seed_states":
            out = seed_states(conn)
        elif kind == "seed_acs":
            out = seed_acs(conn, payload["state_cd"])
        elif kind == "seed_acs_all":
            out = seed_acs_all(conn)
        elif kind == "discover_parts":
            out = discover_parts(conn, job["id"], payload["state_cd"], int(payload["ac_no"]),
                                 payload.get("max_part"))
        elif kind == "collect_part":
            out = collect_part(conn, job["id"], payload["state_cd"], int(payload["ac_no"]),
                               int(payload["part_no"]), bool(payload.get("force")))
        elif kind == "collect_auto":
            out = collect_auto(conn, job["id"], payload.get("state_cd"),
                               payload.get("ac_no"), int(payload.get("max_parts", 25)),
                               bool(payload.get("force")))
        elif kind == "epic_lookup":
            out = epic_lookup_job(conn, job["id"], payload["epic"])
        else:
            raise ValueError("unknown job kind %r" % kind)
        finish(conn, job["id"], "done", out)
        return out
    except Exception as exc:  # noqa: BLE001 - recorded, not raised
        finish(conn, job["id"], "error", None, "%s: %s" % (type(exc).__name__, exc))
        db.event("worker", "job #%s failed: %s" % (job["id"], exc), level="error")
        return {"error": "%s: %s" % (type(exc).__name__, exc)}
    finally:
        STATE["job_id"] = None


def run_forever(poll=2.0):
    """Job loop + auto mode. Never lets an exception escape: a worker that dies
    silently leaves queued jobs stranded with no way to notice."""
    STATE.update(running=True, started=time.time(), tick=time.time(), error=None)
    db.event("worker", "worker started")
    try:
        recover_orphans(None)
    except Exception as exc:  # noqa: BLE001
        db.event("worker", "orphan recovery failed: %s" % exc, level="error")
    conn = None
    last_reap = 0.0
    while not STOP.is_set():
        STATE["tick"] = time.time()
        # Clear the previous turn's failure: the error below is only set when a
        # turn raises, but a recovered worker kept advertising a stale timeout
        # (observed: 'QueryCanceled' shown while parts were being claimed).
        STATE["error"] = None
        try:
            if conn is None:
                conn = db.connect()
            # Live reaper: hand back parts whose holder died (crash, killed
            # window, dead emulator). Stale-only, so it never touches a part a
            # live device is sweeping; runs here every ~60s so any surviving
            # device picks up a dead device's work without waiting for restart.
            if time.time() - last_reap >= 60.0:
                last_reap = time.time()
                try:
                    recover_orphans(None)
                except Exception as exc:  # noqa: BLE001
                    db.event("worker", "reap failed: %s" % exc, level="warn")
            job = claim_job(conn)
            if job:
                run_job(conn, job)
                continue
            if db.setting("auto_enabled", False):   # per-device knob
                nxt = best_pending_part(conn)
                if nxt:
                    STATE["note"] = "auto pipeline x%s: %s AC %s part %s" % (
                        db.setting("parts_parallel", 2), nxt["state_cd"],
                        nxt["ac_no"], nxt["part_no"])
                    out = collect_pipeline(conn, None)
                    if out["parts"]:
                        continue
                    # Every pick is held by another device right now.
                    STATE["note"] = "auto: parts in flight elsewhere"
                else:
                    # Nothing left to pick: don't keep advertising the last part.
                    STATE["note"] = "idle - nothing pending"
            elif STATE.get("reserved"):
                # Auto is off and this device holds parts it is not going to
                # collect: hand them back now instead of hiding them from the
                # fleet until the reservation TTL. Guarded by the cached count,
                # so an idle loop does not run a query every poll.
                n = release_reservations()
                STATE["reserved"] = 0
                if n:
                    db.event("worker", "released %d unused part reservation(s) "
                             "(auto off)" % n)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            STATE["error"] = "%s: %s" % (type(exc).__name__, exc)
            db.event("worker", "loop error: %s" % exc, level="error")
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(poll)
        time.sleep(poll)
    STATE["running"] = False
    # A device that stops must not keep its queue hidden from the fleet; the
    # expiry predicate covers a kill, this covers a clean stop.
    try:
        n = release_reservations()
        if n:
            db.event("worker", "released %d part reservation(s) on stop" % n)
    except Exception:  # noqa: BLE001 - stopping anyway
        pass
    STATE["reserved"] = 0
    db.event("worker", "worker stopped")


def start_background():
    t = threading.Thread(target=run_forever, name="old-eci-worker", daemon=True)
    STATE["thread"] = t
    t.start()
    return t


def ensure_worker():
    """Start the worker thread if it is missing or has gone stale.

    `worker_status()['alive']` is the honest answer to "is work progressing" -
    a hung connection makes `running` true while nothing happens.
    """
    st = worker_status()
    if st["alive"]:
        return {"restarted": False, "worker": st}
    STOP.clear()
    if st["running"] and st["tick_age"] is not None and st["tick_age"] >= STALE_AFTER:
        db.event("worker", "worker looked stalled (no loop turn for %ss) - starting a "
                 "replacement; the stuck thread will exit on its own" % st["tick_age"],
                 level="warn")
    start_background()
    return {"restarted": True, "worker": worker_status()}


def run_once(kind, **payload):
    conn = db.connect()
    return run_job(conn, {"id": None, "kind": kind, "payload": payload})


if __name__ == "__main__":
    import sys
    db.init()
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        print(run_once(sys.argv[2], **json.loads(sys.argv[3] if len(sys.argv) > 3 else "{}")))
    else:
        start_background()
        print("worker running; ctrl-c to stop")
        while True:
            time.sleep(5)
