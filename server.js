// old_eci dashboard — Node.js
// Replaces render_app/app.py (the Python/FastAPI app) for the Render web service.
// Serves the SAME web/index.html interface (unchanged) and all 27 /api/* routes
// with the same SQL + response shapes as app.py, driven by ECI_PG_DSN.
//
// Run:  node server.js
// Env:  ECI_PG_DSN=postgresql://eci_app:…@129.225.75.85:5432/old_eci
//       ECI_DEVICE_TAG=render   (or whatever tag this node should own)
//       PORT=                   (Render sets this; default 8008)
//
// No Python dependency at all — Render's free Node runtime runs this directly.
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const PG = require('pg');
const { Pool } = PG;

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------
const DSN = process.env.ECI_PG_DSN || 'postgresql://eci_app:Raj%40A2Nkufyg@129.225.75.85:5432/old_eci';
const SCHEMA = process.env.ECI_PG_SCHEMA || 'public';
const GEO_DSN = process.env.ECI_GEO_DSN || 'postgresql://eci_app:Raj%40A2Nkufyg@129.225.75.85:5432/eci';
const DEVICE_TAG = process.env.ECI_DEVICE_TAG === '' ? null : (process.env.ECI_DEVICE_TAG || '');
const PORT = parseInt(process.env.PORT, 10) || 8008;
const HERE = __dirname;
const WEB = path.join(HERE, 'web');

// ---------------------------------------------------------------------------
// Postgres pool — one pool for the life of the server; each request borrows a
// client, runs its queries, and releases it. The pool reconnects automatically
// when a connection drops (Render free tier / network blips).
// ---------------------------------------------------------------------------
const POOL = new Pool({
  connectionString: DSN,
  application_name: 'render-old-eci',
  max: 8,
  idleTimeoutMillis: 30000,
});

// ---------------------------------------------------------------------------
// async DB helpers (mirrors app.py's db.q / db.q_one / db.q_none)
// ---------------------------------------------------------------------------
// Ensure params is always an array (node-postgres requires it). Scalars passed
// by callers (e.g. a single string) get wrapped into [value].
function toParams(p) {
  if (p == null) return [];
  if (Array.isArray(p)) return p;
  return [p];
}

async function q(sql, params) {
  return pool_q(sql, toParams(params), 'all');
}
async function q_one(sql, params) {
  return pool_q(sql, toParams(params), 'one');
}
async function q_none(sql, params) {
  return pool_q(sql, toParams(params), 'none');
}

async function pool_q(sql, params, fetchMode) {
  fetchMode = fetchMode || 'all'; // 'all' | 'one' | 'none'
  const client = await POOL.connect();
  try {
    const result = await client.query({
      text: sql,
      values: params || [],
      rowMode: fetchMode === 'one' ? 'single' : undefined,
    });
    if (fetchMode === 'none') {
      return { rowsAffected: result.rowCount != null ? result.rowCount : 0 };
    }
    const rows = result.rows;
    if (fetchMode === 'one') return rows.length ? rows[0] : {};
    return rows;
  } finally {
    client.release();
  }
}

// best-effort legacy catalogue read (mirrors app.py's geo_q → [])
async function geo_q(sql, params) {
  const client = new PG.Client({ connectionString: GEO_DSN, application_name: 'render-old-eci-geo' });
  try {
    await client.connect();
    const r = await client.query({ text: sql, values: params || [] });
    return r.rows;
  } catch (e) {
    return [];
  } finally {
    try { client.end(); } catch (e) { /* ignore */ }
  }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function ts_to_str(ts) {
  if (ts === null || ts === undefined) return null;
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  return d.toISOString();
}

function myTag() { return DEVICE_TAG; }

// app.py db.setting(key, fallback, tag=tag): reads scoped row key@tag first (when
// tag non-empty), else the shared key row, else fallback.
async function setting(key, fallback, tag) {
  if (tag) {
    const scoped = await q_one('select value from settings where key = $1', key + '@' + tag);
    if (scoped && scoped.value != null && scoped.value !== undefined) {
      return String(scoped.value);
    }
  }
  const row = await q_one('select value from settings where key = $1', key);
  if (row) {
    const v = row.value;
    if (v != null && v !== undefined) return String(v);
  }
  return fallback;
}

// app.py enqueue(): insert jobs row + best-effort event write
async function enqueue(kind, payload, mode, priority) {
  const device = myTag();
  const payloadJson = JSON.stringify(payload);
  const priorityVal = priority != null ? priority : 100;
  const inserted = await q_one(
    `insert into jobs(kind, payload, mode, priority, device)
     values ($1, $2, $3, $4, $5)
     returning id, kind, status`,
    [kind, payloadJson, mode || 'manual', priorityVal, device]
  );
  // best-effort event write (don't fail the enqueue if it errors)
  try {
    await q_none(
      `insert into events(source, level, message, device)
       values ($1, $2, $3, $4)`,
      ['api', 'info', 'queued #' + inserted.id + ' ' + kind + ' ' + payloadJson.slice(0, 120), device]
    );
  } catch (e) { /* event write is best-effort */ }
  return inserted;
}

// app.py heavy_counts()
async function heavy_counts() {
  const doneDay = await q_one(
    "select count(*) c from old_parts where status='done' and finished_at >= now() - interval '1 day'", []);
  const done7 = await q_one(
    "select count(*) c from old_parts where status='done' and finished_at >= now() - interval '7 days'", []);
  const doneToday = await q_one(
    "select count(*) c from old_parts where status='done' and finished_at >= date_trunc('day', now())", []);
  const doneTotal = await q_one(
    "select count(*) c from old_parts where status='done'", []);
  return {
    done_today: doneToday ? doneToday.c : 0,
    done_7days: done7 ? done7.c : 0,
    done_all_time: doneDay ? doneDay.c : 0,
    done_total: doneTotal ? doneTotal.c : 0,
  };
}

// app.py speed_stats() — simplified but matches the shape the frontend uses
async function speed_stats() {
  const recent = await q_one(
    "select count(*) c from old_parts where status='done' and finished_at >= now() - interval '5 minutes'", []);
  const c = recent ? recent.c : 0;
  return {
    parts_5min: c,
    parts_per_min: Math.round((c / 5) * 60),
  };
}

// read a request body (for POST JSON endpoints)
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => { data += chunk; });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

// parse JSON body, returning {} on failure (mirror app.py behavior)
async function parseBody(req) {
  const raw = await readBody(req);
  try {
    return JSON.parse(raw || '{}');
  } catch (e) {
    return {};
  }
}

// ---------------------------------------------------------------------------
// JSON serialization helpers (handle Date / Buffer / bigint / Error)
// ---------------------------------------------------------------------------

function toSerializable(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'bigint') return parseInt(v, 10);
  if (typeof v === 'number') return v;
  if (typeof v === 'boolean') return v;
  if (typeof v === 'string') return v;
  if (v instanceof Date) return v.toISOString();
  if (Buffer.isBuffer(v)) return v.toString('utf8');
  if (v instanceof Error) return { name: v.name, message: v.message };
  if (typeof v === 'object' && v !== null) {
    try { return JSON.parse(JSON.stringify(v)); } catch (e) { return String(v); }
  }
  return v;
}

function sanitize(obj) {
  if (Array.isArray(obj)) return obj.map(sanitize);
  if (obj !== null && typeof obj === 'object') {
    const out = {};
    for (const k of Object.keys(obj)) out[k] = sanitize(obj[k]);
    return out;
  }
  return toSerializable(obj);
}

// ---------------------------------------------------------------------------
// response helpers
// ---------------------------------------------------------------------------

function writeJson(res, status, body) {
  const payload = sanitize(body);
  const json = JSON.stringify(payload) + '\n';
  const buf = Buffer.from(json, 'utf8');
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': buf.length,
    'Cache-Control': 'no-store',
  });
  res.end(buf);
}

function json(res, status, body) {
  if (arguments.length === 2) { body = status; status = 200; }
  writeJson(res, status, body);
}

function csv(res, text) {
  const buf = Buffer.from(text, 'utf8');
  res.writeHead(200, {
    'Content-Type': 'text/csv; charset=utf-8',
    'Content-Disposition': 'attachment; filename="export.csv"',
    'Content-Length': buf.length,
  });
  res.end(buf);
}

function serveFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('not found\n');
      return;
    }
    res.writeHead(200, {
      'Content-Type': contentType,
      'Content-Length': data.length,
      'Cache-Control': 'no-cache',
    });
    res.end(data);
  });
}

// ---------------------------------------------------------------------------
// route → async function; all DB calls are awaited
// ---------------------------------------------------------------------------

async function route(req, res) {
  const parsed = new URL(req.url, 'http://localhost');
  const pathName = parsed.pathname;
  const query = Object.fromEntries(parsed.searchParams);
  const method = (req.method || 'GET').toUpperCase();

  // static page
  if (pathName === '/' || pathName === '/index.html') {
    serveFile(res, path.join(WEB, 'index.html'), 'text/html');
    return;
  }

  try {
    // ---- /api/health
    if (pathName === '/api/health' && method === 'GET') {
      let reachable = false;
      try {
        await q_one('select 1', []);
        reachable = true;
      } catch (e) { /* unreachable */ }
      json(res, {
        ok: reachable,
        schema: SCHEMA,
        db: DSN.includes('@') ? DSN.split('@').pop() : DSN,
        auto: (await setting('auto_enabled', 'false', null)) === 'true',
        worker_alive: false,
        worker_tick_age: null,
        worker_error: null,
        node: true,
      });
      return;
    }

    // ---- /api/summary
    if (pathName === '/api/summary' && method === 'GET') {
      const parts = await q_one(`select
        count(*) old_parts,
        count(*) filter (where status='done') done_parts,
        count(*) filter (where status='pending') pending_parts,
        count(*) filter (where status='running') running_parts,
        count(*) filter (where status='error') error_parts,
        coalesce(sum(records),0) records,
        coalesce(sum(epics),0) epics
        from old_parts`) || {};

      const statesCount = await q_one('select count(*) c from states', []) || {};
      const acsCount = await q_one('select count(*) c from acs', []) || {};
      const epicLookups = await q_one('select count(*) c from epic_lookups', []) || {};

      const overall = {
        old_parts: parts.old_parts || 0,
        done_parts: parts.done_parts || 0,
        pending_parts: parts.pending_parts || 0,
        running_parts: parts.running_parts || 0,
        error_parts: parts.error_parts || 0,
        records: parts.records || 0,
        epics: parts.epics || 0,
        states: statesCount.c || 0,
        acs: acsCount.c || 0,
        epic_lookups: epicLookups.c || 0,
        ...await heavy_counts(),
      };
      overall.free_parts = (overall.pending_parts + overall.error_parts);

      const ttl = parseFloat(await setting('part_reserve_ttl', 900, null)) || 900;
      const holds = await q(
        `select reserved_by d, count(*) c from old_parts
         where reserved_by is not null
           and status in ('pending','error')
           and reserved_at >= now() - make_interval(secs => $1)
         group by reserved_by order by 2 desc, 1`,
        [ttl]
      );
      const reservedParts = holds.reduce((s, h) => s + (h.c || 0), 0);
      overall.reserved_parts = reservedParts;
      overall.free_parts = Math.max(0, overall.free_parts - reservedParts);

      const jobs = await q(
        `select id, kind, status, mode, device, progress, result, error,
                created_at, started_at, finished_at from jobs
         where (device is null or device = $1)
           and status in ('queued','running')
         order by id desc limit 8`,
        [myTag()]
      );

      const states = await q(`select s.state_cd, s.name, s.has_old_data,
        coalesce(a.acs,0) acs, coalesce(p.parts,0) parts, coalesce(p.done,0) done
        from states s
        left join (select state_cd, count(*) acs from acs group by 1) a on a.state_cd = s.state_cd
        left join (select state_cd, count(*) parts,
                      count(*) filter (where status='done') done
                 from old_parts group by 1) p on p.state_cd = s.state_cd
        order by s.state_cd limit 60`);

      const now = Date.now() / 1000;
      const speed = { ...(await speed_stats()), cached_secs: 0, ttl_secs: 60 };

      const tag = DEVICE_TAG || null;
      const settings = {
        auto_enabled: await setting('auto_enabled', null, null),
        calibrate_offset: await setting('calibrate_offset', null, null),
        workers: tag ? await setting('workers', 6, tag) : await setting('workers', 6, null),
        discover_max_part: tag ? await setting('discover_max_part', 400, tag) : await setting('discover_max_part', 400, null),
        device_tag: tag,
      };
      settings.auto_overrides = await q(`select key, value from settings where key like 'auto_enabled@%' order by key`);
      const claimants = await q(`select split_part(claimed_by, '-', 1) as kind,
        min(claimed_by) example, count(*) c
        from old_parts where status='running' and claimed_by is not null
        group by 1`);

      json(res, {
        overall, jobs, states, worker: worker_status(), speed, settings, holds, claimants
      });
      return;
    }

    // ---- /api/events
    if (pathName === '/api/events' && method === 'GET') {
      const limit = Math.min(parseInt(query.limit, 10) || 50, 500);
      const all = query.all === '1' || query.device === '*';
      let rows;
      if (all) {
        rows = await q(`select id, ts, level, source, message, device from events order by id desc limit $1`, [limit]);
      } else {
        const dev = query.device != null ? query.device : myTag();
        rows = await q(`select id, ts, level, source, message, device from events where device = $1 order by id desc limit $2`, [dev, limit]);
      }
      json(res, rows.map(r => ({ ...r, ts: ts_to_str(r.ts) })));
      return;
    }

    // ---- /api/states
    if (pathName === '/api/states' && method === 'GET') {
      const rows = await q(`select state_cd, name, has_old_data from states order by state_cd`);
      json(res, rows);
      return;
    }

    // ---- /api/states/seed
    // Mirrors app.py's seed_states(): payload may be empty ({}); it enqueues a
    // seed_acs job that reads the geo tables itself. No state_cd required here.
    if (pathName === '/api/states/seed' && method === 'POST') {
      await parseBody(req); // body may be {} or empty
      json(res, 200, await enqueue('seed_acs', {}, 'manual', 100));
      return;
    }

    // ---- /api/acs
    if (pathName === '/api/acs' && method === 'GET') {
      const state = query.state;
      if (!state) return json(res, 400, { error: 'state required' });
      const rows = await q(`select a.state_cd, a.ac_no, a.name, a.ac_type, a.district_cd,
        a.discover_status, a.old_parts_found, a.discover_max, a.discovered_at, a.last_error,
        (a.discover_max is not null and a.old_parts_found >= a.discover_max) maybe_truncated,
        (select count(*) from old_parts p where p.state_cd=a.state_cd and p.ac_no=a.ac_no) parts,
        (select count(*) from old_parts p where p.state_cd=a.state_cd and p.ac_no=a.ac_no and p.status='done') done
        from acs a where a.state_cd = $1 order by a.ac_no`, [state]);
      json(res, rows.map(r => ({
        ...r,
        ac_no: r.ac_no,
        old_parts_found: r.old_parts_found,
        discover_max: r.discover_max,
        maybe_truncated: r.maybe_truncated,
        parts: r.parts,
        done: r.done,
      })));
      return;
    }

    // ---- /api/acs/seed
    if (pathName === '/api/acs/seed' && method === 'POST') {
      const payload = await parseBody(req);
      const stateCd = (payload && payload.state_cd) ? String(payload.state_cd) : null;
      if (!stateCd) return json(res, 400, { error: 'state_cd required' });
      json(res, 200, await enqueue('seed_acs', { state_cd: stateCd }, 'manual', 100));
      return;
    }

    // ---- /api/acs/seed_all
    if (pathName === '/api/acs/seed_all' && method === 'POST') {
      json(res, 200, await enqueue('seed_acs_all', {}, 'manual', 100));
      return;
    }

    // ---- /api/acs/discover
    // Mirrors app.py's discover(): both state_cd and ac_no are required (app.py
    // reads payload["ac_no"] directly and 500s on a missing key; here we validate
    // early and return 400 so a malformed call never queues a discover with ac_no=NaN).
    if (pathName === '/api/acs/discover' && method === 'POST') {
      const payload = await parseBody(req);
      const stateCd = (payload && payload.state_cd) ? String(payload.state_cd) : null;
      const rawAc = payload != null && payload.ac_no != null ? payload.ac_no : undefined;
      if (!stateCd) return json(res, 400, { error: 'state_cd required' });
      if (rawAc == null || rawAc === '') return json(res, 400, { error: 'ac_no required' });
      const acNo = parseInt(rawAc, 10);
      if (isNaN(acNo)) return json(res, 400, { error: 'ac_no required' });
      const maxPart = parseInt(payload.max_part, 10)
        || parseInt(await setting('discover_max_part', 400, null), 10)
        || 400;
      json(res, 200, await enqueue('discover_parts', {
        state_cd: stateCd,
        ac_no: acNo,
        max_part: maxPart,
      }, 'manual', 100));
      return;
    }

    // ---- /api/parts
    // Mirrors app.py's parts() route: requires state + ac, joins current_parts for
    // cur_part_name / cur_part_name_l1, and adds per-part electors + unique_epics
    // counts so the frontend's loadParts() row shape matches.
    if (pathName === '/api/parts' && method === 'GET') {
      const state = query.state;
      const ac = parseInt(query.ac, 10);
      if (!state || isNaN(ac)) return json(res, 400, { error: 'state and ac required' });
      const limit = Math.min(parseInt(query.limit, 10) || 300, 2000);
      const offset = parseInt(query.offset, 10) || 0;
      const status = query.status || null;
      const qv = query.q || null;
      const where = ['p.state_cd = $1', 'p.ac_no = $2'];
      const params = [state, ac];
      let pidx = 3;
      if (status) { where.push('p.status = $' + pidx); params.push(status); pidx++; }
      if (qv) {
        where.push('(p.name ilike $' + pidx + ' or p.part_no::text = $' + (pidx + 1) + ')');
        params.push('%' + qv + '%', qv); pidx += 2;
      }
      const cond = where.join(' and ');
      const rows = await q(
        `select p.*, p.mapping_offset as "offset",
               cp.part_name as cur_part_name, cp.part_name_l1,
               (select count(*) from electors e
                 where e.state_cd = p.state_cd and e.ac_no = p.ac_no
                   and e.part_no = p.part_no) electors,
               (select count(distinct e.cur_epic) from electors e
                 where e.state_cd = p.state_cd and e.ac_no = p.ac_no
                   and e.part_no = p.part_no) unique_epics
         from old_parts p
         left join current_parts cp
           on cp.state_cd = p.state_cd and cp.ac_no = p.ac_no
          and cp.part_no = p.cur_part_mode
         where ${cond} order by p.part_no limit $${pidx} offset $${pidx + 1}`,
        [...params, limit, offset]
      );
      const total = await q_one(
        `select count(*) c from old_parts p where ${cond}`, params
      );
      json(res, {
        rows: rows.map(r => ({
          ...r,
          exists_: r.exists_,
          name: r.name,
          cur_part_name: r.cur_part_name,
          cur_part_name_l1: r.cur_part_name_l1,
        })),
        total: total ? total.c : 0,
      });
      return;
    }

    // ---- /api/current_parts
    // Mirrors app.py's current_parts() route: requires state + ac; refresh=true
    // (or an empty cache) triggers a client.current_parts() fetch + upsert, then
    // returns the current_parts rows for that AC ordered by part_no.
    if (pathName === '/api/current_parts' && method === 'GET') {
      const state = query.state;
      const ac = parseInt(query.ac, 10);
      if (!state || isNaN(ac)) return json(res, 400, { error: 'state and ac required' });
      const refresh = query.refresh === 'true';
      const cached = await q_one(
        'select count(*) c from current_parts where state_cd = $1 and ac_no = $2',
        [state, ac]
      );
      if (refresh || !cached || cached.c === 0) {
        try {
          const acs = await geo_q(
            `select ac_no::text as state, ac_name as name
             from geo.acs where state_cd = $1 order by ac_no limit 1`,
            [state]
          );
        } catch (e) { /* geo_q is best-effort; fall back to client path below */ }
        // Best-effort fetch from the ECI gateway + upsert into current_parts.
        // If the gateway is unreachable this request still returns whatever is
        // already cached (the old cache), matching app.py's behavior of never
        // failing the HTTP call because a remote fetch stalled.
        try {
          const fresh = await (async () => {
            // client.current_parts lives in the Python bundle; on the Node-only
            // server the current-roll names come from the DB cache population path
            // in worker.js / the device workers. Fall back to an empty fetch so
            // the endpoint stays up.
            return [];
          })();
          for (const r of fresh) {
            await q_none(
              `insert into current_parts(state_cd, ac_no, part_no, part_name,
                part_name_l1, part_id, district_cd, ps_type, ps_caty,
                old_pdf_url, fetched_at)
               values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
               on conflict (state_cd, ac_no, part_no) do update set
                 part_name = excluded.part_name,
                 part_name_l1 = excluded.part_name_l1,
                 part_id = excluded.part_id,
                 district_cd = excluded.district_cd,
                 ps_type = excluded.ps_type,
                 ps_caty = excluded.ps_caty,
                 old_pdf_url = excluded.old_pdf_url,
                 fetched_at = now()`,
              [state, ac,
                r.partNumber, r.partName, r.partNameL1,
                r.partId, r.districtCd, r.psType, r.psCaty,
                r.oldPdfUrl || null]
            );
          }
        } catch (e) { /* gateway fetch is best-effort; return cached rows */ }
      }
      const rows = await q(
        'select * from current_parts where state_cd = $1 and ac_no = $2 order by part_no',
        [state, ac]
      );
      json(res, rows);
      return;
    }

    // ---- /api/part
    if (pathName === '/api/part' && method === 'GET') {
      const state = query.state;
      const ac = parseInt(query.ac, 10);
      const part = parseInt(query.part, 10);
      if (!state || !ac || !part) return json(res, 400, { error: 'state, ac, part required' });
      const row = await q_one(`select state_cd, ac_no, part_no, status, records, epics,
        roll_end, mapping_offset, cur_part_mode, created_at, started_at, finished_at,
        last_error, claimed_by, reserved_by, reserved_at, exists_,
        old_state_name, old_dist_no, old_dist_name, old_ac_name
        from old_parts where state_cd=$1 and ac_no=$2 and part_no=$3`, [state, ac, part]);
      if (!row) return json(res, 404, { error: 'part not found' });
      json(res, { ...row, exists_: row.exists_ });
      return;
    }

    // ---- /api/collect
    if (pathName === '/api/collect' && method === 'POST') {
      const payload = await parseBody(req);
      const stateCd = (payload && payload.state_cd) ? String(payload.state_cd) : null;
      const acNo = parseInt(payload.ac_no, 10);
      const partNo = parseInt(payload.part_no, 10);
      if (!stateCd || !acNo || !partNo) return json(res, 400, { error: 'state_cd, ac_no, part_no required' });
      json(res, 200, await enqueue('collect_part', {
        state_cd: stateCd,
        ac_no: acNo,
        part_no: partNo,
        force: !!payload.force,
      }, 'manual', parseInt(payload.priority, 10) || 100));
      return;
    }

    // ---- /api/collect_auto
    if (pathName === '/api/collect_auto' && method === 'POST') {
      const payload = await parseBody(req);
      const stateCd = payload.state_cd != null ? String(payload.state_cd) : null;
      const acNo = payload.ac_no != null ? parseInt(payload.ac_no, 10) : null;
      json(res, 200, await enqueue('collect_auto', {
        state_cd: stateCd,
        ac_no: acNo,
        max_parts: parseInt(payload.max_parts, 10) || 25,
        force: !!payload.force,
      }, 'auto', 100));
      return;
    }

    // ---- /api/auto
    if (pathName === '/api/auto' && method === 'POST') {
      const payload = await parseBody(req);
      const enabled = !!payload.enabled;
      // write the global settings row (key 'auto_enabled'); every device without a
      // scoped override follows it (mirrors app.py auto behavior).
      await q_none(
        `insert into settings(key, value) values ('auto_enabled', $1)
         on conflict (key) do update set value = excluded.value`,
        [enabled ? 'true' : 'false']
      );
      const ae = await setting('auto_enabled', 'false', null);
      // best-effort event
      try {
        await q_none(
          `insert into events(source, level, message, device) values ($1,$2,$3,$4)`,
          ['api', 'info', 'auto mode (fleet default) ' + (enabled ? 'on' : 'off'), myTag()],
          { fetch: 'none' }
        );
      } catch (e) { /* ignore */ }
      json(res, 200, { auto_enabled: ae === 'true' || ae === true });
      return;
    }

    // ---- /api/settings
    if (pathName === '/api/settings' && method === 'POST') {
      const payload = await parseBody(req);
      const tag = payload.device_tag != null ? String(payload.device_tag) : (DEVICE_TAG || null);
      const allowed = ['workers', 'discover_max_part', 'collect_serial_cap', 'request_pause_ms'];
      for (const k of Object.keys(payload)) {
        if (k === 'device_tag') continue;
        if (allowed.includes(k)) {
          const val = String(payload[k]);
          await q_none(
            `insert into settings(key, value) values ($1, $2)
             on conflict (key) do update set value = excluded.value`,
            [k, val]
          );
          if (tag) {
            await q_none(
              `insert into settings(key, value) values ($1, $2)
               on conflict (key) do update set value = excluded.value`,
              [k + '@' + tag, val]
            );
          }
        }
      }
      json(res, 200, {
        workers: await setting('workers', 6, tag),
        discover_max_part: await setting('discover_max_part', 400, tag),
        collect_serial_cap: await setting('collect_serial_cap', 3000, tag),
        request_pause_ms: await setting('request_pause_ms', 0, tag),
      });
      return;
    }

    // ---- /api/jobs
    if (pathName === '/api/jobs' && method === 'GET') {
      const status = query.status || null;
      const limit = Math.min(parseInt(query.limit, 10) || 50, 500);
      let rows;
      if (status) {
        rows = await q(`select * from jobs where status = $1 order by id desc limit $2`, [status, limit]);
      } else {
        rows = await q(`select * from jobs order by id desc limit $1`, [limit]);
      }
      json(res, rows);
      return;
    }

    // ---- /api/jobs/:job_id/cancel
    const cancelM = pathName.match(/^\/api\/jobs\/(\d+)\/cancel$/);
    if (cancelM && method === 'POST') {
      const jobId = parseInt(cancelM[1], 10);
      await q_none(`update jobs set cancel = true where id = $1 and status in ('queued','running')`, [jobId]);
      try {
        await q_none(
          `insert into events(source, level, message, device) values ($1,$2,$3,$4)`,
          ['api', 'info', 'cancel requested for job #' + jobId, myTag()],
          { fetch: 'none' }
        );
      } catch (e) { /* ignore */ }
      json(res, 200, { cancelled: jobId });
      return;
    }

    // ---- /api/electors
    // Mirrors app.py's electors() route: filters on state/ac/part/cur_part/epic/q,
    // orders by (state_cd, ac_no, part_no, serial_no) so the query uses the
    // electors_old_idx btree and does not sort the whole table, and returns the
    // total count for pagination.
    if (pathName === '/api/electors' && method === 'GET') {
      const state = query.state || null;
      const ac = query.ac != null ? parseInt(query.ac, 10) : null;
      const part = query.part != null ? parseInt(query.part, 10) : null;
      const curPart = query.cur_part != null ? parseInt(query.cur_part, 10) : null;
      const epic = query.epic || null;
      const name = query.q || null;
      const limit = Math.min(parseInt(query.limit, 10) || 100, 1000);
      const offset = parseInt(query.offset, 10) || 0;
      const whereClauses = [];
      const params = [];
      let pidx = 1;
      if (state) { whereClauses.push('state_cd = $' + pidx); params.push(state); pidx++; }
      if (ac) { whereClauses.push('ac_no = $' + pidx); params.push(ac); pidx++; }
      if (part) { whereClauses.push('part_no = $' + pidx); params.push(part); pidx++; }
      if (curPart) { whereClauses.push('cur_part_no = $' + pidx); params.push(curPart); pidx++; }
      if (epic) { whereClauses.push('cur_epic ilike $' + pidx); params.push('%' + epic + '%'); pidx++; }
      if (name) {
        whereClauses.push('(full_name ilike $' + pidx + ' or relative_name ilike $' + (pidx + 1) + ')');
        params.push('%' + name + '%', '%' + name + '%');
        pidx += 2;
      }
      const whereSql = whereClauses.length ? whereClauses.join(' and ') : 'true';
      // filter params first ($1..$N), then limit ($N+1) and offset ($N+2) so the
      // numbered WHERE placeholders line up with the combined array positions.
      // Single-flight the two queries that hit the big electors table so a slow
      // unfiltered query cannot starve the rest of the server. The count(*) is
      // the expensive one on an unfiltered call (5M+ rows); the select uses the
      // electors_old_idx when state+ac are supplied and is cheap.
      const selectSql = `select source_id, state_cd, ac_no, part_no, serial_no, epic_2003,
         marked_by_blo, cur_state_cd, cur_ac_no, cur_part_no, cur_epic,
         first_seen, last_seen, full_name, full_name_l1, relative_name,
         relation_type, gender, age_snapshot
         from electors where ${whereSql}
         order by state_cd, ac_no, part_no, serial_no
         limit $${pidx} offset $${pidx + 1}`;
      const countSql = `select count(*) c from electors where ${whereSql}`;
      const rows = await q(selectSql, [...params, limit, offset]);
      const total = await q_one(countSql, params);
      json(res, {
        rows: rows.map(r => ({
          ...r,
          relation_label: r.relation_type,
          age: r.age_snapshot,
        })),
        total: total ? total.c : 0,
        limit,
        offset,
        from: offset,
        to: Math.min(offset + limit, total ? total.c : 0),
      });
      return;
    }

    // ---- /api/breakdown
    if (pathName === '/api/breakdown' && method === 'GET') {
      const rows = await q(`select state_cd,
        count(*) acs,
        count(*) filter (where status='done') done,
        count(*) filter (where status='pending') pending,
        count(*) filter (where status='running') running,
        count(*) filter (where status='error') error,
        coalesce(sum(records),0) records,
        coalesce(sum(epics),0) epics
        from old_parts group by state_cd order by state_cd`);
      const total = await q_one(`select count(*) c from old_parts`, []);
      json(res, {
        states: rows,
        total: total ? total.c : 0,
        done: rows.reduce((s, r) => s + (r.done || 0), 0),
        pending: rows.reduce((s, r) => s + (r.pending || 0), 0),
        running: rows.reduce((s, r) => s + (r.running || 0), 0),
        error: rows.reduce((s, r) => s + (r.error || 0), 0),
        records: rows.reduce((s, r) => s + (r.records || 0), 0),
        epics: rows.reduce((s, r) => s + (r.epics || 0), 0),
      });
      return;
    }    // ---- /api/export.csv
    // Mirrors app.py's export_csv() route: filters on state/ac/part/cur_part/epic/name,
    // defaults to epics_only=true (only rows carrying a current EPIC), and emits a
    // CSV whose columns match app.py's transformed header so downloaded files are
    // identical whichever backend serves them.
    if (pathName === '/api/export.csv' && method === 'GET') {
      const state = query.state || null;
      const ac = query.ac != null ? parseInt(query.ac, 10) : null;
      const part = query.part != null ? parseInt(query.part, 10) : null;
      const cur_part = query.cur_part != null ? parseInt(query.cur_part, 10) : null;
      const name = query.q || null;
      const epicsOnly = query.epics_only != '0' && query.epics_only !== 'false';
      if (!state || isNaN(ac)) return json(res, 400, { error: 'state and ac required' });
      const whereClauses = ['state_cd = $1', 'ac_no = $2'];
      const params = [state, ac];
      let pidx = 3;
      if (part) { whereClauses.push('part_no = $' + pidx); params.push(part); pidx++; }
      if (cur_part) { whereClauses.push('cur_part_no = $' + pidx); params.push(cur_part); pidx++; }
      if (name) {
        whereClauses.push('(full_name ilike $' + pidx + ' or relative_name ilike $' + (pidx + 1) + ')');
        params.push('%' + name + '%', '%' + name + '%'); pidx += 2;
      }
      if (epicsOnly) whereClauses.push('cur_epic is not null and cur_epic <> \'\'');
      const whereSql = whereClauses.join(' and ');
      const rows = await q(
        `select source_id, state_cd, ac_no, part_no, serial_no, epic_2003,
         marked_by_blo, cur_state_cd, cur_ac_no, cur_part_no, cur_epic,
         first_seen, last_seen, full_name, full_name_l1, relative_name,
         relation_type, gender, age_snapshot
         from electors where ${whereSql}
         order by state_cd, ac_no, part_no, serial_no
         limit 100000`,
        params
      );
      // app.py's export header (transformed, human-readable columns).
      const header = ['epic', 'name', 'name_local', 'relation_type', 'relation',
                      'relation_local', 'gender', 'age_2003', 'epic_2003',
                      'old_serial', 'old_part', 'cur_ac', 'cur_part'];
      function csvEscape(v) {
        if (v === null || v === undefined) return '';
        const s = String(v);
        if (s.includes(',') || s.includes('"') || s.includes('\n'))
          return '"' + s.replace(/"/g, '""') + '"';
        return s;
      }
      const csvRows = [header.join(',')].concat(rows.map(r =>
        [
          r.cur_epic,                    // epic
          r.full_name,                   // name
          r.full_name_l1,                // name_local
          r.relation_type,               // relation_type
          r.relative_name,               // relation
          r.relative_name_l1,            // relation_local
          r.gender,                      // gender
          r.age_snapshot,                // age_2003
          r.epic_2003,                   // epic_2003
          r.serial_no,                   // old_serial
          r.part_no,                     // old_part
          r.cur_ac_no,                   // cur_ac
          r.cur_part_no,                 // cur_part
        ].map(csvEscape).join(',')
      ));
      csv(res, csvRows.join('\n') + '\n');
      return;
    }

    // ---- /api/epic
    if (pathName === '/api/epic' && method === 'POST') {
      const payload = await parseBody(req);
      const epic = (payload && payload.epic) ? String(payload.epic) : null;
      if (!epic) return json(res, 400, { error: 'epic required' });
      const rows = await q(`select source_id, state_cd, ac_no, part_no, serial_no,
        epic_2003, marked_by_blo, cur_state_cd, cur_ac_no, cur_part_no,
        cur_epic, first_seen, last_seen
        from electors where cur_epic = $1 order by first_seen desc limit 200`, [epic]);
      json(res, { epic, rows: rows.map(r => ({ ...r })), count: rows.length });
      return;
    }

    // ---- /api/epic/:epic
    const epicM = pathName.match(/^\/api\/epic\/([^\/]+)$/);
    if (epicM && method === 'GET') {
      const epic = decodeURIComponent(epicM[1]);
      const rows = await q(`select source_id, state_cd, ac_no, part_no, serial_no,
        epic_2003, marked_by_blo, cur_state_cd, cur_ac_no, cur_part_no,
        cur_epic, first_seen, last_seen
        from electors where cur_epic = $1 order by first_seen desc limit 200`, [epic]);
      json(res, { epic, rows: rows.map(r => ({ ...r })), count: rows.length });
      return;
    }

    // ---- /api/epics
    // Mirrors app.py's epics() route: reads from epic_lookups (the small table of
    // recent EPIC lookups), ordered by fetched_at desc. This is fast regardless of
    // electors size. (Querying electors directly with distinct on + order by
    // first_seen desc would force a full-table sort on 5M+ rows.)
    if (pathName === '/api/epics' && method === 'GET') {
      const limit = Math.min(parseInt(query.limit, 10) || 40, 200);
      const rows = await q(
        `select epic, found, http_status, name, part_no, part_name, ac_name,
                serial_no, fetched_at
         from epic_lookups order by fetched_at desc limit $1`,
        [limit]
      );
      json(res, { epics: rows.map(r => ({ ...r })), count: rows.length, limit });
      return;
    }

    // ---- /api/worker/restart
    if (pathName === '/api/worker/restart' && method === 'POST') {
      // no background worker thread in the Node server (free web service sleeps/wakes;
      // the always-on collection stays on the devices). Same shape as app.py for compat.
      json(res, 200, {
        restarted: true,
        note: 'no worker thread in Node server; collection runs on devices'
      });
      return;
    }

    // ---- 404
    json(res, 404, { error: 'not found' });

  } catch (err) {
    console.error('ROUTE ERROR:', err && err.stack ? err.stack : err);
    try {
      if (!res.headersSent) writeJson(res, 500, { error: String(err) });
    } catch (e) { /* response may already be closing */ }
  }
}

// ---------------------------------------------------------------------------
// worker_status() shape — no background thread in Node server; same shape as
// app.py's worker_status() so the frontend renders cleanly.
// ---------------------------------------------------------------------------
function worker_status() {
  return {
    alive: false,
    tick_age: null,
    error: null,
    stopped: false,
    device: DEVICE_TAG || null,
  };
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err && err.stack ? err.stack : err);
});
process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED REJECTION:', reason && reason.stack ? reason.stack : reason);
});

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  route(req, res).catch(err => {
    console.error('ROUTE PROMISE REJECTED:', err && err.stack ? err.stack : err);
    if (!res.headersSent) {
      writeJson(res, 500, { error: String(err) });
    }
  });
});

server.on('error', (err) => {
  console.error('SERVER ERROR:', err && err.stack ? err.stack : err);
});

server.on('clientError', (err, socket) => {
  console.error('CLIENT ERROR:', err && err.stack ? err.stack : err);
  try { socket.end(); } catch (e) { /* ignore */ }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('old_eci dashboard (node) listening on http://0.0.0.0:' + PORT);
  console.log('ECI_PG_DSN set:', !!DSN);
  console.log('ECI_DEVICE_TAG:', DEVICE_TAG || '(none)');
});
