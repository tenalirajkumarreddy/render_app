"""Anonymous ECI API client.

Two families are used:

* the old-roll (SIR/2003) route found during reverse engineering - it answers
  without any token and returns each old-roll elector together with
  `bloMappedEpicNo` (their *current* EPIC) and the current state/ac/part they
  were mapped into;
* the national EPIC display search (via work/app_search.py, the verified
  captcha-less contract) for the EPIC processor and for calibrating how far the
  mapping's part numbering lags the published roll.
"""
from __future__ import annotations

import os
import random
import sys
import threading
import time

import certifi
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
if PARENT not in sys.path:
    sys.path.insert(0, PARENT)

OLD_EROLL = ("https://gateway-vha.eci.gov.in/api/v1/"
             "elastic-sir-citizen/get-eroll-data-2003")
PART_API = ("https://gateway-vha.eci.gov.in/api/v1/"
            "common/part/get/bystatecd/districtcd/acNumber")
# The voters.eci.gov.in web app's own service. `getPartByAc` is the AC-scoped
# part list; the vha route above is NOT AC-scoped (asked for AC 1 of S01 it
# returns 319 rows where the AC has 153 - it answers for the district).
WEB_API = "https://gateway-voters.eci.gov.in/api/v1/"
PART_BY_AC = WEB_API + "citizen/sir/getPartByAc"
WEB_HEADERS = {
    "Accept": "*/*",
    "applicationname": "VSP",
    "channelidobo": "VSP",
    "currentrole": "citizen",
    "platform-type": "ECIWEB",
    "Origin": "https://voters.eci.gov.in",
    "Referer": "https://voters.eci.gov.in/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"),
}
STATES_API = "https://gateway-vha.eci.gov.in/api/v1/common/states"
ASM_API = "https://gateway-vha.eci.gov.in/api/v1/citizen/sir/getAsmbly"

APP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "applicationName": "VHA",
    "appName": "VHA",
    "channelidobo": "VHA",
    "platform-type": "ANDROIDMOB",
    "currentRole": "citizen",
    "User-Agent": "okhttp/4.9.2",
}

_local = threading.local()
_epic_lock = threading.Lock()
_epic_last = [0.0]


def session() -> requests.Session:
    if not hasattr(_local, "s"):
        s = requests.Session()
        s.verify = certifi.where()
        s.headers.update(APP_HEADERS)
        _local.s = s
    return _local.s


def _post(url, body, timeout=30, retries=3):
    for attempt in range(retries):
        try:
            r = session().post(url, json=body, timeout=timeout)
        except requests.RequestException:
            time.sleep(1.0 * (attempt + 1) * (0.5 + random.random()))
            continue
        if r.status_code == 429:
            # Jitter: several devices retrying the same rate-limited route must
            # not land in lockstep and re-trigger the limiter together.
            time.sleep(2.0 * (attempt + 1) * (0.5 + random.random()))
            continue
        try:
            payload = r.json()
        except ValueError:
            payload = None
        return r.status_code, payload
    return 0, None


def fetch_serial(state, ac, part, serial, timeout=30):
    """One serial of an old part -> (http_status, payload list)."""
    body = {"oldStateCd": state, "oldAcNo": str(ac), "oldPartNo": str(part),
            "oldPartSerialNo": str(serial)}
    status, payload = _post(OLD_EROLL, body, timeout=timeout)
    if status == 200 and isinstance(payload, dict):
        return status, payload.get("payload") or []
    return status, []


def fetch_window(state, ac, part, timeout=30):
    """A ~50-record window of an old part (also used for name discovery)."""
    return fetch_serial(state, ac, part, "", timeout=timeout)


def probe_roll_end(state, ac, part, hard_cap=3000, hint=0):
    """Highest serial that answers, +20 margin (30 if the part is empty).

    `hint` seeds the probe with a neighbour part's roll_end: if the hint
    answers, the result is identical to the full ascending probe but skips the
    wasted low candidates; if the hint misses, the full probe runs.
    """
    cands = (50, 100, 200, 300, 400, 500, 650, 800, 1000, 1200, 1500, 2000, 2500)
    seq = cands
    last = 0
    if hint and int(hint) >= 50:
        h = min(int(hint), hard_cap)
        status, payload = fetch_serial(state, ac, part, h)
        if status == 200 and payload:
            last = h
            seq = tuple(c for c in cands if c > h)
    for cand in seq:
        if cand > hard_cap:
            break
        status, payload = fetch_serial(state, ac, part, cand)
        if status == 200 and payload:
            last = cand
        elif last and cand > last + 120:
            break
    return min(hard_cap, (last + 20) if last else 30)


def web_session() -> requests.Session:
    if not hasattr(_local, "w"):
        s = requests.Session()
        s.verify = certifi.where()
        s.headers.update(WEB_HEADERS)
        _local.w = s
    return _local.w


def _rows(data):
    if isinstance(data, dict):
        for key in ("payload", "data", "result"):
            if isinstance(data.get(key), list):
                return data[key]
        return []
    return data if isinstance(data, list) else []


def current_parts(state, ac, timeout=30):
    """Current-roll parts of one AC, normalised to the fields we store.

    Prefers the web app's AC-scoped `getPartByAc`. The vha fallback answers for
    the whole district, so it is filtered to the rows that name this AC - and if
    those rows do not carry an AC number at all the result is discarded rather
    than silently stored as if it were this AC's list (that is what made AC 1
    look like it had 319 parts when it has 153).
    """
    def norm(x):
        dist = x.get("distNo", x.get("districtCd"))
        return {
            "acNumber": x.get("acNumber", ac),
            "partNumber": x.get("partNumber"),
            "partName": x.get("partName"),
            "partNameL1": x.get("partNameV1") or x.get("partNameL1"),
            "partId": x.get("id", x.get("partId")),
            "districtCd": None if dist is None else str(dist),
            "psType": x.get("psType"),
            "psCaty": x.get("psCaty"),
            "oldPdfUrl": x.get("oldPdfUrl"),
        }

    try:
        r = web_session().get(PART_BY_AC, params={"Asmbly": str(ac)},
                              headers={"state": str(state)}, timeout=timeout)
        if r.status_code == 200:
            rows = [norm(x) for x in _rows(r.json()) if isinstance(x, dict)]
            rows = [x for x in rows if x["partNumber"] is not None]
            if rows:
                return rows
    except Exception:
        pass

    try:
        r = session().get(PART_API, headers={"state": str(state)},
                          params={"stateCd": state, "acNumber": str(ac)},
                          timeout=timeout)
        rows = [x for x in _rows(r.json()) if isinstance(x, dict)]
    except Exception:
        return []
    if rows and all(x.get("acNumber") is not None for x in rows):
        rows = [x for x in rows if str(x.get("acNumber")) == str(ac)]
    elif rows:
        return []
    return [norm(x) for x in rows]


def states_live(timeout=30):
    try:
        r = session().get(STATES_API, timeout=timeout)
        data = r.json()
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("payload") or data.get("data") or []
    out = []
    for row in data or []:
        cd = row.get("stateCd") or row.get("stateCode") or row.get("state_cd")
        nm = row.get("stateName") or row.get("name")
        if cd:
            out.append({"state_cd": cd, "name": nm})
    return out


ASM_API_WEB = WEB_API + "citizen/sir/getAsmbly"


# The reserved-seat category arrives spelled differently per state: 'GEN' /
# 'General' / 'G', '(ST)', and even Devanagari abbreviations (अ.ज.जा. =
# अनुसूचित जाति = Scheduled Caste; अ.जा. = अनुसूचित जनजाति = Scheduled Tribe).
# Values with no known mapping are passed through unchanged so a new spelling
# shows up instead of being silently coerced.
AC_TYPES = {
    "GEN": "GEN", "GENERAL": "GEN", "G": "GEN", "UR": "GEN", "UNRESERVED": "GEN",
    "SC": "SC", "(SC)": "SC", "अ.ज.जा.": "SC",
    "ST": "ST", "(ST)": "ST", "अ.जा.": "ST",
}


def ac_type_label(code):
    if code is None or str(code).strip() == "":
        return None
    raw = str(code).strip()
    return AC_TYPES.get(raw.upper(), raw)


def _ac_row(row):
    ac = (row.get("acNo") or row.get("asmblyNo") or row.get("ac_number")
          or row.get("assemblyNo"))
    if ac is None:
        return None
    dist = row.get("distNo", row.get("districtCd"))
    return {"ac_no": int(ac),
            "name": row.get("acNameV1") or row.get("acName") or row.get("ac_name"),
            "name_l1": row.get("acName"),
            "ac_type": ac_type_label(row.get("acType")),
            "district_cd": None if dist is None else str(dist)}


def acs_live(state, timeout=30):
    """Assembly constituencies of a state, normalised.

    The web gateway is preferred: same list, but it also carries `acType`
    (GEN / SC / ST reserved-seat category), and it is the service
    voters.eci.gov.in itself uses. The VHA route is the fallback.
    """
    try:
        r = web_session().get(ASM_API_WEB, headers={"state": str(state)},
                              timeout=timeout)
        if r.status_code == 200:
            out = [x for x in (_ac_row(row) for row in _rows(r.json())) if x]
            if out:
                return out
    except Exception:
        pass

    try:
        r = session().get(ASM_API, headers={"state": str(state)}, timeout=timeout)
        data = r.json()
    except Exception:
        return []
    return [x for x in (_ac_row(row) for row in _rows(data) if isinstance(row, dict)) if x]


# ---------------------------------------------------------------- EPIC search

def epic_lookup(epic, timeout=40, min_interval=1.1):
    """National EPIC display search (verified contract, ~1 req/s)."""
    with _epic_lock:
        wait = min_interval - (time.time() - _epic_last[0])
        if wait > 0:
            time.sleep(wait)
        try:
            import app_search
            report = app_search.fetch(epic, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            return {"epic": epic, "status": 0, "hits": [], "raw": "",
                    "error": "%s: %s" % (type(exc).__name__, exc)}
        _epic_last[0] = time.time()
    hits = report.get("hits") or []
    content = {}
    if hits:
        content = hits[0].get("content") or {}
    return {"epic": report.get("epic", epic), "status": report.get("status", 0),
            "hits": len(hits), "content": content, "raw": report.get("raw", "")}


# The old-roll route returns single-letter relation codes. Resolved against the
# national search for the same person (12/12 agreement on F->FTHR, H->HSBN,
# M->MTHR, O->OTHR, with the relative's name matching too), so the mapping is
# measured rather than assumed. The old roll never emitted a code outside these
# four across 50k collected rows.
RELATION_TYPES = {
    "F": ("FTHR", "Father"),
    "H": ("HSBN", "Husband"),
    "M": ("MTHR", "Mother"),
    "O": ("OTHR", "Other"),
}
GENDER_TYPES = {"M": "Male", "F": "Female", "T": "Third gender", "O": "Other"}


def relation_label(code, short=False):
    """'M' -> 'Mother' (or 'MTHR' when short=True). Unknown codes pass through,
    so a new code the API starts returning shows up instead of disappearing."""
    if code is None or code == "":
        return None
    hit = RELATION_TYPES.get(str(code).strip().upper())
    if not hit:
        return code
    return hit[0] if short else hit[1]


def gender_label(code):
    if code is None or code == "":
        return None
    return GENDER_TYPES.get(str(code).strip().upper(), code)


def profile_from_content(content: dict) -> dict:
    """Map a national-display record to flat, storable fields."""
    def g(*keys):
        for k in keys:
            v = content.get(k)
            if v not in (None, ""):
                return v
        return None
    return {
        "name": g("fullName", "applicantFirstName"),
        "name_local": g("fullNameL1", "applicantFirstNameL1"),
        "relation": g("relativeFullName", "relationName"),
        "relation_local": g("relativeFullNameL1", "relationNameL1"),
        "relation_type": g("relationType"),
        "relation_label": relation_label(g("relationType")),
        "age": g("age"),
        "gender": g("gender"),
        "state_cd": g("stateCd"),
        "state_name": g("stateName"),
        "district": g("districtValue"),
        "ac_no": g("acNumber"),
        "ac_name": g("asmblyName"),
        "part_no": g("partNumber"),
        "part_name": g("partName"),
        "part_name_l1": g("partNameL1"),
        "part_id": g("partId"),
        "serial_no": g("partSerialNumber"),
        "section_no": g("sectionNo"),
        "ps_building": g("psbuildingName", "buildingAddress"),
        "ps_building_l1": g("psBuildingNameL1", "buildingAddressL1"),
        "record_id": g("id"),
    }
