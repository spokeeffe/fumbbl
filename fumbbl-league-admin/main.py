from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
import sqlite3
import time
import csv
import io
import asyncio
import uuid
import traceback
from contextlib import asynccontextmanager
from typing import Optional, List
from urllib.parse import quote_plus
import re
import json

# --- Rate limiter ---
class RateLimiter:
    def __init__(self, calls_per_second: float = 10.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()

rate_limiter = RateLimiter(calls_per_second=10.0)

# In-memory job store for progress tracking: {job_id: {status, completed, total, redirect, error}}
jobs: dict = {}

VALID_SEASONS = {"Winter", "Spring", "Autumn", "Summer"}

def extract_year(tournament_name: str) -> str:
    """Extract the CIBBL year from a tournament name, e.g. 'Y13' from 'CIBBL - Y13, Autumn - Bronze Division'."""
    match = re.search(r'Y(\d+)', tournament_name)
    return match.group(1) if match else ""

def extract_season(tournament_name: str) -> Optional[str]:
    for season in VALID_SEASONS:
        if season in tournament_name:
            return season
    return None

def extract_event(tournament_name: str) -> str:
    """Extract the event name following the season, e.g. 'Bronze Division' from 'CIBBL - Y13, Autumn - Bronze Division'."""
    for season in VALID_SEASONS:
        idx = tournament_name.find(season)
        if idx != -1:
            after = tournament_name[idx + len(season):]
            return re.sub(r'^[\s\-,]+', '', after)
    return tournament_name

def match_metadata_row(tournament_name: str, season: str, metadata_rows) -> Optional[dict]:
    """Find the best-matching metadata row for a tournament.
    Matches on season equality and tournament_name being a substring of the full tournament name.
    Returns the most specific match (longest tournament_name substring).
    """
    matches = [
        row for row in metadata_rows
        if row["season"] == season and row["tournament_name"] in tournament_name
    ]
    return max(matches, key=lambda r: len(r["tournament_name"])) if matches else None

# --- DB setup ---
import os
import shutil
_ON_VERCEL = bool(os.environ.get("VERCEL"))
DB_SEED = "fumbbl_leagues.db"
DB_PATH = "/tmp/fumbbl_leagues.db" if _ON_VERCEL else "fumbbl_leagues.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    # On Vercel, seed /tmp from the committed DB so leagues/metadata/cache
    # survive across cold starts even though /tmp itself is ephemeral.
    if _ON_VERCEL and not os.path.exists(DB_PATH) and os.path.exists(DB_SEED):
        shutil.copy2(DB_SEED, DB_PATH)
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL UNIQUE,
            ruleset_id INTEGER NOT NULL,
            league_name TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS league_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id INTEGER NOT NULL,
            season TEXT NOT NULL,
            tournament_name TEXT NOT NULL,
            prestige_1st INTEGER NOT NULL DEFAULT 0,
            prestige_2nd INTEGER NOT NULL DEFAULT 0,
            prestige_3rd INTEGER NOT NULL DEFAULT 0,
            prestige_4th INTEGER NOT NULL DEFAULT 0,
            summer_year TEXT,
            tournament_image_id INTEGER,
            player_award_name TEXT,
            player_award_id INTEGER,
            UNIQUE(league_id, season, tournament_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS standings_cache (
            league_id INTEGER PRIMARY KEY,
            data TEXT NOT NULL,
            generated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_stats_cache (
            league_id INTEGER PRIMARY KEY,
            data TEXT NOT NULL,
            generated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS achievements_cache (
            league_id INTEGER PRIMARY KEY,
            data TEXT NOT NULL,
            generated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS match_cache (
            match_id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_cache (
            player_id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT,
            spp INTEGER DEFAULT 0
        )
    """)
    for tbl in ["standings_cache", "player_stats_cache", "achievements_cache"]:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN perf_summary TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    conn.close()

# --- FUMBBL API client ---
FUMBBL_BASE = "https://fumbbl.com/api"

def _log_api_call(job_id: Optional[str], endpoint: str, elapsed: float):
    if job_id and job_id in jobs:
        jobs[job_id]["api_log"].append({"endpoint": endpoint, "elapsed": round(elapsed, 3)})

def _log_fn_call(job_id: Optional[str], fn_name: str, elapsed: float):
    if job_id and job_id in jobs:
        jobs[job_id]["fn_log"].append({"fn": fn_name, "elapsed": round(elapsed, 3)})

def _compute_perf_summary(j: dict) -> dict:
    api_agg: dict = {}
    for entry in j.get("api_log", []):
        ep = entry["endpoint"]
        if ep not in api_agg:
            api_agg[ep] = {"endpoint": ep, "count": 0, "total_elapsed": 0.0}
        api_agg[ep]["count"] += 1
        api_agg[ep]["total_elapsed"] += entry["elapsed"]

    fn_agg: dict = {}
    for entry in j.get("fn_log", []):
        fn = entry["fn"]
        if fn not in fn_agg:
            fn_agg[fn] = {"fn": fn, "count": 0, "total_elapsed": 0.0}
        fn_agg[fn]["count"] += 1
        fn_agg[fn]["total_elapsed"] += entry["elapsed"]

    return {
        "api_summary": [
            {"endpoint": v["endpoint"], "count": v["count"],
             "total_elapsed": round(v["total_elapsed"], 3),
             "avg_elapsed": round(v["total_elapsed"] / v["count"], 3)}
            for v in sorted(api_agg.values(), key=lambda x: -x["total_elapsed"])
        ],
        "fn_summary": [
            {"fn": v["fn"], "count": v["count"],
             "total_elapsed": round(v["total_elapsed"], 3),
             "avg_elapsed": round(v["total_elapsed"] / v["count"], 3)}
            for v in sorted(fn_agg.values(), key=lambda x: -x["total_elapsed"])
        ],
    }

async def fetch_group(group_id: int, job_id: Optional[str] = None) -> dict:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/group/get/{group_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        _log_api_call(job_id, "group/get", time.time() - t0)
        return resp.json()

async def fetch_tournaments(group_id: int, job_id: Optional[str] = None) -> list:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/group/tournaments/{group_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        _log_api_call(job_id, "group/tournaments", time.time() - t0)
        return data if isinstance(data, list) else []

async def fetch_tournament_info(tournament_id: int, job_id: Optional[str] = None) -> dict:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/tournament/get/{tournament_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        _log_api_call(job_id, "tournament/get", time.time() - t0)
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return {"name": f"Tournament {tournament_id}", "season": None}

async def fetch_tournament_schedule(tournament_id: int, job_id: Optional[str] = None) -> list:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/tournament/schedule/{tournament_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        _log_api_call(job_id, "tournament/schedule", time.time() - t0)
        return data if isinstance(data, list) else []

async def fetch_match(match_id: int, job_id: Optional[str] = None) -> dict:
    conn = get_db()
    cached = conn.execute("SELECT data FROM match_cache WHERE match_id = ?", (match_id,)).fetchone()
    conn.close()
    if cached:
        return json.loads(cached["data"])
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/match/get/{match_id}?verbose=1"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        _log_api_call(job_id, "match/get", time.time() - t0)
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO match_cache (match_id, data) VALUES (?, ?)", (match_id, json.dumps(data)))
        conn.commit()
        conn.close()
        return data

async def fetch_player(player_id: int, job_id: Optional[str] = None) -> dict:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/player/get/{player_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        _log_api_call(job_id, "player/get", time.time() - t0)
        return resp.json()

async def fetch_team(team_id: int, job_id: Optional[str] = None) -> dict:
    rate_limiter.wait()
    url = f"{FUMBBL_BASE}/team/get/{team_id}"
    t0 = time.time()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        _log_api_call(job_id, "team/get", time.time() - t0)
        return data

def _cas_total(t: dict) -> int:
    c = t.get("casualties") or {}
    return sum(c.values()) if c else 0

def compute_standings(match_records: list) -> list:
    """
    match_records: list of dicts with keys:
      t1_id, t1_name, t1_coach, t1_score, t1_cas,
      t2_id, t2_name, t2_coach, t2_score, t2_cas,
      winner_id  (team ID of winner, or 0 for draw)

    Tiebreaker order (when points are equal):
      1. Points earned in head-to-head matches among the tied teams (descending)
      2. Overall TD differential (descending)
      3. Overall CAS differential (descending)
      4. Team name (ascending) — coin-flip proxy for stability
    """
    teams: dict = {}
    # h2h_pts[tid][opp_tid] = league points tid earned in matches against opp_tid
    h2h_pts: dict = {}

    for rec in match_records:
        t1_id = rec["t1_id"]
        t2_id = rec["t2_id"]

        for tid, name, coach in [
            (t1_id, rec["t1_name"], rec["t1_coach"]),
            (t2_id, rec["t2_name"], rec["t2_coach"]),
        ]:
            if tid not in teams:
                teams[tid] = {
                    "team_id": tid,
                    "team_name": name,
                    "coach_name": coach,
                    "wins": 0, "draws": 0, "losses": 0,
                    "td_for": 0, "td_against": 0,
                    "cas_for": 0, "cas_against": 0,
                    "points": 0,
                }
                h2h_pts[tid] = {}

        teams[t1_id]["td_for"] += rec["t1_score"]
        teams[t1_id]["td_against"] += rec["t2_score"]
        teams[t1_id]["cas_for"] += rec["t1_cas"]
        teams[t1_id]["cas_against"] += rec["t2_cas"]

        teams[t2_id]["td_for"] += rec["t2_score"]
        teams[t2_id]["td_against"] += rec["t1_score"]
        teams[t2_id]["cas_for"] += rec["t2_cas"]
        teams[t2_id]["cas_against"] += rec["t1_cas"]

        winner_id = rec["winner_id"]
        if winner_id == t1_id:
            teams[t1_id]["wins"] += 1
            teams[t1_id]["points"] += 3
            teams[t2_id]["losses"] += 1
            h2h_pts[t1_id][t2_id] = h2h_pts[t1_id].get(t2_id, 0) + 3
        elif winner_id == t2_id:
            teams[t2_id]["wins"] += 1
            teams[t2_id]["points"] += 3
            teams[t1_id]["losses"] += 1
            h2h_pts[t2_id][t1_id] = h2h_pts[t2_id].get(t1_id, 0) + 3
        else:  # draw
            teams[t1_id]["draws"] += 1
            teams[t1_id]["points"] += 1
            teams[t2_id]["draws"] += 1
            teams[t2_id]["points"] += 1
            h2h_pts[t1_id][t2_id] = h2h_pts[t1_id].get(t2_id, 0) + 1
            h2h_pts[t2_id][t1_id] = h2h_pts[t2_id].get(t1_id, 0) + 1

    # Primary sort by points, then resolve ties group by group
    ordered = sorted(teams.values(), key=lambda t: -t["points"])

    result = []
    i = 0
    while i < len(ordered):
        j = i + 1
        while j < len(ordered) and ordered[j]["points"] == ordered[i]["points"]:
            j += 1
        group = ordered[i:j]

        if len(group) > 1:
            group_ids = {t["team_id"] for t in group}
            group.sort(key=lambda t: (
                -sum(h2h_pts[t["team_id"]].get(opp, 0) for opp in group_ids if opp != t["team_id"]),
                -(t["td_for"] - t["td_against"]),
                -(t["cas_for"] - t["cas_against"]),
                t["team_name"],
            ))

        result.extend(group)
        i = j

    for i, row in enumerate(result, 1):
        row["position"] = i
        row["td_delta"] = row["td_for"] - row["td_against"]
        row["cas_delta"] = row["cas_for"] - row["cas_against"]
    return result

def compute_player_stats(match_perf_records: list, player_info: dict) -> list:
    """
    Aggregate per-match per-player performance records into player-level stats.

    match_perf_records: list of dicts, one entry per player per match:
        player_id, team_id, team_name, match_id,
        td, comp, cas, int_, mvp, pass_, rush, blocks, fouls, turns

    player_info: dict keyed by player_id -> {"name": str, "status": str}

    Returns list of player stat dicts sorted by spp desc.
    """
    players: dict = {}

    for rec in match_perf_records:
        pid = rec["player_id"]
        if pid not in players:
            info = player_info.get(pid, {})
            players[pid] = {
                "player_id": pid,
                "player_name": info.get("name", f"Player {pid}"),
                "player_status": info.get("status", ""),
                "team_id": rec["team_id"],
                "team_name": rec["team_name"],
                "games": 0,
                "td": 0, "comp": 0, "cas": 0, "int_": 0, "mvp": 0,
                "pass_": 0, "rush": 0, "blocks": 0, "fouls": 0,
                "larson": 0, "mean_scoring_machine": 0,
                "triple_x": 0, "aerodynamic_aim": 0,
            }
        p = players[pid]
        td    = rec["td"]
        comp  = rec["comp"]
        cas   = rec["cas"]
        int_  = rec["int_"]
        mvp   = rec["mvp"]
        turns = rec["turns"]

        if turns > 0:
            p["games"] += 1
        p["td"]     += td
        p["comp"]   += comp
        p["cas"]    += cas
        p["int_"]   += int_
        p["mvp"]    += mvp
        p["pass_"]  += rec["pass_"]
        p["rush"]   += rec["rush"]
        p["blocks"] += rec["blocks"]
        p["fouls"]  += rec["fouls"]

        # Per-match enrichments (summed across all qualifying matches)
        if td >= 1 and cas >= 1 and comp >= 1 and int_ >= 1:
            p["larson"] += 1
        if td >= 3:
            p["mean_scoring_machine"] += 1
        if cas >= 3:
            p["triple_x"] += 1
        if comp >= 4:
            p["aerodynamic_aim"] += 1

    result = []
    for p in players.values():
        td   = p["td"]
        comp = p["comp"]
        cas  = p["cas"]
        int_ = p["int_"]

        p["spp"] = td * 3 + comp + cas * 2 + int_ * 2 + p["mvp"] * 4

        # Post-aggregation derived stats (0 if any required input is 0)
        p["blocking_scorer"]  = min(td, cas)        if td   > 0 and cas  > 0              else 0
        p["blocking_thrower"] = min(comp, cas)      if comp > 0 and cas  > 0              else 0
        p["scoring_thrower"]  = min(td, comp)       if td   > 0 and comp > 0              else 0
        p["triple"]           = min(td, cas, comp)  if td   > 0 and cas  > 0 and comp > 0 else 0
        p["all_rounder"]      = min(td, cas, comp, int_) \
                                if td > 0 and cas > 0 and comp > 0 and int_ > 0 else 0

        result.append(p)

    result.sort(key=lambda p: (-p["spp"], p["team_name"], p["player_name"]))
    return result


# --- Achievements ---

SPP_THRESHOLDS = [(51, "Star"), (76, "Super Star"), (126, "Mega Star"), (176, "Legend")]

TOURNAMENT_AWARDS = [
    ("spp",             "SPP"),
    ("comp",            "Completions"),
    ("td",              "Touchdowns"),
    ("cas",             "Casualties"),
    ("fouls",           "Fouls"),
    ("int_",            "Interceptions"),
    ("scoring_thrower", "Scoring Thrower"),
    ("blocking_scorer", "Blocking Scorer"),
    ("blocking_thrower","Blocking Thrower"),
    ("triple",          "Triple"),
]


def compute_achievements(tournament_data: list) -> list:
    """
    tournament_data: list of per-tournament dicts, each with:
      tournament_id, tournament_name, season,
      match_perf_records, players (from compute_player_stats),
      player_info (pid -> {name, status}),
      player_career_spp (pid -> int)
    Returns list of achievement dicts, newest-first.
    """
    # Process oldest-first so SPP threshold running total is chronologically correct
    sorted_data = sorted(tournament_data, key=lambda t: t["tournament_id"])

    # Total SPP each player earns across all selected tournaments
    total_selected_spp: dict = {}
    all_career_spp: dict = {}
    for td in sorted_data:
        all_career_spp.update(td["player_career_spp"])
        for p in td["players"]:
            pid = p["player_id"]
            total_selected_spp[pid] = total_selected_spp.get(pid, 0) + p["spp"]

    # Running SPP starts at (career total - total in selected tournaments)
    running_spp: dict = {
        pid: all_career_spp.get(pid, 0) - total_selected_spp.get(pid, 0)
        for pid in total_selected_spp
    }

    results = []
    for td in sorted_data:
        players   = td["players"]
        pinfo     = td["player_info"]
        perf_recs = td["match_perf_records"]
        career_spp = td["player_career_spp"]
        achievements = []

        # 1. Tournament awards — group all badges won by the same player into one row
        player_awards: dict = {}  # player_name -> {player_name, team_name, badges}
        individual_award_count = 0
        for col, award_name in TOURNAMENT_AWARDS:
            eligible = [
                p for p in players
                if (p.get("player_status") or "").lower() != "dead" and p.get(col, 0) > 0
            ]
            if not eligible:
                continue
            max_val = max(p[col] for p in eligible)
            winners = [p for p in eligible if p[col] == max_val]
            if len(winners) == 1:
                w = winners[0]
                key = w["player_name"]
                if key not in player_awards:
                    player_awards[key] = {
                        "player_name": w["player_name"],
                        "team_name":   w["team_name"],
                        "badges":      [],
                    }
                player_awards[key]["badges"].append(award_name)
                individual_award_count += 1

        for pa in player_awards.values():
            achievements.append({
                "achievement_type": "tournament_award",
                "badges":      pa["badges"],
                "player_name": pa["player_name"],
                "team_name":   pa["team_name"],
                "match_url":   None,
            })

        # 2. SPP threshold achievements — career milestones crossed in this tournament
        spp_milestone_count = 0
        for p in players:
            pid       = p["player_id"]
            spp_here  = p["spp"]
            spp_before = running_spp.get(pid, 0)
            spp_after  = spp_before + spp_here
            if career_spp.get(pid, 0) > 0:
                for threshold, label in SPP_THRESHOLDS:
                    if spp_before < threshold <= spp_after:
                        achievements.append({
                            "achievement_type": "spp_milestone",
                            "achievement_name": label,
                            "player_name": p["player_name"],
                            "team_name":   p["team_name"],
                            "match_url":   None,
                        })
                        spp_milestone_count += 1
            running_spp[pid] = spp_after

        # 3. Per-game achievements — one row per qualifying match performance
        per_game_count = 0
        for rec in perf_recs:
            pid   = rec["player_id"]
            pname = (pinfo.get(pid) or {}).get("name", f"Player {pid}")
            tname = rec["team_name"]
            murl  = f"https://fumbbl.com/p/match?id={rec['match_id']}"
            td_v, cas_v, comp_v, int_v = rec["td"], rec["cas"], rec["comp"], rec["int_"]
            if cas_v >= 3:
                achievements.append({"achievement_type": "per_game", "achievement_name": "Triple X",            "player_name": pname, "team_name": tname, "match_url": murl})
                per_game_count += 1
            if td_v >= 3:
                achievements.append({"achievement_type": "per_game", "achievement_name": "Mean Scoring Machine", "player_name": pname, "team_name": tname, "match_url": murl})
                per_game_count += 1
            if comp_v >= 4:
                achievements.append({"achievement_type": "per_game", "achievement_name": "Aerodynamic Aim",      "player_name": pname, "team_name": tname, "match_url": murl})
                per_game_count += 1
            if td_v >= 1 and cas_v >= 1 and comp_v >= 1 and int_v >= 1:
                achievements.append({"achievement_type": "per_game", "achievement_name": "Larson",               "player_name": pname, "team_name": tname, "match_url": murl})
                per_game_count += 1

        total_count = individual_award_count + spp_milestone_count + per_game_count

        results.append({
            "tournament_id":     td["tournament_id"],
            "tournament_name":   td["tournament_name"],
            "season":            td["season"],
            "achievement_count": total_count,
            "achievements":      achievements,
        })

    results.reverse()  # newest-first for display
    return results


# --- App ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/favicon", StaticFiles(directory="favicon"), name="favicon")
templates = Jinja2Templates(directory="templates")

# --- Input validation ---
def validate_positive_int(value: str, field_name: str) -> int:
    try:
        v = int(value)
        if v <= 0:
            raise ValueError
        return v
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"{field_name} must be a positive integer.")

# --- Routes ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, success: Optional[str] = None, error: Optional[str] = None):
    conn = get_db()
    leagues = conn.execute(
        "SELECT * FROM leagues ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "leagues": leagues,
        "success": success,
        "error": error,
    })

@app.post("/leagues/add")
async def add_league(
    request: Request,
    group_id: str = Form(...),
    ruleset_id: str = Form(...),
):
    # Validate inputs
    try:
        gid = validate_positive_int(group_id, "Group ID")
        rid = validate_positive_int(ruleset_id, "Ruleset ID")
    except HTTPException as e:
        return RedirectResponse(url=f"/?error={e.detail}", status_code=303)

    # Check duplicate
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM leagues WHERE group_id = ?", (gid,)
    ).fetchone()
    if existing:
        conn.close()
        return RedirectResponse(url=f"/?error=League+with+Group+ID+{gid}+already+exists.", status_code=303)

    # Fetch league name from FUMBBL API
    try:
        data = await fetch_group(gid)
        league_name = data.get("name") or data.get("shortName") or f"Group {gid}"
    except httpx.HTTPStatusError as e:
        conn.close()
        if e.response.status_code == 404:
            return RedirectResponse(url=f"/?error=Group+ID+{gid}+not+found+on+FUMBBL.", status_code=303)
        return RedirectResponse(url=f"/?error=FUMBBL+API+error+(HTTP+{e.response.status_code}).", status_code=303)
    except httpx.RequestError:
        conn.close()
        return RedirectResponse(url=f"/?error=Could+not+reach+FUMBBL+API.+Check+your+connection.", status_code=303)
    except Exception:
        conn.close()
        return RedirectResponse(url=f"/?error=Unexpected+error+fetching+league+data.", status_code=303)

    # Store in DB
    try:
        conn.execute(
            "INSERT INTO leagues (group_id, ruleset_id, league_name) VALUES (?, ?, ?)",
            (gid, rid, league_name)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return RedirectResponse(url=f"/?error=League+with+Group+ID+{gid}+already+exists.", status_code=303)
    finally:
        conn.close()

    return RedirectResponse(url=f"/?success=League+%22{league_name}%22+added+successfully.", status_code=303)

@app.post("/leagues/delete/{league_id}")
async def delete_league(league_id: int):
    conn = get_db()
    conn.execute("DELETE FROM leagues WHERE id = ?", (league_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/?success=League+removed.", status_code=303)

@app.get("/jobs/{job_id}")
async def job_status(job_id: str):
    j = jobs.get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    total = j["total"]
    completed = j["completed"]
    if j["status"] == "done":
        pct = 100
    elif total > 0:
        pct = min(99, int(completed / total * 100))
    else:
        pct = 0

    perf = _compute_perf_summary(j)
    api_log = j.get("api_log", [])
    fn_log = j.get("fn_log", [])

    return {
        "status": j["status"],
        "percent": pct,
        "redirect": j["redirect"],
        "error": j["error"],
        "recent_api_calls": api_log[-50:],
        "recent_fn_calls": fn_log[-50:],
        "api_summary": perf["api_summary"],
        "fn_summary": perf["fn_summary"],
    }


async def _work_standings(league_id: int, tournament_ids: list, job_id: str):
    j = jobs[job_id]
    try:
        results = []
        for tid in tournament_ids:
            info = await fetch_tournament_info(tid, job_id=job_id)
            j["completed"] += 1
            schedule = await fetch_tournament_schedule(tid, job_id=job_id)
            j["completed"] += 1
            j["total"] += sum(1 for m in schedule if (m.get("result") or {}).get("id"))

            match_records = []
            crushing_victories = []
            for scheduled_match in schedule:
                result = scheduled_match.get("result") or {}
                winner_id = result.get("winner")
                match_id = result.get("id")

                if not match_id and not winner_id:
                    continue

                sched_teams = {t["id"]: t for t in (scheduled_match.get("teams") or []) if t.get("id")}
                result_teams = {t["id"]: t for t in (result.get("teams") or []) if t.get("id")}
                if len(sched_teams) < 2:
                    continue

                t1_id, t2_id = list(sched_teams.keys())[:2]
                rec = {
                    "t1_id": t1_id,
                    "t1_name": sched_teams[t1_id].get("name", ""),
                    "t1_score": (result_teams.get(t1_id) or {}).get("score") or 0,
                    "t1_cas": 0,
                    "t1_coach": "",
                    "t2_id": t2_id,
                    "t2_name": sched_teams[t2_id].get("name", ""),
                    "t2_score": (result_teams.get(t2_id) or {}).get("score") or 0,
                    "t2_cas": 0,
                    "t2_coach": "",
                    "winner_id": winner_id,
                    "match_id": match_id,
                }

                if match_id:
                    match_data = await fetch_match(match_id, job_id=job_id)
                    j["completed"] += 1
                    for side in ["team1", "team2"]:
                        t = match_data.get(side) or {}
                        team_id = t.get("id")
                        if team_id == t1_id:
                            rec["t1_score"] = t.get("score") or 0
                            rec["t1_cas"] = _cas_total(t)
                            rec["t1_coach"] = (t.get("coach") or {}).get("name", "")
                        elif team_id == t2_id:
                            rec["t2_score"] = t.get("score") or 0
                            rec["t2_cas"] = _cas_total(t)
                            rec["t2_coach"] = (t.get("coach") or {}).get("name", "")

                    if rec["t1_score"] > rec["t2_score"]:
                        rec["winner_id"] = t1_id
                    elif rec["t2_score"] > rec["t1_score"]:
                        rec["winner_id"] = t2_id
                    else:
                        rec["winner_id"] = 0

                    hi, lo = sorted(
                        [(rec["t1_name"], rec["t1_score"]), (rec["t2_name"], rec["t2_score"])],
                        key=lambda x: -x[1],
                    )
                    if hi[1] >= 4 and lo[1] == 0:
                        crushing_victories.append({
                            "match_id": match_id,
                            "winner_name": hi[0],
                            "winner_score": hi[1],
                            "loser_name": lo[0],
                            "loser_score": lo[1],
                        })

                match_records.append(rec)

            filler_non_losses: dict = {}
            for rec in match_records:
                for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
                    if "Filler" in name:
                        if team_id not in filler_non_losses:
                            filler_non_losses[team_id] = 0
                        if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                            filler_non_losses[team_id] += 1

            filler_exclude = {fid for fid, non_losses in filler_non_losses.items() if non_losses == 0}
            if filler_exclude:
                removed_match_ids = {
                    rec["match_id"] for rec in match_records
                    if rec["t1_id"] in filler_exclude or rec["t2_id"] in filler_exclude
                }
                match_records = [
                    rec for rec in match_records
                    if rec["t1_id"] not in filler_exclude and rec["t2_id"] not in filler_exclude
                ]
                crushing_victories = [
                    cv for cv in crushing_victories
                    if cv["match_id"] not in removed_match_ids
                ]

            t0 = time.time()
            standings = compute_standings(match_records)
            _log_fn_call(job_id, "compute_standings", time.time() - t0)
            results.append({
                "tournament_id": tid,
                "tournament_name": info.get("name", f"Tournament {tid}"),
                "season": info.get("season"),
                "match_count": len(match_records),
                "standings": standings,
                "crushing_victories": crushing_victories,
            })

        perf_summary = _compute_perf_summary(j)
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO standings_cache (league_id, data, generated_at, perf_summary) VALUES (?, ?, datetime('now'), ?)",
            (league_id, json.dumps(results), json.dumps(perf_summary)),
        )
        conn.commit()
        conn.close()
        j["redirect"] = f"/leagues/{league_id}?tab=standings"
        j["status"] = "done"
    except httpx.HTTPStatusError as e:
        j["status"] = "error"
        j["error"] = f"FUMBBL API error (HTTP {e.response.status_code})."
    except httpx.RequestError:
        j["status"] = "error"
        j["error"] = "Could not reach FUMBBL API. Check your connection."
    except Exception as e:
        j["status"] = "error"
        j["error"] = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"


@app.post("/leagues/{league_id}/standings", response_class=HTMLResponse)
async def generate_standings(
    request: Request,
    league_id: int,
    tournament_ids: Optional[List[int]] = Form(default=None),
):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    conn.close()
    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not tournament_ids:
        return RedirectResponse(url=f"/leagues/{league_id}", status_code=303)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "completed": 0, "total": len(tournament_ids) * 2, "redirect": None, "error": None, "api_log": [], "fn_log": []}
    asyncio.create_task(_work_standings(league_id, tournament_ids, job_id))
    return templates.TemplateResponse("progress.html", {"request": request, "job_id": job_id, "operation": "Standings"})

async def _gather_player_info(
    match_perf_records: list, job_id: str, need_spp: bool = False
) -> tuple[dict, dict]:
    """
    Build player_info {pid -> {name, status}} and player_career_spp {pid -> int}.

    Priority order (cheapest first):
      1. team/get for each unique team  — marks current roster players as Active
      2. player_cache SQLite table      — avoids re-fetching known players
      3. player/get API                 — only for players not yet seen; result is cached
    """
    j = jobs[job_id]
    unique_pids: set = {p["player_id"] for p in match_perf_records}
    unique_team_ids: set = {p["team_id"] for p in match_perf_records}
    player_info: dict = {}
    player_career_spp: dict = {}

    # 1. Fetch team rosters — one call per team covers all currently active players
    j["total"] += len(unique_team_ids)
    for team_id in unique_team_ids:
        try:
            team_data = await fetch_team(team_id, job_id=job_id)
            for p in (team_data.get("players") or []):
                pid = p.get("id")
                if pid in unique_pids:
                    player_info[pid] = {"name": p.get("name", f"Player {pid}"), "status": "Active"}
                    if need_spp:
                        player_career_spp[pid] = int((p.get("record") or {}).get("spp") or 0)
        except Exception:
            pass
        j["completed"] += 1

    # 2. Check SQLite cache for players not found in any team roster
    still_missing = unique_pids - set(player_info)
    if still_missing:
        conn = get_db()
        found_in_cache = []
        for pid in still_missing:
            row = conn.execute(
                "SELECT name, status, spp FROM player_cache WHERE player_id = ?", (pid,)
            ).fetchone()
            if row:
                player_info[pid] = {"name": row["name"] or f"Player {pid}", "status": row["status"] or ""}
                if need_spp:
                    player_career_spp[pid] = row["spp"] or 0
                found_in_cache.append(pid)
        conn.close()
        still_missing -= set(found_in_cache)

    # 3. API calls only for players not in team roster or cache; results stored in cache
    j["total"] += len(still_missing)
    for pid in still_missing:
        try:
            pdata = await fetch_player(pid, job_id=job_id)
            name = pdata.get("name", f"Player {pid}")
            status = pdata.get("status", "")
            spp = int((pdata.get("statistics") or {}).get("spp") or 0)
            player_info[pid] = {"name": name, "status": status}
            if need_spp:
                player_career_spp[pid] = spp
            conn = get_db()
            conn.execute(
                "INSERT OR REPLACE INTO player_cache (player_id, name, status, spp) VALUES (?, ?, ?, ?)",
                (pid, name, status, spp),
            )
            conn.commit()
            conn.close()
        except Exception:
            player_info[pid] = {"name": f"Player {pid}", "status": ""}
            if need_spp:
                player_career_spp[pid] = 0
        j["completed"] += 1

    for pid in unique_pids:
        player_info.setdefault(pid, {"name": f"Player {pid}", "status": ""})
        if need_spp:
            player_career_spp.setdefault(pid, 0)

    return player_info, player_career_spp


async def _work_player_stats(league_id: int, tournament_ids: list, job_id: str):
    j = jobs[job_id]
    try:
        results = []
        for tid in tournament_ids:
            info = await fetch_tournament_info(tid, job_id=job_id)
            j["completed"] += 1
            schedule = await fetch_tournament_schedule(tid, job_id=job_id)
            j["completed"] += 1
            j["total"] += sum(1 for m in schedule if (m.get("result") or {}).get("id"))

            match_records = []
            match_perf_records = []

            for scheduled_match in schedule:
                result_block = scheduled_match.get("result") or {}
                winner_id = result_block.get("winner")
                match_id = result_block.get("id")

                if not match_id and not winner_id:
                    continue

                sched_teams = {t["id"]: t for t in (scheduled_match.get("teams") or []) if t.get("id")}
                if len(sched_teams) < 2:
                    continue

                t1_id, t2_id = list(sched_teams.keys())[:2]
                t1_name = sched_teams[t1_id].get("name", "")
                t2_name = sched_teams[t2_id].get("name", "")

                rec = {
                    "t1_id": t1_id, "t1_name": t1_name,
                    "t2_id": t2_id, "t2_name": t2_name,
                    "winner_id": winner_id,
                    "match_id": match_id,
                }

                if match_id:
                    match_data = await fetch_match(match_id, job_id=job_id)
                    j["completed"] += 1
                    t1_score = t2_score = 0
                    for side in ["team1", "team2"]:
                        t = match_data.get(side) or {}
                        team_id = t.get("id")
                        if team_id == t1_id:
                            t1_score = t.get("score") or 0
                            team_name = t1_name
                        elif team_id == t2_id:
                            t2_score = t.get("score") or 0
                            team_name = t2_name
                        else:
                            continue

                        for pid_str, perf in (t.get("performances") or {}).items():
                            try:
                                pid = int(pid_str)
                            except (ValueError, TypeError):
                                continue
                            match_perf_records.append({
                                "player_id": pid,
                                "team_id":   team_id,
                                "team_name": team_name,
                                "match_id":  match_id,
                                "td":     int(perf.get("td")     or 0),
                                "comp":   int(perf.get("comp")   or 0),
                                "cas":    int(perf.get("cas")    or 0),
                                "int_":   int(perf.get("int")    or 0),
                                "mvp":    int(perf.get("mvp")    or 0),
                                "pass_":  int(perf.get("pass")   or 0),
                                "rush":   int(perf.get("rush")   or 0),
                                "blocks": int(perf.get("blocks") or 0),
                                "fouls":  int(perf.get("fouls")  or 0),
                                "turns":  int(perf.get("turns")  or 0),
                            })

                    if t1_score > t2_score:
                        rec["winner_id"] = t1_id
                    elif t2_score > t1_score:
                        rec["winner_id"] = t2_id
                    else:
                        rec["winner_id"] = 0

                match_records.append(rec)

            filler_non_losses: dict = {}
            for rec in match_records:
                for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
                    if "Filler" in name:
                        if team_id not in filler_non_losses:
                            filler_non_losses[team_id] = 0
                        if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                            filler_non_losses[team_id] += 1

            filler_exclude = {fid for fid, nl in filler_non_losses.items() if nl == 0}
            if filler_exclude:
                removed_match_ids = {
                    rec["match_id"] for rec in match_records
                    if rec["t1_id"] in filler_exclude or rec["t2_id"] in filler_exclude
                }
                match_perf_records = [
                    p for p in match_perf_records
                    if p["match_id"] not in removed_match_ids
                ]

            pid_spp: dict = {}
            for rec in match_perf_records:
                pid_spp[rec["player_id"]] = pid_spp.get(rec["player_id"], 0) + rec["td"] * 3 + rec["comp"] + rec["cas"] * 2 + rec["int_"] * 2 + rec["mvp"] * 4
            match_perf_records = [r for r in match_perf_records if pid_spp.get(r["player_id"], 0) > 0]

            player_info, _ = await _gather_player_info(match_perf_records, job_id, need_spp=False)

            t0 = time.time()
            players = compute_player_stats(match_perf_records, player_info)
            _log_fn_call(job_id, "compute_player_stats", time.time() - t0)
            results.append({
                "tournament_id":   tid,
                "tournament_name": info.get("name", f"Tournament {tid}"),
                "season":          info.get("season"),
                "player_count":    len(players),
                "players":         players,
            })

        perf_summary = _compute_perf_summary(j)
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO player_stats_cache (league_id, data, generated_at, perf_summary) VALUES (?, ?, datetime('now'), ?)",
            (league_id, json.dumps(results), json.dumps(perf_summary)),
        )
        conn.commit()
        conn.close()
        j["redirect"] = f"/leagues/{league_id}?tab=player-stats"
        j["status"] = "done"
    except httpx.HTTPStatusError as e:
        j["status"] = "error"
        j["error"] = f"FUMBBL API error (HTTP {e.response.status_code})."
    except httpx.RequestError:
        j["status"] = "error"
        j["error"] = "Could not reach FUMBBL API. Check your connection."
    except Exception as e:
        j["status"] = "error"
        j["error"] = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"


@app.post("/leagues/{league_id}/player_stats", response_class=HTMLResponse)
async def generate_player_stats(
    request: Request,
    league_id: int,
    tournament_ids: Optional[List[int]] = Form(default=None),
):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    conn.close()
    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not tournament_ids:
        return RedirectResponse(url=f"/leagues/{league_id}", status_code=303)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "completed": 0, "total": len(tournament_ids) * 2, "redirect": None, "error": None, "api_log": [], "fn_log": []}
    asyncio.create_task(_work_player_stats(league_id, tournament_ids, job_id))
    return templates.TemplateResponse("progress.html", {"request": request, "job_id": job_id, "operation": "Player Stats"})


@app.get("/leagues/{league_id}/standings/perf", response_class=HTMLResponse)
async def standings_perf(request: Request, league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    row = conn.execute("SELECT perf_summary FROM standings_cache WHERE league_id = ?", (league_id,)).fetchone()
    conn.close()
    if not league or not row or not row["perf_summary"]:
        raise HTTPException(status_code=404, detail="No performance data available")
    perf = json.loads(row["perf_summary"])
    return templates.TemplateResponse("perf.html", {
        "request": request, "league": league, "tab_name": "Standings",
        "api_summary": perf.get("api_summary", []), "fn_summary": perf.get("fn_summary", []),
    })


@app.get("/leagues/{league_id}/player_stats/perf", response_class=HTMLResponse)
async def player_stats_perf(request: Request, league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    row = conn.execute("SELECT perf_summary FROM player_stats_cache WHERE league_id = ?", (league_id,)).fetchone()
    conn.close()
    if not league or not row or not row["perf_summary"]:
        raise HTTPException(status_code=404, detail="No performance data available")
    perf = json.loads(row["perf_summary"])
    return templates.TemplateResponse("perf.html", {
        "request": request, "league": league, "tab_name": "Player Stats",
        "api_summary": perf.get("api_summary", []), "fn_summary": perf.get("fn_summary", []),
    })


@app.get("/leagues/{league_id}/achievements/perf", response_class=HTMLResponse)
async def achievements_perf(request: Request, league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    row = conn.execute("SELECT perf_summary FROM achievements_cache WHERE league_id = ?", (league_id,)).fetchone()
    conn.close()
    if not league or not row or not row["perf_summary"]:
        raise HTTPException(status_code=404, detail="No performance data available")
    perf = json.loads(row["perf_summary"])
    return templates.TemplateResponse("perf.html", {
        "request": request, "league": league, "tab_name": "Achievements",
        "api_summary": perf.get("api_summary", []), "fn_summary": perf.get("fn_summary", []),
    })


@app.get("/leagues/{league_id}/standings/export")
async def export_standings(league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    standings_row = conn.execute(
        "SELECT data FROM standings_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    metadata_rows = conn.execute(
        "SELECT season, tournament_name FROM league_metadata WHERE league_id = ?", (league_id,)
    ).fetchall()
    conn.close()

    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not standings_row:
        raise HTTPException(status_code=404, detail="No standings generated yet")

    results = json.loads(standings_row["data"])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Year", "Season", "Tournament Name", "Tournament ID",
        "Position", "Team Name", "Team ID", "Wins", "Draws",
    ])

    for tournament in results:
        t_name = tournament["tournament_name"]
        t_id = tournament["tournament_id"]
        year = extract_year(t_name)
        season = extract_season(t_name)
        meta = match_metadata_row(t_name, season, metadata_rows) if season else None

        for row in tournament["standings"]:
            writer.writerow([
                year,
                season or "",
                meta["tournament_name"] if meta else "",
                t_id,
                row["position"],
                row["team_name"],
                row["team_id"],
                row["wins"],
                row["draws"],
            ])

    league_slug = league["league_name"].replace(" ", "_")
    filename = f"{league_slug}_standings_export.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/leagues/{league_id}/player_stats/export")
async def export_player_stats(league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    stats_row = conn.execute(
        "SELECT data FROM player_stats_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    conn.close()

    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not stats_row:
        raise HTTPException(status_code=404, detail="No player stats generated yet")

    results = json.loads(stats_row["data"])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Tournament Name", "Tournament ID",
        "Player Name", "Player ID", "Player Status",
        "Team Name", "Team ID",
        "Games Played", "SPP",
        "Completions", "Touchdowns", "Casualties", "Fouls", "Interceptions",
        "Blocks", "Rushing Yards", "Passing Yards",
        "Scoring Thrower", "Blocking Thrower", "Blocking Scorer",
        "Triple", "All Rounder",
        "Larson", "Mean Scoring Machine", "Triple X", "Aerodynamic Aim",
    ])

    for tournament in results:
        for p in tournament["players"]:
            writer.writerow([
                tournament["tournament_name"], tournament["tournament_id"],
                p["player_name"], p["player_id"], p["player_status"],
                p["team_name"], p["team_id"],
                p["games"], p["spp"],
                p["comp"], p["td"], p["cas"], p["fouls"], p["int_"],
                p["blocks"], p["rush"], p["pass_"],
                p["scoring_thrower"], p["blocking_thrower"], p["blocking_scorer"],
                p["triple"], p["all_rounder"],
                p["larson"], p["mean_scoring_machine"], p["triple_x"], p["aerodynamic_aim"],
            ])

    league_slug = league["league_name"].replace(" ", "_")
    filename = f"{league_slug}_player_stats_export.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def _work_achievements(league_id: int, tournament_ids: list, job_id: str):
    j = jobs[job_id]
    try:
        per_tournament_data = []
        for tid in tournament_ids:
            info     = await fetch_tournament_info(tid, job_id=job_id)
            j["completed"] += 1
            schedule = await fetch_tournament_schedule(tid, job_id=job_id)
            j["completed"] += 1
            j["total"] += sum(1 for m in schedule if (m.get("result") or {}).get("id"))

            match_records: list = []
            match_perf_records: list = []

            for scheduled_match in schedule:
                result_block = scheduled_match.get("result") or {}
                winner_id    = result_block.get("winner")
                match_id     = result_block.get("id")

                if not match_id and not winner_id:
                    continue

                sched_teams = {t["id"]: t for t in (scheduled_match.get("teams") or []) if t.get("id")}
                if len(sched_teams) < 2:
                    continue

                t1_id, t2_id = list(sched_teams.keys())[:2]
                t1_name = sched_teams[t1_id].get("name", "")
                t2_name = sched_teams[t2_id].get("name", "")

                rec = {
                    "t1_id": t1_id, "t1_name": t1_name,
                    "t2_id": t2_id, "t2_name": t2_name,
                    "winner_id": winner_id, "match_id": match_id,
                }

                if match_id:
                    match_data = await fetch_match(match_id, job_id=job_id)
                    j["completed"] += 1
                    t1_score = t2_score = 0
                    for side in ["team1", "team2"]:
                        t       = match_data.get(side) or {}
                        team_id = t.get("id")
                        if team_id == t1_id:
                            t1_score  = t.get("score") or 0
                            team_name = t1_name
                        elif team_id == t2_id:
                            t2_score  = t.get("score") or 0
                            team_name = t2_name
                        else:
                            continue

                        for pid_str, perf in (t.get("performances") or {}).items():
                            try:
                                pid = int(pid_str)
                            except (ValueError, TypeError):
                                continue
                            match_perf_records.append({
                                "player_id": pid,
                                "team_id":   team_id,
                                "team_name": team_name,
                                "match_id":  match_id,
                                "td":     int(perf.get("td")     or 0),
                                "comp":   int(perf.get("comp")   or 0),
                                "cas":    int(perf.get("cas")    or 0),
                                "int_":   int(perf.get("int")    or 0),
                                "mvp":    int(perf.get("mvp")    or 0),
                                "pass_":  int(perf.get("pass")   or 0),
                                "rush":   int(perf.get("rush")   or 0),
                                "blocks": int(perf.get("blocks") or 0),
                                "fouls":  int(perf.get("fouls")  or 0),
                                "turns":  int(perf.get("turns")  or 0),
                            })

                    if t1_score > t2_score:
                        rec["winner_id"] = t1_id
                    elif t2_score > t1_score:
                        rec["winner_id"] = t2_id
                    else:
                        rec["winner_id"] = 0

                match_records.append(rec)

            filler_non_losses: dict = {}
            for rec in match_records:
                for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
                    if "Filler" in name:
                        if team_id not in filler_non_losses:
                            filler_non_losses[team_id] = 0
                        if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                            filler_non_losses[team_id] += 1

            filler_exclude = {fid for fid, nl in filler_non_losses.items() if nl == 0}
            if filler_exclude:
                removed_match_ids = {
                    rec["match_id"] for rec in match_records
                    if rec["t1_id"] in filler_exclude or rec["t2_id"] in filler_exclude
                }
                match_perf_records = [p for p in match_perf_records if p["match_id"] not in removed_match_ids]

            pid_spp: dict = {}
            for rec in match_perf_records:
                pid_spp[rec["player_id"]] = pid_spp.get(rec["player_id"], 0) + rec["td"] * 3 + rec["comp"] + rec["cas"] * 2 + rec["int_"] * 2 + rec["mvp"] * 4
            match_perf_records = [r for r in match_perf_records if pid_spp.get(r["player_id"], 0) > 0]

            player_info, player_career_spp = await _gather_player_info(match_perf_records, job_id, need_spp=True)

            t0 = time.time()
            players = compute_player_stats(match_perf_records, player_info)
            _log_fn_call(job_id, "compute_player_stats", time.time() - t0)
            per_tournament_data.append({
                "tournament_id":      tid,
                "tournament_name":    info.get("name", f"Tournament {tid}"),
                "season":             info.get("season"),
                "match_perf_records": match_perf_records,
                "players":            players,
                "player_info":        player_info,
                "player_career_spp":  player_career_spp,
            })

        t0 = time.time()
        results = compute_achievements(per_tournament_data)
        _log_fn_call(job_id, "compute_achievements", time.time() - t0)
        perf_summary = _compute_perf_summary(j)
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO achievements_cache (league_id, data, generated_at, perf_summary) VALUES (?, ?, datetime('now'), ?)",
            (league_id, json.dumps(results), json.dumps(perf_summary)),
        )
        conn.commit()
        conn.close()
        j["redirect"] = f"/leagues/{league_id}?tab=achievements"
        j["status"] = "done"
    except httpx.HTTPStatusError as e:
        j["status"] = "error"
        j["error"] = f"FUMBBL API error (HTTP {e.response.status_code})."
    except httpx.RequestError:
        j["status"] = "error"
        j["error"] = "Could not reach FUMBBL API. Check your connection."
    except Exception as e:
        j["status"] = "error"
        j["error"] = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"


@app.post("/leagues/{league_id}/achievements", response_class=HTMLResponse)
async def generate_achievements(
    request: Request,
    league_id: int,
    tournament_ids: Optional[List[int]] = Form(default=None),
):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    conn.close()
    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not tournament_ids:
        return RedirectResponse(url=f"/leagues/{league_id}", status_code=303)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "completed": 0, "total": len(tournament_ids) * 2, "redirect": None, "error": None, "api_log": [], "fn_log": []}
    asyncio.create_task(_work_achievements(league_id, tournament_ids, job_id))
    return templates.TemplateResponse("progress.html", {"request": request, "job_id": job_id, "operation": "Achievements"})


@app.get("/leagues/{league_id}/achievements/export")
async def export_achievements(league_id: int):
    conn = get_db()
    league = conn.execute("SELECT * FROM leagues WHERE id = ?", (league_id,)).fetchone()
    ach_row = conn.execute(
        "SELECT data FROM achievements_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    conn.close()

    if not league:
        raise HTTPException(status_code=404, detail="League not found")
    if not ach_row:
        raise HTTPException(status_code=404, detail="No achievements generated yet")

    results = json.loads(ach_row["data"])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Year", "Season", "Tournament", "Achievement", "Player Name", "Team Name", "URL"])

    for tournament in results:
        tname = tournament["tournament_name"]
        year = extract_year(tname)
        season = extract_season(tname) or ""
        event = extract_event(tname)
        for a in tournament["achievements"]:
            writer.writerow([
                year,
                season,
                event,
                "; ".join(a["badges"]) if a.get("achievement_type") == "tournament_award" else a["achievement_name"],
                a["player_name"],
                a["team_name"],
                a["match_url"] or "",
            ])

    league_slug = league["league_name"].replace(" ", "_")
    filename = f"{league_slug}_achievements_export.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/leagues/{league_id}", response_class=HTMLResponse)
async def league_detail(
    request: Request,
    league_id: int,
    season: Optional[int] = None,
    success: Optional[str] = None,
    error: Optional[str] = None,
):
    conn = get_db()
    league = conn.execute(
        "SELECT * FROM leagues WHERE id = ?", (league_id,)
    ).fetchone()
    if not league:
        conn.close()
        raise HTTPException(status_code=404, detail="League not found")

    metadata_rows = conn.execute(
        """SELECT season, tournament_name, prestige_1st, prestige_2nd, prestige_3rd, prestige_4th,
                  summer_year, tournament_image_id, player_award_name, player_award_id
           FROM league_metadata WHERE league_id = ?
           ORDER BY season, tournament_name""",
        (league_id,),
    ).fetchall()
    metadata_total = len(metadata_rows)

    standings_row = conn.execute(
        "SELECT data, generated_at, perf_summary FROM standings_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    standings_results = json.loads(standings_row["data"]) if standings_row else None
    standings_generated_at = standings_row["generated_at"] if standings_row else None
    standings_has_perf = bool(standings_row and standings_row["perf_summary"]) if standings_row else False

    player_stats_row = conn.execute(
        "SELECT data, generated_at, perf_summary FROM player_stats_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    player_stats_results = json.loads(player_stats_row["data"]) if player_stats_row else None
    player_stats_generated_at = player_stats_row["generated_at"] if player_stats_row else None
    player_stats_has_perf = bool(player_stats_row and player_stats_row["perf_summary"]) if player_stats_row else False

    achievements_row = conn.execute(
        "SELECT data, generated_at, perf_summary FROM achievements_cache WHERE league_id = ?", (league_id,)
    ).fetchone()
    achievements_results    = json.loads(achievements_row["data"]) if achievements_row else None
    achievements_generated_at = achievements_row["generated_at"] if achievements_row else None
    achievements_has_perf = bool(achievements_row and achievements_row["perf_summary"]) if achievements_row else False
    conn.close()

    all_tournaments = []
    tournaments_error = None
    try:
        all_tournaments = await fetch_tournaments(league["group_id"])
        all_tournaments.sort(key=lambda t: (-(t.get("season") or 0), t.get("name") or ""))
    except httpx.HTTPStatusError as e:
        tournaments_error = f"FUMBBL API error (HTTP {e.response.status_code})."
    except httpx.RequestError:
        tournaments_error = "Could not reach FUMBBL API. Check your connection."
    except Exception:
        tournaments_error = "Unexpected error fetching tournament data."

    seasons = sorted({t["season"] for t in all_tournaments if t.get("season") is not None}, reverse=True)
    tournaments = [t for t in all_tournaments if t.get("season") == season] if season is not None else all_tournaments

    return templates.TemplateResponse("league.html", {
        "request": request,
        "league": league,
        "tournaments": tournaments,
        "tournaments_error": tournaments_error,
        "seasons": seasons,
        "selected_season": season,
        "metadata_rows": metadata_rows,
        "metadata_total": metadata_total,
        "standings_results": standings_results,
        "standings_generated_at": standings_generated_at,
        "standings_has_perf": standings_has_perf,
        "player_stats_results": player_stats_results,
        "player_stats_generated_at": player_stats_generated_at,
        "player_stats_has_perf": player_stats_has_perf,
        "achievements_results": achievements_results,
        "achievements_generated_at": achievements_generated_at,
        "achievements_has_perf": achievements_has_perf,
        "success": success,
        "error": error,
    })


@app.post("/leagues/{league_id}/metadata/upload")
async def upload_metadata(league_id: int, csv_file: UploadFile = File(...)):
    conn = get_db()
    league = conn.execute("SELECT id FROM leagues WHERE id = ?", (league_id,)).fetchone()
    if not league:
        conn.close()
        raise HTTPException(status_code=404, detail="League not found")

    content = await csv_file.read()
    try:
        text = content.decode("utf-8-sig")  # handle BOM from Excel exports
    except UnicodeDecodeError:
        conn.close()
        return RedirectResponse(url=f"/leagues/{league_id}?tab=metadata&error=File+must+be+UTF-8+encoded.", status_code=303)

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = set(reader.fieldnames or [])
    required_cols = {"season", "tournament_name", "prestige_1st", "prestige_2nd", "prestige_3rd", "prestige_4th"}
    missing = required_cols - fieldnames
    if missing:
        conn.close()
        return RedirectResponse(
            url=f"/leagues/{league_id}?tab=metadata&error={quote_plus('Missing CSV columns: ' + ', '.join(sorted(missing)))}",
            status_code=303,
        )

    rows = []
    errors = []
    for i, row in enumerate(reader, start=2):
        season = (row.get("season") or "").strip()
        tournament_name = (row.get("tournament_name") or "").strip()

        if not season:
            errors.append(f"Row {i}: season is required.")
        elif season not in VALID_SEASONS:
            errors.append(f"Row {i}: season '{season}' must be one of: {', '.join(sorted(VALID_SEASONS))}.")

        if not tournament_name:
            errors.append(f"Row {i}: tournament_name is required.")

        prestige = {}
        for col in ("prestige_1st", "prestige_2nd", "prestige_3rd", "prestige_4th"):
            raw = (row.get(col) or "0").strip() or "0"
            try:
                v = int(raw)
                if v < 0:
                    raise ValueError
                prestige[col] = v
            except (ValueError, TypeError):
                errors.append(f"Row {i}: {col} must be a non-negative integer.")
                prestige[col] = 0

        summer_year = (row.get("summer_year") or "").strip() or None
        if summer_year and not re.match(r"^Y\d+$", summer_year):
            errors.append(f"Row {i}: summer_year '{summer_year}' must be empty or match Y<number> (e.g. Y9).")
            summer_year = None

        tournament_image_id = None
        raw_img = (row.get("tournament_image_id") or "").strip()
        if raw_img:
            try:
                tournament_image_id = int(raw_img)
            except ValueError:
                errors.append(f"Row {i}: tournament_image_id must be an integer.")

        player_award_name = (row.get("player_award_name") or "").strip() or None

        player_award_id = None
        raw_award = (row.get("player_award_id") or "").strip()
        if raw_award:
            try:
                player_award_id = int(raw_award)
            except ValueError:
                errors.append(f"Row {i}: player_award_id must be an integer.")

        if len(errors) >= 5:
            break

        if not errors:
            rows.append((
                league_id, season, tournament_name,
                prestige["prestige_1st"], prestige["prestige_2nd"],
                prestige["prestige_3rd"], prestige["prestige_4th"],
                summer_year, tournament_image_id, player_award_name, player_award_id,
            ))

    if errors:
        conn.close()
        return RedirectResponse(
            url=f"/leagues/{league_id}?tab=metadata&error={quote_plus('; '.join(errors[:3]))}",
            status_code=303,
        )

    if not rows:
        conn.close()
        return RedirectResponse(
            url=f"/leagues/{league_id}?tab=metadata&error=CSV+file+is+empty+or+has+no+valid+data+rows.",
            status_code=303,
        )

    conn.execute("DELETE FROM league_metadata WHERE league_id = ?", (league_id,))
    conn.executemany(
        """INSERT INTO league_metadata
           (league_id, season, tournament_name, prestige_1st, prestige_2nd, prestige_3rd, prestige_4th,
            summer_year, tournament_image_id, player_award_name, player_award_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    conn.close()
    return RedirectResponse(
        url=f"/leagues/{league_id}?tab=metadata&success={quote_plus(f'Loaded {len(rows)} metadata rows.')}",
        status_code=303,
    )


@app.post("/leagues/{league_id}/metadata/clear")
async def clear_metadata(league_id: int):
    conn = get_db()
    conn.execute("DELETE FROM league_metadata WHERE league_id = ?", (league_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(
        url=f"/leagues/{league_id}?tab=metadata&success=Metadata+cleared.",
        status_code=303,
    )
