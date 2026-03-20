"""
Fetch real FUMBBL data for a tournament and save it as a test fixture.

Usage:
    python fetch_fixture.py <tournament_id>

The script will write fixtures/<tournament_id>.json with the raw schedule,
all match data (verbose, with player performances), and player info/career SPP.
You then manually add "expected_standings" and "expected_achievements".
"""

import sys
import json
import time
import pathlib
import httpx

FUMBBL_BASE = "https://fumbbl.com/api"
CALLS_PER_SECOND = 4.0
MIN_INTERVAL = 1.0 / CALLS_PER_SECOND
_last_call = 0.0

def _get(url: str) -> dict | list:
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_call = time.time()
    resp = httpx.get(url, timeout=10.0)
    resp.raise_for_status()
    return resp.json()

def fetch_fixture(tournament_id: int) -> dict:
    print(f"Fetching tournament info for {tournament_id}...")
    info_raw = _get(f"{FUMBBL_BASE}/tournament/get/{tournament_id}")
    if isinstance(info_raw, list) and info_raw:
        info = info_raw[0]
    elif isinstance(info_raw, dict):
        info = info_raw
    else:
        info = {}

    print(f"Fetching schedule...")
    schedule = _get(f"{FUMBBL_BASE}/tournament/schedule/{tournament_id}")
    if not isinstance(schedule, list):
        schedule = []

    match_ids = [
        sm["result"]["id"]
        for sm in schedule
        if (sm.get("result") or {}).get("id")
    ]
    print(f"Found {len(schedule)} scheduled matches, {len(match_ids)} with game data.")

    matches = {}
    for i, mid in enumerate(match_ids, 1):
        print(f"  Fetching match {i}/{len(match_ids)} (id={mid}, verbose)...")
        matches[str(mid)] = _get(f"{FUMBBL_BASE}/match/get/{mid}?verbose=1")

    # Collect unique player IDs from performances
    unique_pids: set = set()
    for md in matches.values():
        for side in ["team1", "team2"]:
            for pid_str in (md.get(side) or {}).get("performances", {}).keys():
                try:
                    unique_pids.add(int(pid_str))
                except (ValueError, TypeError):
                    pass

    print(f"Fetching player info for {len(unique_pids)} unique players...")
    player_info: dict = {}
    player_career_spp: dict = {}
    for i, pid in enumerate(sorted(unique_pids), 1):
        print(f"  Player {i}/{len(unique_pids)} (id={pid})...")
        try:
            pdata = _get(f"{FUMBBL_BASE}/player/get/{pid}")
            player_info[str(pid)] = {
                "name":   pdata.get("name", f"Player {pid}"),
                "status": pdata.get("status", ""),
            }
            player_career_spp[str(pid)] = int((pdata.get("statistics") or {}).get("spp") or 0)
        except Exception as e:
            print(f"    Warning: could not fetch player {pid}: {e}")
            player_info[str(pid)] = {"name": f"Player {pid}", "status": ""}
            player_career_spp[str(pid)] = 0

    return {
        "tournament_id": tournament_id,
        "tournament_name": info.get("name", f"Tournament {tournament_id}"),
        "season": info.get("season"),
        "schedule": schedule,
        "matches": matches,
        "player_info": player_info,
        "player_career_spp": player_career_spp,
        "expected_standings": [
            # Fill this in manually from the FUMBBL standings page.
            # Each entry should match the output of compute_standings():
            # {
            #   "position": 1,
            #   "team_id": 0,
            #   "team_name": "",
            #   "coach_name": "",
            #   "wins": 0, "draws": 0, "losses": 0,
            #   "td_for": 0, "td_against": 0, "td_delta": 0,
            #   "cas_for": 0, "cas_against": 0, "cas_delta": 0,
            #   "points": 0
            # }
        ],
        "expected_achievements": [
            # Fill this in manually from the FUMBBL website / your own knowledge.
            # Each entry should match the output of compute_achievements() per achievement:
            # Tournament award:
            #   {"achievement_type": "tournament_award", "badges": ["SPP"], "player_name": "", "team_name": ""}
            # SPP milestone:
            #   {"achievement_type": "spp_milestone", "achievement_name": "Super Star", "player_name": "", "team_name": ""}
            # Per-game:
            #   {"achievement_type": "per_game", "achievement_name": "Triple X", "player_name": "", "team_name": ""}
        ]
    }

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch_fixture.py <tournament_id>")
        sys.exit(1)

    tid = int(sys.argv[1])
    fixture = fetch_fixture(tid)

    out_dir = pathlib.Path("fixtures")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{tid}.json"
    out_path.write_text(json.dumps(fixture, indent=2))

    print(f"\nSaved to {out_path}")
    print(f"Next steps:")
    print(f"  1. Fill in 'expected_standings' from the FUMBBL standings page.")
    print(f"  2. Fill in 'expected_achievements' from the FUMBBL website.")
    print(f"  3. Run: python test_standings.py {tid}")
    print(f"  4. Run: python test_achievements.py {tid}")
