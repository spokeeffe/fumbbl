"""
Validate compute_achievements() against known-correct fixture data.

Usage:
    python test_achievements.py              # run all fixtures
    python test_achievements.py <fixture_id> # run one fixture by tournament id

Fixtures live in fixtures/<tournament_id>.json.
Add expected_achievements to a fixture to enable validation.

Each fixture must have been fetched with the current fetch_fixture.py (which
includes verbose match data with player performances and player_career_spp).
"""

import json
import sys
import pathlib
from main import compute_player_stats, compute_achievements
from test_standings import build_records


def build_perf_records(fixture: dict) -> list:
    """
    Build match_perf_records from verbose match data (with 'performances') in the fixture.
    Mirrors the logic in _work_achievements().
    """
    matches = fixture.get("matches", {})
    schedule = fixture.get("schedule", [])

    # Build team_id -> team_name from the schedule
    team_names: dict = {}
    for sm in schedule:
        for t in (sm.get("teams") or []):
            if t.get("id"):
                team_names[t["id"]] = t.get("name", "")

    perf_records = []
    for match_id_str, md in matches.items():
        match_id = int(match_id_str)
        for side in ["team1", "team2"]:
            t = md.get(side) or {}
            team_id = t.get("id")
            if not team_id:
                continue
            team_name = team_names.get(team_id, t.get("name", f"Team {team_id}"))
            injuries = t.get("injuries") or {}
            for pid_str, perf in (t.get("performances") or {}).items():
                try:
                    pid = int(pid_str)
                except (ValueError, TypeError):
                    continue
                player_injuries = injuries.get(pid_str) or []
                perf_records.append({
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
                    "died":   any(inj.get("injury") == "d" for inj in player_injuries),
                })
    return perf_records


def apply_filler_exclusion(perf_records: list, match_records: list) -> list:
    """Remove perf records for matches involving always-losing filler teams."""
    filler_non_losses: dict = {}
    for rec in match_records:
        for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
            if "Filler" in name:
                if team_id not in filler_non_losses:
                    filler_non_losses[team_id] = 0
                if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                    filler_non_losses[team_id] += 1

    filler_exclude = {tid for tid, nl in filler_non_losses.items() if nl == 0}
    if not filler_exclude:
        return perf_records

    removed_match_ids = {
        rec["match_id"] for rec in match_records
        if rec["t1_id"] in filler_exclude or rec["t2_id"] in filler_exclude
    }
    return [r for r in perf_records if r["match_id"] not in removed_match_ids]


def ach_key(a: dict) -> tuple:
    """Canonical sort key for an achievement dict (for order-independent comparison)."""
    if a.get("achievement_type") == "tournament_award":
        name_part = tuple(sorted(a.get("badges") or []))
    else:
        name_part = (a.get("achievement_name", ""),)
    return (a.get("achievement_type", ""), name_part, a.get("player_name", ""), a.get("team_name", ""))


def compare_achievements(actual: list, expected: list) -> list[str]:
    """Return a list of failure messages (empty = pass)."""
    def normalise(a: dict) -> dict:
        """Return only the fields we compare (drop match_url etc.)."""
        if a.get("achievement_type") == "tournament_award":
            return {
                "achievement_type": a["achievement_type"],
                "badges": sorted(a.get("badges") or []),
                "player_name": a.get("player_name", ""),
                "team_name": a.get("team_name", ""),
            }
        return {
            "achievement_type": a.get("achievement_type", ""),
            "achievement_name": a.get("achievement_name", ""),
            "player_name": a.get("player_name", ""),
            "team_name": a.get("team_name", ""),
        }

    actual_norm   = sorted([normalise(a) for a in actual],   key=ach_key)
    expected_norm = sorted([normalise(e) for e in expected], key=ach_key)

    failures = []
    if len(actual_norm) != len(expected_norm):
        failures.append(f"  count: got {len(actual_norm)}, expected {len(expected_norm)}")
        # Show extras / missing for easier debugging
        actual_set   = {json.dumps(a, sort_keys=True) for a in actual_norm}
        expected_set = {json.dumps(e, sort_keys=True) for e in expected_norm}
        for x in sorted(actual_set - expected_set):
            failures.append(f"  EXTRA:   {x}")
        for x in sorted(expected_set - actual_set):
            failures.append(f"  MISSING: {x}")
        return failures

    for i, (a, e) in enumerate(zip(actual_norm, expected_norm)):
        if a != e:
            failures.append(f"  position {i+1}: got {a!r}")
            failures.append(f"            expected {e!r}")
    return failures


def compute_dead_players(tournament_data: list) -> list:
    """Mirrors the dead_players computation in _work_achievements."""
    dead = []
    for td in tournament_data:
        pinfo      = td["player_info"]
        career_spp = td["player_career_spp"]
        for rec in td["match_perf_records"]:
            if rec.get("died"):
                pid = rec["player_id"]
                dead.append({
                    "player_name":     (pinfo.get(pid) or {}).get("name", f"Player {pid}"),
                    "team_name":       rec["team_name"],
                    "career_spp":      career_spp.get(pid, 0),
                    "match_id":        rec["match_id"],
                    "tournament_name": td["tournament_name"],
                })
    return dead


def compare_dead_players(actual: list, expected: list) -> list[str]:
    """Return failure messages (empty = pass). Compares player_name + team_name + match_id."""
    def key(d: dict) -> tuple:
        return (d.get("player_name", ""), d.get("team_name", ""), d.get("match_id", 0))

    def normalise(d: dict) -> dict:
        return {
            "player_name": d.get("player_name", ""),
            "team_name":   d.get("team_name", ""),
            "match_id":    d.get("match_id", 0),
        }

    actual_norm   = sorted([normalise(d) for d in actual],   key=key)
    expected_norm = sorted([normalise(d) for d in expected], key=key)

    failures = []
    if len(actual_norm) != len(expected_norm):
        failures.append(f"  dead player count: got {len(actual_norm)}, expected {len(expected_norm)}")
        actual_set   = {json.dumps(d, sort_keys=True) for d in actual_norm}
        expected_set = {json.dumps(d, sort_keys=True) for d in expected_norm}
        for x in sorted(actual_set - expected_set):
            failures.append(f"  EXTRA:   {x}")
        for x in sorted(expected_set - actual_set):
            failures.append(f"  MISSING: {x}")
        return failures

    for i, (a, e) in enumerate(zip(actual_norm, expected_norm)):
        if a != e:
            failures.append(f"  dead[{i}]: got {a!r}")
            failures.append(f"            expected {e!r}")
    return failures


def run_fixture(path: pathlib.Path) -> bool:
    fixture  = json.loads(path.read_text())
    name     = fixture.get("tournament_name", path.stem)
    expected_ach  = fixture.get("expected_achievements")
    expected_dead = fixture.get("expected_dead_players")

    # Build inputs
    match_records = build_records(fixture)
    perf_records  = build_perf_records(fixture)
    perf_records  = apply_filler_exclusion(perf_records, match_records)

    # Filter out players with 0 SPP (mirrors _work_achievements)
    pid_spp: dict = {}
    for rec in perf_records:
        pid_spp[rec["player_id"]] = (
            pid_spp.get(rec["player_id"], 0)
            + rec["td"] * 3 + rec["comp"] + rec["cas"] * 2
            + rec["int_"] * 2 + rec["mvp"] * 4
        )
    # Keep players with SPP > 0, or who died (they may have 0 SPP)
    perf_records = [r for r in perf_records if pid_spp.get(r["player_id"], 0) > 0 or r.get("died")]

    # Load player_info and player_career_spp from fixture (keys are str pids in JSON)
    player_info: dict = {
        int(pid): info
        for pid, info in (fixture.get("player_info") or {}).items()
    }
    player_career_spp: dict = {
        int(pid): spp
        for pid, spp in (fixture.get("player_career_spp") or {}).items()
    }
    # Fill defaults for any players not in fixture
    for rec in perf_records:
        pid = rec["player_id"]
        player_info.setdefault(pid, {"name": f"Player {pid}", "status": ""})
        player_career_spp.setdefault(pid, 0)

    players = compute_player_stats(perf_records, player_info)

    tournament_data = [{
        "tournament_id":      fixture["tournament_id"],
        "tournament_name":    fixture.get("tournament_name", ""),
        "season":             fixture.get("season"),
        "match_perf_records": perf_records,
        "players":            players,
        "player_info":        player_info,
        "player_career_spp":  player_career_spp,
    }]

    results = compute_achievements(tournament_data)
    actual_achievements = results[0]["achievements"] if results else []
    actual_dead         = compute_dead_players(tournament_data)

    passed = True

    # --- Achievements ---
    if not expected_ach:
        print(f"[SKIP] {name}  (no expected_achievements)")
        print(f"       Computed {len(actual_achievements)} achievements:")
        for a in sorted(actual_achievements, key=ach_key):
            if a.get("achievement_type") == "tournament_award":
                print(f"         [award]     {', '.join(a.get('badges', []))} — {a['player_name']} ({a['team_name']})")
            elif a.get("achievement_type") == "spp_milestone":
                print(f"         [milestone] {a['achievement_name']} — {a['player_name']} ({a['team_name']})")
            else:
                print(f"         [per_game]  {a['achievement_name']} — {a['player_name']} ({a['team_name']})")
    else:
        failures = compare_achievements(actual_achievements, expected_ach)
        if failures:
            print(f"[FAIL] {name}  achievements")
            for msg in failures:
                print(msg)
            passed = False
        else:
            print(f"[PASS] {name}  ({len(actual_achievements)} achievements)")

    # --- Dead players ---
    if not expected_dead:
        print(f"[SKIP] {name}  (no expected_dead_players)")
        print(f"       Computed {len(actual_dead)} dead player(s):")
        for d in sorted(actual_dead, key=lambda d: d["player_name"]):
            print(f"         {d['player_name']} ({d['team_name']})  career_spp={d['career_spp']}  match={d['match_id']}")
    else:
        failures = compare_dead_players(actual_dead, expected_dead)
        if failures:
            print(f"[FAIL] {name}  dead players")
            for msg in failures:
                print(msg)
            passed = False
        else:
            print(f"[PASS] {name}  ({len(actual_dead)} dead player(s))")

    return passed


def main():
    fixtures_dir = pathlib.Path("fixtures")
    if not fixtures_dir.exists():
        print("No fixtures/ directory found. Run fetch_fixture.py first.")
        sys.exit(1)

    if len(sys.argv) == 2:
        paths = [fixtures_dir / f"{sys.argv[1]}.json"]
        if not paths[0].exists():
            print(f"Fixture not found: {paths[0]}")
            sys.exit(1)
    else:
        paths = sorted(fixtures_dir.glob("*.json"))
        if not paths:
            print("No fixture files found in fixtures/. Run fetch_fixture.py first.")
            sys.exit(1)

    passed = failed = 0
    for path in paths:
        ok = run_fixture(path)
        if ok:
            passed += 1
        else:
            failed += 1

    print(f"\n{passed} passed, {failed} failed, {len(paths) - passed - failed} skipped")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
