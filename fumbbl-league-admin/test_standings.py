"""
Validate compute_standings() against known-correct fixture data.

Usage:
    python test_standings.py              # run all fixtures
    python test_standings.py <fixture_id> # run one fixture by tournament id

Fixtures live in fixtures/<tournament_id>.json.
Add expected_standings to a fixture to enable validation.
"""

import json
import sys
import pathlib
from main import compute_standings, _cas_total


def build_records(fixture: dict) -> list:
    """Mirrors the match-building logic in generate_standings()."""
    match_records = []
    matches = fixture.get("matches", {})

    for sm in fixture.get("schedule", []):
        result    = sm.get("result") or {}
        winner_id = result.get("winner")
        match_id  = result.get("id")

        if not match_id and not winner_id:
            continue

        sched_teams  = {t["id"]: t for t in (sm.get("teams") or []) if t.get("id")}
        result_teams = {t["id"]: t for t in (result.get("teams") or []) if t.get("id")}
        if len(sched_teams) < 2:
            continue

        t1_id, t2_id = list(sched_teams.keys())[:2]
        rec = {
            "t1_id":    t1_id,
            "t1_name":  sched_teams[t1_id].get("name", ""),
            "t1_score": (result_teams.get(t1_id) or {}).get("score") or 0,
            "t1_cas":   0,
            "t1_coach": "",
            "t2_id":    t2_id,
            "t2_name":  sched_teams[t2_id].get("name", ""),
            "t2_score": (result_teams.get(t2_id) or {}).get("score") or 0,
            "t2_cas":   0,
            "t2_coach": "",
            "winner_id": winner_id,
            "match_id":  match_id,
        }

        if match_id and str(match_id) in matches:
            md = matches[str(match_id)]
            for side in ["team1", "team2"]:
                t   = md.get(side) or {}
                tid = t.get("id")
                if tid == t1_id:
                    rec["t1_score"] = t.get("score") or 0
                    rec["t1_cas"]   = _cas_total(t)
                    rec["t1_coach"] = (t.get("coach") or {}).get("name", "")
                elif tid == t2_id:
                    rec["t2_score"] = t.get("score") or 0
                    rec["t2_cas"]   = _cas_total(t)
                    rec["t2_coach"] = (t.get("coach") or {}).get("name", "")

            if rec["t1_score"] > rec["t2_score"]:
                rec["winner_id"] = t1_id
            elif rec["t2_score"] > rec["t1_score"]:
                rec["winner_id"] = t2_id
            else:
                rec["winner_id"] = 0  # genuine draw

        match_records.append(rec)

    # Exclude filler teams (name contains "Filler") that lost every match.
    filler_non_losses: dict = {}
    for rec in match_records:
        for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
            if "Filler" in name:
                if team_id not in filler_non_losses:
                    filler_non_losses[team_id] = 0
                if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                    filler_non_losses[team_id] += 1

    filler_exclude = {tid for tid, non_losses in filler_non_losses.items() if non_losses == 0}
    if filler_exclude:
        match_records = [
            rec for rec in match_records
            if rec["t1_id"] not in filler_exclude and rec["t2_id"] not in filler_exclude
        ]

    return match_records


def compare(actual: list, expected: list) -> list[str]:
    """Return a list of failure messages (empty = pass)."""
    failures = []
    if len(actual) != len(expected):
        failures.append(f"  row count: got {len(actual)}, expected {len(expected)}")

    for i, (a, e) in enumerate(zip(actual, expected)):
        row_failures = []
        for key in e:
            if a.get(key) != e[key]:
                row_failures.append(f"    {key}: got {a.get(key)!r}, expected {e[key]!r}")
        if row_failures:
            failures.append(f"  position {i+1} ({e.get('team_name', '?')}):")
            failures.extend(row_failures)
    return failures


def run_fixture(path: pathlib.Path) -> bool:
    fixture  = json.loads(path.read_text())
    name     = fixture.get("tournament_name", path.stem)
    expected = fixture.get("expected_standings")

    records  = build_records(fixture)
    actual   = compute_standings(records)

    if not expected:
        print(f"[SKIP] {name}  (no expected_standings — add them to enable validation)")
        print(f"       Computed {len(actual)} teams from {len(records)} match records.")
        for row in actual:
            print(f"       {row['position']:>2}. {row['team_name']:<30} "
                  f"W{row['wins']} D{row['draws']} L{row['losses']}  "
                  f"TD {row['td_for']}-{row['td_against']}  "
                  f"CAS {row['cas_for']}-{row['cas_against']}  "
                  f"Pts {row['points']}")
        return True

    failures = compare(actual, expected)
    if failures:
        print(f"[FAIL] {name}")
        for msg in failures:
            print(msg)
        return False
    else:
        print(f"[PASS] {name}  ({len(actual)} teams, {len(records)} matches)")
        return True


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

    passed = failed = skipped = 0
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
