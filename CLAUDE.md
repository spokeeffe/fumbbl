# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working Directory

All application code lives in `fumbbl-league-admin/`. Run commands from that directory.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the development server (from fumbbl-league-admin/)
uvicorn main:app --reload

# Run all standing tests
python test_standings.py

# Run a single fixture test
python test_standings.py <tournament_id>

# Fetch a new test fixture from the live FUMBBL API
python fetch_fixture.py <tournament_id>
```

## Architecture

Single-file FastAPI app (`main.py`, ~1500 lines) with Jinja2 templates and SQLite persistence.

**Request flow for standings/player stats/achievements:**
1. Route handler validates input, creates a job entry in the in-memory `jobs` dict, launches `asyncio.create_task(_work_*)`, and returns `progress.html` immediately.
2. The `_work_*` background function calls the FUMBBL API, updates `jobs[job_id]["completed"]` and `jobs[job_id]["total"]` after each API call, then saves results to the SQLite cache table.
3. The progress page polls `GET /jobs/{job_id}` every 500ms and redirects when `status == "done"`.

**Rate limiter:** `RateLimiter` class (top of `main.py`) uses a synchronous `time.sleep` inside the async fetch functions — this blocks the event loop intentionally since requests are sequential. Currently set to 10 calls/second.

**Core computation functions (pure, no I/O):**
- `compute_standings(match_records)` — standings with H2H tiebreakers
- `compute_player_stats(match_perf_records, player_info)` — per-player aggregated stats
- `compute_achievements(tournament_data)` — tournament awards, SPP milestones, per-game badges

**SQLite cache tables:** `standings_cache`, `player_stats_cache`, `achievements_cache` — each keyed by `league_id` (one row per league, replaced on regenerate).

## FUMBBL API Quirks

- `result.winner` in schedule data is unreliable for draws — always fetch the full match via `GET /match/get/{id}?verbose=1` and compare actual scores.
- Forfeits have `result.winner` but no `result.id` — trusted as-is, recorded as 0–0.
- Performance stats in match data are **strings**, not ints — cast explicitly.
- `turns > 0` is the signal that a player actually played in a match.
- Career SPP for SPP milestone achievements comes from `GET /player/get/{playerId}` → `spp` field.

## Filler Team Exclusion

Teams with "Filler" in their name that lose every match are excluded entirely before standings/stats are computed. See `LOGIC.md` for full details.

## Testing

Fixtures in `fixtures/<tournament_id>.json` capture real API responses and expected standings. After adding a new fixture, manually populate `expected_standings` from the FUMBBL website. See `LOGIC.md` for fixture schema.
