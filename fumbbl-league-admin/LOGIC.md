# Business Logic & API Behaviour Notes

This file documents key decisions, API quirks, and edge cases discovered during development.
Update it whenever new logic is added or an API behaviour is discovered.

---

## Standings Computation

### Points System

| Result | Points |
|--------|--------|
| Win    | 3      |
| Draw   | 1      |
| Loss   | 0      |

### Tiebreaker Order

When teams are tied on points, the following checks are applied in order:

1. **Head-to-head points** — points earned in matches played only among the tied teams (descending). Uses the same 3/1/0 system, so draws count. Only matches between teams within the tied group are considered.
2. **Overall TD differential** (TD for − TD against, descending)
3. **Overall CAS differential** (CAS for − CAS against, descending)
4. **Team name** (ascending, alphabetical) — coin-flip proxy for display stability

Implemented in `compute_standings()` in `main.py`.

---

## FUMBBL API Quirks

### `result.winner` is unreliable for draws

`GET /tournament/schedule/{id}` returns scheduled matches. Each match has a `result` block.
`result.winner` is **always set to a team ID** — even when the match ended in a draw (equal scores).
It cannot be used to detect draws.

**Fix**: For played matches (those with a `result.id` / `match_id`), fetch the full match via
`GET /match/get/{match_id}` and compare the actual scores:

```python
if rec["t1_score"] > rec["t2_score"]:
    rec["winner_id"] = t1_id
elif rec["t2_score"] > rec["t1_score"]:
    rec["winner_id"] = t2_id
else:
    rec["winner_id"] = 0  # genuine draw
```

For forfeits (no `match_id`), `result.winner` is the only signal and is trusted as-is.

---

## Forfeits

Occasionally a match is awarded to a team without a game being played (forfeit).
These matches have `result.winner` set but no `result.id` (no game record exists).

**Handling**:
- Counted as a win for the awarded team and a loss for the other.
- TDs and CAS are recorded as 0–0 for both teams.
- Detection: `match_id` is absent/null in the schedule result.

**Filtering unplayed matches**: Schedule slots with neither a `match_id` nor a `winner_id`
are skipped entirely — they represent future/unscheduled games.

```python
if not match_id and not winner_id:
    continue
```

---

## Filler Teams

Some tournaments use "Filler" teams to pad out the bracket when there aren't enough real participants.
A filler team has `"Filler"` in its name (e.g. "Filler 1", "The Fillers").

**Exclusion rule**: After collecting all match records for a tournament, any filler team that lost
every single match (no wins, no draws) is excluded entirely. All matches involving that team are
dropped before standings are computed, so they have no effect on other teams' W/D/L, TD, or CAS totals.

A filler team that wins or draws at least one match is treated as a normal participant and kept.

**Detection**:

```python
filler_non_losses = {}  # team_id -> count of wins+draws
for rec in match_records:
    for team_id, name in [(rec["t1_id"], rec["t1_name"]), (rec["t2_id"], rec["t2_name"])]:
        if "Filler" in name:
            if team_id not in filler_non_losses:
                filler_non_losses[team_id] = 0
            if rec["winner_id"] == team_id or rec["winner_id"] == 0:
                filler_non_losses[team_id] += 1

filler_exclude = {tid for tid, non_losses in filler_non_losses.items() if non_losses == 0}
```

Implemented in `generate_standings()` in `main.py` and mirrored in `build_records()` in `test_standings.py`.

---

## Player Stats

### API: verbose match data

`GET /match/get/{matchId}?verbose=1` adds a `performances` dict to each team object, keyed by
player ID (as a string). Each entry contains per-match stats for that player:

```json
"performances": {
  "12199586": {
    "blocks": "2", "cas": "1", "comp": "0", "fouls": "0",
    "int": "0",   "mvp": "0",  "pass": "3", "rush": "25",
    "td": "1",    "turns": "16"
  }
}
```

All values are **strings** and must be cast to `int`. `pass` (passing yards) can be **negative**
(incomplete pass deducts yards). `turns` > 0 indicates the player participated in the match.

Player name and status are not included — fetch `GET /player/get/{playerId}` per unique player
to obtain `name`, `status` (Active / Dead / Retired), and `teamId`.

### SPP formula

SPP is not returned directly; derive it from the per-match performance:

| Action       | SPP |
|--------------|-----|
| Touchdown    | 3   |
| Completion   | 1   |
| Casualty     | 2   |
| Interception | 2   |
| MVP          | 4   |

```python
spp = td * 3 + comp * 1 + cas * 2 + int_ * 2 + mvp * 4
```

### Per-match enrichments (computed before aggregation)

| Column            | Rule                                                                 |
|-------------------|----------------------------------------------------------------------|
| Larson            | 1 if td ≥ 1 AND cas ≥ 1 AND comp ≥ 1 AND int ≥ 1 in same match     |
| Mean Scoring Machine | 1 if td ≥ 3 in same match                                        |
| Triple X          | 1 if cas ≥ 3 in same match                                          |
| Aerodynamic Aim   | 1 if comp ≥ 4 in same match                                         |

These are summed across all matches at aggregation (count of qualifying matches, not a flag).

### Post-aggregation enrichments

| Column          | Rule                                                      |
|-----------------|-----------------------------------------------------------|
| Games Played    | Count of matches where `turns` > 0                       |
| Blocking Scorer | `min(td, cas)` — both must be > 0, else 0                |
| Blocking Thrower| `min(comp, cas)` — both must be > 0, else 0              |
| Scoring Thrower | `min(td, comp)` — both must be > 0, else 0               |
| Triple          | `min(td, cas, comp)` — all must be > 0, else 0           |
| All Rounder     | `min(td, cas, comp, int)` — all must be > 0, else 0      |
| Player Status   | From `/player/get/{playerId}` → `status` field           |

---

## Achievements

### SPP Milestones

Players are awarded a career milestone when they cross an SPP threshold **during** a tournament.
The thresholds are:

| Threshold | Award      |
|-----------|------------|
| 51 SPP    | Star       |
| 76 SPP    | Super Star |
| 126 SPP   | Mega Star  |
| 176 SPP   | Legend     |

**Algorithm** (implemented in `compute_achievements()` in `main.py`):

1. Sum each player's SPP earned across all selected tournaments (`total_selected_spp`).
2. Compute a pre-tournament baseline: `running_spp = career_spp_total - total_selected_spp`.
3. Process tournaments oldest-first. For each tournament, check whether `running_spp` crosses
   any threshold after adding the player's SPP earned in that tournament:

```python
spp_before = running_spp[pid]
spp_after  = spp_before + spp_here
for threshold, label in SPP_THRESHOLDS:
    if spp_before < threshold <= spp_after:
        # award milestone
```

4. Advance `running_spp[pid] = spp_after` before moving to the next tournament.

A player can earn multiple milestones in the same tournament if they earn enough SPP to cross
more than one threshold. Each milestone is only ever awarded once (the running total ensures
a crossed threshold is never re-checked).

### API: Career SPP field paths

Career SPP is **not** available directly in match data and must be fetched separately.
The two sources used in `_gather_player_info()` have **different** JSON paths:

| Source | Endpoint | SPP field path |
|--------|----------|----------------|
| Team roster | `GET /team/get/{teamId}` | `players[].record.spp` |
| Individual player | `GET /player/get/{playerId}` | `statistics.spp` |

The team roster is fetched first (one call per unique team, covering all active roster members).
Players not found there fall back to the SQLite `player_cache`, then to individual player API calls.

**Bug history**: an earlier version used `p.get("spp")` and `pdata.get("spp")` (top-level), which
always returned `None` → 0, silently suppressing all SPP milestone achievements.

---

## Test Infrastructure

### Fixtures (`fixtures/<tournament_id>.json`)

Each fixture captures a snapshot of real FUMBBL data for a tournament:

- `tournament_id` / `tournament_name` / `season` — metadata
- `schedule` — raw response from `GET /tournament/schedule/{id}`
- `matches` — map of `match_id → raw response from GET /match/get/{id}?verbose=1` for every played match (verbose adds `performances` per player)
- `player_info` — map of `str(player_id) → {name, status}` fetched from `/player/get/{id}`
- `player_career_spp` — map of `str(player_id) → int` career SPP from `/player/get/{id}` → `statistics.spp`
- `expected_standings` — manually populated from the FUMBBL website standings page
- `expected_achievements` — manually populated; each entry is one of:
  - `{"achievement_type": "spp_milestone", "achievement_name": "Super Star", "player_name": "", "team_name": ""}`
  - `{"achievement_type": "tournament_award", "badges": ["SPP"], "player_name": "", "team_name": ""}`
  - `{"achievement_type": "per_game", "achievement_name": "Triple X", "player_name": "", "team_name": ""}`

Use `fetch_fixture.py <tournament_id>` to create a new fixture, then fill in `expected_standings`
and `expected_achievements`.

### Running Tests

```
python test_standings.py              # all fixtures
python test_standings.py <id>         # single fixture by tournament id

python test_achievements.py           # all fixtures
python test_achievements.py <id>      # single fixture by tournament id
```

- `[PASS]` — computed results match expected exactly
- `[FAIL]` — field-by-field diff shown (extras and missing listed separately)
- `[SKIP]` — no expected data provided; computed results printed for eyeballing
