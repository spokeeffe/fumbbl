# FUMBBL League Admin Tool

A FastAPI web app for managing FUMBBL leagues — retrieving data, computing standings, player stats and achievements.

## Setup

```bash
# 1. Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the development server
uvicorn main:app --reload
```

Then open http://localhost:8000 in your browser.

## Project Structure

```
fumbbl-admin/
├── main.py               # FastAPI app, routes, DB, API client
├── requirements.txt
├── fumbbl.db             # SQLite database (auto-created on first run)
├── templates/
│   ├── index.html        # League list + add form
│   └── league.html       # League detail page
└── static/
    ├── css/main.css
    └── js/main.js
```

## Features (Phase 1)
- Add leagues by Group ID + Ruleset ID
- Auto-fetches league name from FUMBBL API on save
- Lists all saved leagues
- Rate-limited API calls (max 2/sec)
- Input validation and graceful error handling
- SQLite persistence

## Roadmap
- Tournament retrieval and display
- Standings computation with tiebreakers
- Player stats (SPP, TDs, casualties, etc.)
- Player achievements
- CSV export
