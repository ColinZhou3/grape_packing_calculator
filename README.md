# Packing Costing (per punnet) - Streamlit

This is a small Streamlit app for warehouse packing data entry and auto costing (labour cost per punnet).

## What it does
- Staff input: date, minutes, people, finished punnets (manual), optional waste/downtime/note
- Auto calculates:
  - labour hours = minutes * people / 60
  - punnets per labour hour
  - labour cost per punnet = labour hours * hourly_rate / finished_punnets
- Saves to local SQLite file: `packing.db`
- Export Excel report with 2 sheets: `Raw_Log` and `Summary`

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy on Streamlit Community Cloud
1. Push this repo to GitHub (public repo is easiest)
2. Go to Streamlit Community Cloud and choose the repo
3. Main file: `app.py`

### Note about data persistence on Streamlit Cloud
Streamlit Community Cloud can reset the local filesystem sometimes (redeploy / app restart).
So the `packing.db` may not be 100% permanent there.

For stable storage (still low/no cost), you can later switch to:
- Google Sheet as database, or
- a free-tier hosted DB (Supabase / etc)

If you want, tell me which one you prefer, I can update the code.
