import sqlite3
from datetime import datetime, date
import pandas as pd
import streamlit as st
from io import BytesIO

DB_PATH = "packing.db"

# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pack_types(
        name TEXT PRIMARY KEY
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS records(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        work_date TEXT NOT NULL,
        shift TEXT,
        line TEXT,
        pack_type TEXT,
        minutes REAL NOT NULL,
        people REAL NOT NULL,
        finished_punnets REAL NOT NULL,
        waste_punnets REAL DEFAULT 0,
        downtime_minutes REAL DEFAULT 0,
        note TEXT
    )
    """)

    # default settings
    cur.execute("SELECT value FROM settings WHERE key='hourly_rate'")
    if cur.fetchone() is None:
        cur.execute("INSERT INTO settings(key, value) VALUES(?, ?)", ("hourly_rate", "0"))

    # some default pack types (optional)
    cur.execute("SELECT COUNT(*) FROM pack_types")
    if cur.fetchone()[0] == 0:
        defaults = ["500g x 20", "200g x 30", "100g x 52"]
        cur.executemany("INSERT OR IGNORE INTO pack_types(name) VALUES(?)", [(x,) for x in defaults])

    conn.commit()
    conn.close()

def get_setting(key: str, default=""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default

def set_setting(key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_pack_types():
    conn = get_conn()
    df = pd.read_sql_query("SELECT name FROM pack_types ORDER BY name", conn)
    conn.close()
    return df["name"].tolist()

def add_pack_type(name: str):
    name = (name or "").strip()
    if not name:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO pack_types(name) VALUES(?)", (name,))
    conn.commit()
    conn.close()

def delete_pack_type(name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM pack_types WHERE name=?", (name,))
    conn.commit()
    conn.close()

def insert_record(r: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO records(
        created_at, work_date, shift, line, pack_type,
        minutes, people, finished_punnets, waste_punnets, downtime_minutes, note
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (
        r["created_at"], r["work_date"], r.get("shift"), r.get("line"), r.get("pack_type"),
        r["minutes"], r["people"], r["finished_punnets"], r.get("waste_punnets", 0),
        r.get("downtime_minutes", 0), r.get("note")
    ))
    conn.commit()
    conn.close()

def load_records(start: date, end: date):
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT * FROM records
        WHERE date(work_date) BETWEEN date(?) AND date(?)
        ORDER BY date(work_date) DESC, id DESC
        """,
        conn,
        params=(start.isoformat(), end.isoformat())
    )
    conn.close()
    return df

def delete_record(record_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE id=?", (record_id,))
    conn.commit()
    conn.close()

# ----------------------------
# Calculations
# ----------------------------
def add_calculated_cols(df: pd.DataFrame, hourly_rate: float) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["labour_hours"] = (df["minutes"] * df["people"]) / 60.0

    df["punnets_per_labour_hour"] = df.apply(
        lambda x: (x["finished_punnets"] / x["labour_hours"]) if x["labour_hours"] > 0 else 0,
        axis=1
    )
    df["labour_cost_per_punnet"] = df.apply(
        lambda x: ((x["labour_hours"] * hourly_rate) / x["finished_punnets"]) if x["finished_punnets"] > 0 else 0,
        axis=1
    )

    df["waste_rate"] = df.apply(
        lambda x: (x["waste_punnets"] / (x["finished_punnets"] + x["waste_punnets"]))
        if (x["finished_punnets"] + x["waste_punnets"]) > 0 else 0,
        axis=1
    )
    return df

def make_summary(df: pd.DataFrame, hourly_rate: float) -> pd.DataFrame:
    if df.empty:
        return df

    df2 = df.copy()
    df2["work_date"] = pd.to_datetime(df2["work_date"]).dt.date

    gcols = ["work_date", "pack_type"]
    out = df2.groupby(gcols, dropna=False).agg(
        minutes=("minutes", "sum"),
        people=("people", "sum"),
        finished_punnets=("finished_punnets", "sum"),
        waste_punnets=("waste_punnets", "sum"),
        downtime_minutes=("downtime_minutes", "sum"),
    ).reset_index()

    df2["labour_hours"] = (df2["minutes"] * df2["people"]) / 60.0
    lh = df2.groupby(gcols, dropna=False)["labour_hours"].sum().reset_index(name="labour_hours")
    out = out.merge(lh, on=gcols, how="left")

    out["punnets_per_labour_hour"] = out.apply(
        lambda x: (x["finished_punnets"] / x["labour_hours"]) if x["labour_hours"] > 0 else 0, axis=1
    )
    out["labour_cost_per_punnet"] = out.apply(
        lambda x: ((x["labour_hours"] * hourly_rate) / x["finished_punnets"]) if x["finished_punnets"] > 0 else 0,
        axis=1
    )
    out["waste_rate"] = out.apply(
        lambda x: (x["waste_punnets"] / (x["finished_punnets"] + x["waste_punnets"]))
        if (x["finished_punnets"] + x["waste_punnets"]) > 0 else 0,
        axis=1
    )

    out = out.sort_values(["work_date", "pack_type"], ascending=[False, True])
    return out

def to_excel_bytes(raw_df: pd.DataFrame, summary_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        raw_df.to_excel(writer, index=False, sheet_name="Raw_Log")
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
    return output.getvalue()

# ----------------------------
# App
# ----------------------------
st.set_page_config(page_title="Packing Costing (per punnet)", layout="wide")
init_db()

st.title("Packing input → Auto costing (per punnet) → Export Excel")

# Sidebar settings
st.sidebar.header("Settings")
try:
    hourly_rate = float(get_setting("hourly_rate", "0"))
except:
    hourly_rate = 0.0

new_rate = st.sidebar.number_input("Hourly rate ($/hour)", min_value=0.0, value=float(hourly_rate), step=0.5)
if st.sidebar.button("Save hourly rate"):
    set_setting("hourly_rate", str(new_rate))
    st.sidebar.success("Saved")

st.sidebar.divider()
st.sidebar.subheader("Pack Types (optional)")
pack_types = get_pack_types()
new_pack = st.sidebar.text_input("Add pack type", placeholder="e.g. 500g x 20")
if st.sidebar.button("Add pack type"):
    add_pack_type(new_pack)
    st.sidebar.success("Added (if not exists)")
    st.rerun()

if pack_types:
    del_pack = st.sidebar.selectbox("Delete pack type", ["(none)"] + pack_types)
    if st.sidebar.button("Delete selected"):
        if del_pack != "(none)":
            delete_pack_type(del_pack)
            st.sidebar.success("Deleted")
            st.rerun()

st.sidebar.divider()
st.sidebar.caption("Tip: Finished punnets is manual input (no need to keep punnet-per-crate updated).")

tab1, tab2, tab3 = st.tabs(["New entry", "View & delete", "Export report"])

# ----------------------------
# Tab 1: New entry
# ----------------------------
with tab1:
    st.subheader("New packing record")

    pack_types = get_pack_types()
    pack_choice_list = ["(blank)"] + pack_types

    with st.form("new_record_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            work_date = st.date_input("Work date", value=date.today())
            shift = st.text_input("Shift (optional)", placeholder="e.g. AM / PM")
        with col2:
            line = st.text_input("Line (optional)", placeholder="e.g. Line 1")
            pack_type = st.selectbox("Pack type (optional)", pack_choice_list)
        with col3:
            minutes = st.number_input("Minutes", min_value=0.0, value=0.0, step=1.0)
            people = st.number_input("People", min_value=0.0, value=0.0, step=0.5)

        col4, col5, col6 = st.columns(3)
        with col4:
            finished_punnets = st.number_input("Finished punnets", min_value=0.0, value=0.0, step=1.0)
        with col5:
            waste_punnets = st.number_input("Waste punnets (optional)", min_value=0.0, value=0.0, step=1.0)
        with col6:
            downtime_minutes = st.number_input("Downtime minutes (optional)", min_value=0.0, value=0.0, step=1.0)

        note = st.text_area("Note (optional)", placeholder="e.g. changeover, fruit soft, waiting pallet...")

        submitted = st.form_submit_button("Save record")

    if submitted:
        if minutes <= 0 or people <= 0:
            st.error("Minutes and People must be > 0.")
        elif finished_punnets <= 0:
            st.error("Finished punnets must be > 0 (for costing).")
        else:
            rec = {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "work_date": work_date.isoformat(),
                "shift": shift.strip() if shift else None,
                "line": line.strip() if line else None,
                "pack_type": None if pack_type == "(blank)" else pack_type,
                "minutes": float(minutes),
                "people": float(people),
                "finished_punnets": float(finished_punnets),
                "waste_punnets": float(waste_punnets),
                "downtime_minutes": float(downtime_minutes),
                "note": note.strip() if note else None
            }
            insert_record(rec)

            # quick result
            labour_hours = (minutes * people) / 60.0
            pph = finished_punnets / labour_hours if labour_hours > 0 else 0
            lcpp = (labour_hours * float(get_setting("hourly_rate", "0"))) / finished_punnets if finished_punnets > 0 else 0

            st.success("Saved ✅")
            st.write("Quick result:")
            st.metric("Labour hours", f"{labour_hours:.2f}")
            st.metric("Punnets per labour hour", f"{pph:.1f}")
            st.metric("Labour cost per punnet", f"${lcpp:.4f}")

# ----------------------------
# Tab 2: View & delete
# ----------------------------
with tab2:
    st.subheader("View records")
    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("Start date", value=date.today().replace(day=1))
    with c2:
        end = st.date_input("End date", value=date.today())

    df = load_records(start, end)
    df_calc = add_calculated_cols(df, float(get_setting("hourly_rate", "0")))

    if df_calc.empty:
        st.info("No data in this date range.")
    else:
        show_cols = [
            "id", "work_date", "shift", "line", "pack_type",
            "minutes", "people", "finished_punnets", "waste_punnets", "downtime_minutes",
            "labour_hours", "punnets_per_labour_hour", "labour_cost_per_punnet",
            "note", "created_at"
        ]
        st.dataframe(df_calc[show_cols], use_container_width=True)

        st.divider()
        st.write("Delete a record (careful):")
        del_id = st.number_input("Record ID to delete", min_value=0, value=0, step=1)
        if st.button("Delete"):
            if del_id > 0:
                delete_record(int(del_id))
                st.success(f"Deleted ID {int(del_id)}")
                st.rerun()
            else:
                st.warning("Please input a valid ID (>0).")

# ----------------------------
# Tab 3: Export
# ----------------------------
with tab3:
    st.subheader("Export Excel report")
    c1, c2 = st.columns(2)
    with c1:
        ex_start = st.date_input("Export start date", value=date.today().replace(day=1), key="ex_start")
    with c2:
        ex_end = st.date_input("Export end date", value=date.today(), key="ex_end")

    hourly_rate_now = float(get_setting("hourly_rate", "0"))
    df_raw = load_records(ex_start, ex_end)
    df_raw_calc = add_calculated_cols(df_raw, hourly_rate_now)
    df_summary = make_summary(df_raw, hourly_rate_now)

    if df_raw.empty:
        st.info("No data to export.")
    else:
        st.write("Preview (Summary):")
        st.dataframe(df_summary, use_container_width=True)

        file_bytes = to_excel_bytes(df_raw_calc, df_summary)
        fname = f"packing_report_{ex_start.isoformat()}_to_{ex_end.isoformat()}.xlsx"
        st.download_button(
            label="Download Excel report",
            data=file_bytes,
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
