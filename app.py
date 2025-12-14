import sqlite3
from datetime import datetime, date
import pandas as pd
import streamlit as st
from io import BytesIO
import requests

DB_PATH = "packing.db"

# ----------------------------
# SharePoint / Graph helpers
# ----------------------------
def has_sp_secrets() -> bool:
    keys = ["TENANT_ID", "CLIENT_ID", "CLIENT_SECRET", "SP_HOSTNAME", "SP_SITE_PATH", "SP_LIST_NAME"]
    return all(k in st.secrets and str(st.secrets.get(k, "")).strip() for k in keys)

def graph_get_token() -> str:
    tenant = st.secrets["TENANT_ID"]
    client_id = st.secrets["CLIENT_ID"]
    client_secret = st.secrets["CLIENT_SECRET"]

    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def graph_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_site_id(token: str) -> str:
    hostname = st.secrets["SP_HOSTNAME"].strip()
    site_path = st.secrets["SP_SITE_PATH"].strip()
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    r = requests.get(url, headers=graph_headers(token), timeout=30)
    r.raise_for_status()
    return r.json()["id"]

def get_list_id(token: str, site_id: str) -> str:
    list_name = st.secrets["SP_LIST_NAME"].strip()
    # filter by displayName
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$filter=displayName eq '{list_name}'"
    r = requests.get(url, headers=graph_headers(token), timeout=30)
    r.raise_for_status()
    data = r.json()
    if "value" not in data or len(data["value"]) == 0:
        raise ValueError(f"List not found: {list_name}")
    return data["value"][0]["id"]

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, str) and x.strip() == "":
            return float(default)
        return float(x)
    except:
        return float(default)

def sp_field_name(secret_key: str, fallback: str) -> str:
    # allow override via secrets
    v = str(st.secrets.get(secret_key, fallback)).strip()
    return v if v else fallback

def create_sp_list_item(token: str, site_id: str, list_id: str, rec: dict) -> int:
    # Column internal names (can be overridden in secrets)
    COL_TITLE = sp_field_name("SP_COL_TITLE", "Title")
    COL_WORKDATE = sp_field_name("SP_COL_WORKDATE", "WorkDate")
    COL_SHIFT = sp_field_name("SP_COL_SHIFT", "Shift")
    COL_LINE = sp_field_name("SP_COL_LINE", "Line")
    COL_PACKTYPE = sp_field_name("SP_COL_PACKTYPE", "PackType")
    COL_MINUTES = sp_field_name("SP_COL_MINUTES", "Minutes")
    COL_PEOPLE = sp_field_name("SP_COL_PEOPLE", "People")
    COL_FINISHED = sp_field_name("SP_COL_FINISHED", "FinishedPunnets")
    COL_WASTE = sp_field_name("SP_COL_WASTE", "WastePunnets")
    COL_DOWNTIME = sp_field_name("SP_COL_DOWNTIME", "DowntimeMinutes")
    COL_NOTE = sp_field_name("SP_COL_NOTE", "Note")

    work_date = rec["work_date"]  # 'YYYY-MM-DD'
    pack_type = rec.get("pack_type") or ""
    title = f"{work_date} - {pack_type}" if pack_type else f"{work_date}"

    fields = {
        COL_TITLE: title,
        COL_WORKDATE: work_date,  # date only
        COL_SHIFT: rec.get("shift") or "",
        COL_LINE: rec.get("line") or "",
        COL_PACKTYPE: rec.get("pack_type") or "",
        COL_MINUTES: safe_float(rec.get("minutes"), 0),
        COL_PEOPLE: safe_float(rec.get("people"), 0),
        COL_FINISHED: safe_float(rec.get("finished_punnets"), 0),
        COL_WASTE: safe_float(rec.get("waste_punnets"), 0),
        COL_DOWNTIME: safe_float(rec.get("downtime_minutes"), 0),
        COL_NOTE: rec.get("note") or "",
    }

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    body = {"fields": fields}
    r = requests.post(url, headers=graph_headers(token), json=body, timeout=30)
    r.raise_for_status()
    # returns created item with id
    return int(r.json()["id"])

# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def ensure_columns_exist(conn):
    # add sp_synced, sp_item_id if missing
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(records)")
    cols = [r[1] for r in cur.fetchall()]
    if "sp_synced" not in cols:
        cur.execute("ALTER TABLE records ADD COLUMN sp_synced INTEGER DEFAULT 0")
    if "sp_item_id" not in cols:
        cur.execute("ALTER TABLE records ADD COLUMN sp_item_id TEXT")
    conn.commit()

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

    ensure_columns_exist(conn)

    # default settings
    cur.execute("SELECT value FROM settings WHERE key='hourly_rate'")
    if cur.fetchone() is None:
        cur.execute("INSERT INTO settings(key, value) VALUES(?, ?)", ("hourly_rate", "0"))

    cur.execute("SELECT value FROM settings WHERE key='auto_sync_sp'")
    if cur.fetchone() is None:
        cur.execute("INSERT INTO settings(key, value) VALUES(?, ?)", ("auto_sync_sp", "0"))

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

def insert_record(r: dict) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO records(
        created_at, work_date, shift, line, pack_type,
        minutes, people, finished_punnets, waste_punnets, downtime_minutes, note,
        sp_synced, sp_item_id
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        r["created_at"], r["work_date"], r.get("shift"), r.get("line"), r.get("pack_type"),
        r["minutes"], r["people"], r["finished_punnets"], r.get("waste_punnets", 0),
        r.get("downtime_minutes", 0), r.get("note"),
        int(r.get("sp_synced", 0)),
        r.get("sp_item_id")
    ))
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(new_id)

def mark_synced(record_id: int, sp_item_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE records SET sp_synced=1, sp_item_id=? WHERE id=?", (str(sp_item_id), record_id))
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

def load_unsynced_records(limit: int = 200):
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT * FROM records
        WHERE COALESCE(sp_synced,0) = 0
        ORDER BY date(work_date) ASC, id ASC
        LIMIT ?
        """,
        conn,
        params=(limit,)
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

st.title("Packing input → Auto costing (per punnet) → Export Excel + (optional) Sync SharePoint")

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
st.sidebar.subheader("SharePoint Sync (optional)")

auto_sync = get_setting("auto_sync_sp", "0") == "1"
auto_sync_ui = st.sidebar.checkbox("Auto sync to SharePoint when saving", value=auto_sync, disabled=not has_sp_secrets())
set_setting("auto_sync_sp", "1" if auto_sync_ui else "0")

if not has_sp_secrets():
    st.sidebar.info("No SharePoint secrets found. Add secrets in Streamlit Cloud to enable sync.")

if has_sp_secrets():
    with st.sidebar.expander("Test SharePoint connection", expanded=False):
        if st.button("Test now"):
            try:
                token = graph_get_token()
                site_id = get_site_id(token)
                list_id = get_list_id(token, site_id)
                st.success("OK ✅")
                st.write("site_id:", site_id)
                st.write("list_id:", list_id)
            except Exception as e:
                st.error(f"Failed: {e}")

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
st.sidebar.caption("Tip: Finished punnets is manual input.")

tab1, tab2, tab3, tab4 = st.tabs(["New entry", "View & delete", "Export report", "Sync to SharePoint"])

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
                "note": note.strip() if note else None,
                "sp_synced": 0,
                "sp_item_id": None
            }

            # Save local first
            local_id = insert_record(rec)

            # optional auto sync
            synced_msg = ""
            if has_sp_secrets() and (get_setting("auto_sync_sp", "0") == "1"):
                try:
                    token = graph_get_token()
                    site_id = get_site_id(token)
                    list_id = get_list_id(token, site_id)
                    sp_item_id = create_sp_list_item(token, site_id, list_id, rec)
                    mark_synced(local_id, sp_item_id)
                    synced_msg = f" + SharePoint synced (item id {sp_item_id})"
                except Exception as e:
                    synced_msg = f" (SharePoint sync failed: {e})"

            # quick result
            labour_hours = (minutes * people) / 60.0
            pph = finished_punnets / labour_hours if labour_hours > 0 else 0
            lcpp = (labour_hours * float(get_setting("hourly_rate", "0"))) / finished_punnets if finished_punnets > 0 else 0

            st.success("Saved ✅" + synced_msg)
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
            "sp_synced", "sp_item_id",
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

# ----------------------------
# Tab 4: Sync unsynced records to SharePoint
# ----------------------------
with tab4:
    st.subheader("Sync local records → SharePoint")

    if not has_sp_secrets():
        st.warning("SharePoint secrets missing. Please add secrets first.")
    else:
        df_unsynced = load_unsynced_records(limit=200)

        if df_unsynced.empty:
            st.success("No unsynced records ✅")
        else:
            st.info(f"Unsynced records: {len(df_unsynced)} (showing up to 200)")
            st.dataframe(df_unsynced[[
                "id", "work_date", "shift", "line", "pack_type",
                "minutes", "people", "finished_punnets", "waste_punnets", "downtime_minutes", "note",
                "sp_synced", "sp_item_id"
            ]], use_container_width=True)

            colA, colB = st.columns([1, 2])
            with colA:
                sync_limit = st.number_input("Sync how many now", min_value=1, max_value=int(len(df_unsynced)), value=min(20, int(len(df_unsynced))), step=1)
            with colB:
                st.caption("It will create new items in SharePoint for these local records.")

            if st.button("Sync now"):
                try:
                    token = graph_get_token()
                    site_id = get_site_id(token)
                    list_id = get_list_id(token, site_id)

                    ok = 0
                    fail = 0
                    errs = []

                    for _, row in df_unsynced.head(int(sync_limit)).iterrows():
                        rec = row.to_dict()
                        # ensure required fields
                        rec["work_date"] = str(rec["work_date"])[:10]
                        try:
                            sp_item_id = create_sp_list_item(token, site_id, list_id, rec)
                            mark_synced(int(rec["id"]), sp_item_id)
                            ok += 1
                        except Exception as e:
                            fail += 1
                            errs.append(f"ID {int(rec['id'])}: {e}")

                    st.success(f"Done. OK={ok}, Failed={fail}")
                    if errs:
                        st.warning("Some failed:")
                        st.code("\n".join(errs))
                    st.rerun()

                except Exception as e:
                    st.error(f"Sync failed: {e}")
