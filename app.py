import os
import time
import requests
import pandas as pd
import streamlit as st
from datetime import date, datetime
from io import BytesIO

# =========================
# Your SharePoint config
# =========================
SP_HOSTNAME = "healthyfresh.sharepoint.com"
SP_SITE_PATH = "/sites/Packing"
SP_LIST_NAME = "PackingRecords"

SP_COL_TITLE = "Title"
SP_COL_WORKDATE = "WorkDate"
SP_COL_SHIFT = "Shift"
SP_COL_LINE = "Line"
SP_COL_PACKTYPE = "PackType"
SP_COL_MINUTES = "Minutes"
SP_COL_PEOPLE = "People"
SP_COL_FINISHED = "FinishedPunnets"
SP_COL_WASTE = "WastePunnets"
SP_COL_DOWNTIME = "DowntimeMinutes"
SP_COL_NOTE = "Note"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


# =========================
# Helpers
# =========================
def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


@st.cache_data(show_spinner=False)
def get_access_token_cached(tenant_id: str, client_id: str, client_secret: str) -> dict:
    """
    Return dict:
      { "access_token": "...", "expires_at": 1234567890 }
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(token_url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Token request failed: {r.status_code} {r.text}")

    js = r.json()
    access_token = js.get("access_token", "")
    expires_in = int(js.get("expires_in", 3599))
    return {"access_token": access_token, "expires_at": int(time.time()) + expires_in - 60}


def get_token() -> str:
    tenant_id = must_env("AZURE_TENANT_ID")
    client_id = must_env("AZURE_CLIENT_ID")
    client_secret = must_env("AZURE_CLIENT_SECRET")

    tok = get_access_token_cached(tenant_id, client_id, client_secret)
    if not tok.get("access_token"):
        raise RuntimeError("Access token is empty. Check your client secret VALUE.")
    return tok["access_token"]


def graph_headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}


@st.cache_data(show_spinner=False)
def get_site_id(hostname: str, site_path: str) -> str:
    # Correct Graph format:
    # GET /sites/{hostname}:{site-path}
    url = f"{GRAPH_BASE}/sites/{hostname}:{site_path}"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Get site failed: {r.status_code} {r.text}")
    return r.json()["id"]


@st.cache_data(show_spinner=False)
def get_list_id(site_id: str, list_name: str) -> str:
    # Find list by displayName
    url = f"{GRAPH_BASE}/sites/{site_id}/lists?$select=id,displayName"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Get lists failed: {r.status_code} {r.text}")

    lists = r.json().get("value", [])
    for it in lists:
        if it.get("displayName") == list_name:
            return it["id"]

    # If not found, show what exists
    names = [x.get("displayName") for x in lists]
    raise RuntimeError(f"List '{list_name}' not found. Existing lists: {names}")


def to_number(x, default=0.0):
    if x is None:
        return default
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return default
    try:
        return float(s)
    except:
        return default


def safe_text(x):
    s = "" if x is None else str(x).strip()
    return s


def build_title(work_date: date, pack_type: str, shift: str, line: str) -> str:
    # Title is usually required in SharePoint list, so we always set it.
    pt = pack_type if pack_type else "Packing"
    sh = shift if shift else ""
    ln = line if line else ""
    return f"{work_date.isoformat()} {pt} {sh} {ln}".strip()


def create_list_item(site_id: str, list_id: str, fields: dict) -> dict:
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items"
    payload = {"fields": fields}
    r = requests.post(url, headers=graph_headers(), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Create item failed: {r.status_code} {r.text}")
    return r.json()


def get_all_items(site_id: str, list_id: str, top=200) -> list:
    # Get items with fields expanded
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items?expand=fields&$top={top}"
    out = []
    while url:
        r = requests.get(url, headers=graph_headers(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"List items failed: {r.status_code} {r.text}")
        js = r.json()
        out.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return out


def items_to_df(items: list) -> pd.DataFrame:
    rows = []
    for it in items:
        f = it.get("fields", {}) or {}
        rows.append({
            "ItemId": it.get("id"),
            SP_COL_TITLE: f.get(SP_COL_TITLE),
            SP_COL_WORKDATE: f.get(SP_COL_WORKDATE),
            SP_COL_SHIFT: f.get(SP_COL_SHIFT),
            SP_COL_LINE: f.get(SP_COL_LINE),
            SP_COL_PACKTYPE: f.get(SP_COL_PACKTYPE),
            SP_COL_MINUTES: f.get(SP_COL_MINUTES),
            SP_COL_PEOPLE: f.get(SP_COL_PEOPLE),
            SP_COL_FINISHED: f.get(SP_COL_FINISHED),
            SP_COL_WASTE: f.get(SP_COL_WASTE),
            SP_COL_DOWNTIME: f.get(SP_COL_DOWNTIME),
            SP_COL_NOTE: f.get(SP_COL_NOTE),
        })
    df = pd.DataFrame(rows)
    if not df.empty and SP_COL_WORKDATE in df.columns:
        df[SP_COL_WORKDATE] = pd.to_datetime(df[SP_COL_WORKDATE], errors="coerce").dt.date
    return df


def add_calculated_cols(df: pd.DataFrame, hourly_rate: float) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    # Convert numbers
    for c in [SP_COL_MINUTES, SP_COL_PEOPLE, SP_COL_FINISHED, SP_COL_WASTE, SP_COL_DOWNTIME]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["labour_hours"] = (df[SP_COL_MINUTES] * df[SP_COL_PEOPLE]) / 60.0
    df["punnets_per_labour_hour"] = df.apply(
        lambda x: (x[SP_COL_FINISHED] / x["labour_hours"]) if x["labour_hours"] > 0 else 0,
        axis=1
    )
    df["labour_cost_per_punnet"] = df.apply(
        lambda x: ((x["labour_hours"] * hourly_rate) / x[SP_COL_FINISHED]) if x[SP_COL_FINISHED] > 0 else 0,
        axis=1
    )
    df["waste_rate"] = df.apply(
        lambda x: (x[SP_COL_WASTE] / (x[SP_COL_FINISHED] + x[SP_COL_WASTE]))
        if (x[SP_COL_FINISHED] + x[SP_COL_WASTE]) > 0 else 0,
        axis=1
    )
    return df


def make_summary(df: pd.DataFrame, hourly_rate: float) -> pd.DataFrame:
    if df.empty:
        return df

    df2 = df.copy()
    gcols = [SP_COL_WORKDATE, SP_COL_PACKTYPE]
    out = df2.groupby(gcols, dropna=False).agg(
        minutes=(SP_COL_MINUTES, "sum"),
        people=(SP_COL_PEOPLE, "sum"),
        finished_punnets=(SP_COL_FINISHED, "sum"),
        waste_punnets=(SP_COL_WASTE, "sum"),
        downtime_minutes=(SP_COL_DOWNTIME, "sum"),
    ).reset_index()

    df2["labour_hours"] = (df2[SP_COL_MINUTES] * df2[SP_COL_PEOPLE]) / 60.0
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

    out = out.sort_values([SP_COL_WORKDATE, SP_COL_PACKTYPE], ascending=[False, True])
    return out


def to_excel_bytes(raw_df: pd.DataFrame, summary_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        raw_df.to_excel(writer, index=False, sheet_name="Raw_Log")
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
    return output.getvalue()


# =========================
# UI
# =========================
st.set_page_config(page_title="Packing → SharePoint", layout="wide")
st.title("Packing input → Save to SharePoint → Export Excel")

st.sidebar.header("Connection test")

try:
    site_id = get_site_id(SP_HOSTNAME, SP_SITE_PATH)
    list_id = get_list_id(site_id, SP_LIST_NAME)
    st.sidebar.success("Graph token OK + Site OK + List OK")
    st.sidebar.write("Site ID:", site_id)
    st.sidebar.write("List ID:", list_id)
except Exception as e:
    st.sidebar.error(str(e))
    st.stop()

st.sidebar.divider()
hourly_rate = st.sidebar.number_input("Hourly rate ($/hour)", min_value=0.0, value=0.0, step=0.5)

tab1, tab2, tab3 = st.tabs(["New entry", "View", "Export"])


# -------------------------
# Tab 1: New entry
# -------------------------
with tab1:
    st.subheader("New packing record (save to SharePoint)")

    with st.form("new_record", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            work_date = st.date_input("Work date", value=date.today())
            shift = st.text_input("Shift (optional)", placeholder="AM / PM")
        with c2:
            line = st.text_input("Line (optional)", placeholder="Line 1")
            pack_type = st.text_input("Pack type (optional)", placeholder="500g x 20")
        with c3:
            minutes = st.number_input("Minutes", min_value=0.0, value=0.0, step=1.0)
            people = st.number_input("People", min_value=0.0, value=0.0, step=0.5)

        c4, c5, c6 = st.columns(3)
        with c4:
            finished = st.number_input("Finished punnets", min_value=0.0, value=0.0, step=1.0)
        with c5:
            waste = st.number_input("Waste punnets (optional)", min_value=0.0, value=0.0, step=1.0)
        with c6:
            downtime = st.number_input("Downtime minutes (optional)", min_value=0.0, value=0.0, step=1.0)

        note = st.text_area("Note (optional)", placeholder="changeover, fruit soft, waiting pallet...")

        ok = st.form_submit_button("Save to SharePoint")

    if ok:
        if minutes <= 0 or people <= 0:
            st.error("Minutes and People must be > 0.")
        elif finished <= 0:
            st.error("Finished punnets must be > 0.")
        else:
            # IMPORTANT: For number columns, never send "" (empty string), send 0 or omit
            fields = {}

            title_val = build_title(work_date, safe_text(pack_type), safe_text(shift), safe_text(line))
            fields[SP_COL_TITLE] = title_val

            # date column: send YYYY-MM-DD
            fields[SP_COL_WORKDATE] = work_date.isoformat()

            if safe_text(shift):
                fields[SP_COL_SHIFT] = safe_text(shift)
            if safe_text(line):
                fields[SP_COL_LINE] = safe_text(line)
            if safe_text(pack_type):
                fields[SP_COL_PACKTYPE] = safe_text(pack_type)
            if safe_text(note):
                fields[SP_COL_NOTE] = safe_text(note)

            fields[SP_COL_MINUTES] = to_number(minutes, 0)
            fields[SP_COL_PEOPLE] = to_number(people, 0)
            fields[SP_COL_FINISHED] = to_number(finished, 0)

            # optional numbers: send 0 if empty
            fields[SP_COL_WASTE] = to_number(waste, 0)
            fields[SP_COL_DOWNTIME] = to_number(downtime, 0)

            try:
                create_list_item(site_id, list_id, fields)
                st.success("Saved ✅ (SharePoint item created)")

                labour_hours = (minutes * people) / 60.0
                pph = finished / labour_hours if labour_hours > 0 else 0
                lcpp = (labour_hours * hourly_rate) / finished if finished > 0 else 0

                st.write("Quick result:")
                st.metric("Labour hours", f"{labour_hours:.2f}")
                st.metric("Punnets per labour hour", f"{pph:.1f}")
                st.metric("Labour cost per punnet", f"${lcpp:.4f}")
            except Exception as e:
                st.error(str(e))


# -------------------------
# Tab 2: View
# -------------------------
with tab2:
    st.subheader("View records (read from SharePoint)")
    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("Start date", value=date.today().replace(day=1))
    with c2:
        end = st.date_input("End date", value=date.today())

    try:
        items = get_all_items(site_id, list_id, top=200)
        df = items_to_df(items)

        if df.empty:
            st.info("No items yet.")
        else:
            # filter locally (simple)
            df = df[df[SP_COL_WORKDATE].between(start, end)]
            df_calc = add_calculated_cols(df, hourly_rate)

            show_cols = [
                "ItemId",
                SP_COL_WORKDATE, SP_COL_SHIFT, SP_COL_LINE, SP_COL_PACKTYPE,
                SP_COL_MINUTES, SP_COL_PEOPLE, SP_COL_FINISHED, SP_COL_WASTE, SP_COL_DOWNTIME,
                "labour_hours", "punnets_per_labour_hour", "labour_cost_per_punnet",
                SP_COL_NOTE, SP_COL_TITLE
            ]
            show_cols = [c for c in show_cols if c in df_calc.columns]
            st.dataframe(df_calc[show_cols].sort_values(SP_COL_WORKDATE, ascending=False), use_container_width=True)

            st.caption("如果你想要删除 item，我也可以帮你加 Delete 功能（需要 Graph delete item endpoint）。")
    except Exception as e:
        st.error(str(e))


# -------------------------
# Tab 3: Export
# -------------------------
with tab3:
    st.subheader("Export Excel report")
    c1, c2 = st.columns(2)
    with c1:
        ex_start = st.date_input("Export start date", value=date.today().replace(day=1), key="ex_start")
    with c2:
        ex_end = st.date_input("Export end date", value=date.today(), key="ex_end")

    try:
        items = get_all_items(site_id, list_id, top=500)
        df_raw = items_to_df(items)

        if df_raw.empty:
            st.info("No data to export.")
        else:
            df_raw = df_raw[df_raw[SP_COL_WORKDATE].between(ex_start, ex_end)]
            if df_raw.empty:
                st.info("No data in this date range.")
            else:
                df_raw_calc = add_calculated_cols(df_raw, hourly_rate)
                df_summary = make_summary(df_raw, hourly_rate)

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
    except Exception as e:
        st.error(str(e))
