from datetime import datetime, date
import pandas as pd
import streamlit as st
from io import BytesIO
import requests
import time

# ============================
# SharePoint column internal names
# (必须跟你 List 的 internal name 一样)
# ============================
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

# ============================
# Secrets helpers
# ============================
def secrets_get(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default

# ============================
# Graph auth
# ============================
def graph_get_token() -> str:
    tenant = secrets_get("TENANT_ID", "")
    client_id = secrets_get("CLIENT_ID", "")
    client_secret = secrets_get("CLIENT_SECRET", "")

    if not tenant or not client_id or not client_secret:
        raise Exception("Missing secrets: TENANT_ID / CLIENT_ID / CLIENT_SECRET")

    cache = st.session_state.get("_graph_token_cache", {})
    now = int(time.time())
    if cache and cache.get("access_token") and cache.get("expires_at", 0) > now + 60:
        return cache["access_token"]

    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise Exception(f"Token request failed: {r.status_code} {r.text}")

    js = r.json()
    token = js.get("access_token", "")
    if not token:
        raise Exception(f"Token is empty. Raw response: {r.text}")

    expires_in = int(js.get("expires_in", 3600))
    st.session_state["_graph_token_cache"] = {"access_token": token, "expires_at": now + expires_in}
    return token

def graph_headers() -> dict:
    token = graph_get_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ============================
# Graph: site / list
# ============================
def graph_get_site_id(host: str, site_path: str) -> str:
    # 注意：site_path 要以 /sites/... 开头
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get site failed: {r.status_code} {r.text}")
    site_id = r.json().get("id", "")
    if not site_id:
        raise Exception(f"Site id empty. Raw: {r.text}")
    return site_id

def graph_get_list_id(site_id: str, list_name: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$top=200"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get lists failed: {r.status_code} {r.text}")

    for it in r.json().get("value", []):
        if it.get("displayName") == list_name:
            list_id = it.get("id", "")
            if list_id:
                return list_id
    raise Exception(f"List not found: {list_name}")

def get_site_and_list_ids():
    host = secrets_get("SP_HOST", "")
    site_path = secrets_get("SP_SITE_PATH", "")
    list_name = secrets_get("SP_LIST_NAME", "")

    if not host or not site_path or not list_name:
        raise Exception("Missing secrets: SP_HOST / SP_SITE_PATH / SP_LIST_NAME")

    site_id = st.session_state.get("_sp_site_id")
    list_id = st.session_state.get("_sp_list_id")

    if not site_id:
        site_id = graph_get_site_id(host, site_path)
        st.session_state["_sp_site_id"] = site_id

    if not list_id:
        list_id = graph_get_list_id(site_id, list_name)
        st.session_state["_sp_list_id"] = list_id

    return site_id, list_id

# ============================
# Graph: read list items (auto paging)
# ============================
def graph_list_items_all(site_id: str, list_id: str, top: int = 2000) -> list[dict]:
    # expand fields 才拿得到你自定义列
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top={top}"
    out = []
    while url:
        r = requests.get(url, headers=graph_headers(), timeout=30)
        if r.status_code != 200:
            raise Exception(f"Get items failed: {r.status_code} {r.text}")
        js = r.json()
        out.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return out

# ============================
# Convert fields -> dataframe row
# ============================
def _to_float(v, default=0.0):
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)

def _to_text(v):
    if v is None:
        return ""
    return str(v)

def _parse_work_date(v):
    # SharePoint/Graph 可能给：
    # "2025-12-14T00:00:00Z" or "2025-12-14" or "12/14/2025"
    if not v:
        return None
    s = str(v).strip()
    try:
        # ISO
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        # yyyy-mm-dd
        if "-" in s and len(s) >= 10:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        # mm/dd/yyyy
        if "/" in s:
            return datetime.strptime(s, "%m/%d/%Y").date()
    except Exception:
        return None
    return None

def sp_fetch_records_df(start: date, end: date) -> pd.DataFrame:
    site_id, list_id = get_site_and_list_ids()
    items = graph_list_items_all(site_id, list_id, top=2000)

    rows = []
    for it in items:
        fields = (it.get("fields") or {})
        wd = _parse_work_date(fields.get(SP_COL_WORKDATE))
        if not wd:
            continue
        if wd < start or wd > end:
            continue

        rows.append({
            "sp_item_id": it.get("id"),
            "created_at": _to_text(fields.get("Created")),  # 系统字段可能有，也可能没有
            "work_date": wd.isoformat(),
            "shift": _to_text(fields.get(SP_COL_SHIFT)),
            "line": _to_text(fields.get(SP_COL_LINE)),
            "pack_type": _to_text(fields.get(SP_COL_PACKTYPE)),
            "minutes": _to_float(fields.get(SP_COL_MINUTES), 0),
            "people": _to_float(fields.get(SP_COL_PEOPLE), 0),
            "finished_punnets": _to_float(fields.get(SP_COL_FINISHED), 0),
            "waste_punnets": _to_float(fields.get(SP_COL_WASTE), 0),
            "downtime_minutes": _to_float(fields.get(SP_COL_DOWNTIME), 0),
            "note": _to_text(fields.get(SP_COL_NOTE)),
            "title": _to_text(fields.get(SP_COL_TITLE)),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["work_date"] = pd.to_datetime(df["work_date"]).dt.date.astype(str)
    # sort
    df = df.sort_values(["work_date", "sp_item_id"], ascending=[False, False])
    return df

# ============================
# Calculations
# ============================
def add_calculated_cols(df: pd.DataFrame, hourly_rate: float) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["minutes"] = df["minutes"].astype(float)
    df["people"] = df["people"].astype(float)
    df["finished_punnets"] = df["finished_punnets"].astype(float)
    df["waste_punnets"] = df["waste_punnets"].astype(float)

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

# ============================
# UI
# ============================
st.set_page_config(page_title="Packing Auto Costing (SharePoint)", layout="wide")
st.title("Packing (Form → SharePoint) → Auto costing (per punnet) → Export Excel")

st.sidebar.header("Settings")
hourly_rate = st.sidebar.number_input("Hourly rate ($/hour)", min_value=0.0, value=0.0, step=0.5)

with st.sidebar.expander("Test SharePoint connection"):
    if st.button("Test Graph token + site + list"):
        try:
            token = graph_get_token()
            st.success(f"Token OK (len={len(token)})")

            site_id, list_id = get_site_and_list_ids()
            st.success(f"Site OK: {site_id[:25]}...")
            st.success(f"List OK: {list_id[:25]}...")
        except Exception as e:
            st.error(str(e))

tab1, tab2 = st.tabs(["View (from SharePoint)", "Export report"])

# ---- Tab1: View ----
with tab1:
    st.subheader("View records (SharePoint list)")
    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("Start date", value=date.today().replace(day=1))
    with c2:
        end = st.date_input("End date", value=date.today())

    if st.button("Refresh from SharePoint"):
        st.session_state.pop("_sp_cache_df", None)

    # simple cache (避免每次重刷都打 Graph)
    cache_key = f"{start.isoformat()}_{end.isoformat()}"
    cached = st.session_state.get("_sp_cache_df")
    cached_key = st.session_state.get("_sp_cache_key")
    if cached is None or cached_key != cache_key:
        try:
            df = sp_fetch_records_df(start, end)
            st.session_state["_sp_cache_df"] = df
            st.session_state["_sp_cache_key"] = cache_key
        except Exception as e:
            st.error(str(e))
            df = pd.DataFrame()
    else:
        df = cached

    df_calc = add_calculated_cols(df, hourly_rate)

    if df_calc.empty:
        st.info("No data in this date range (from SharePoint).")
        st.write("Tip: 先去 SharePoint List 确认 WorkDate/Minutes/People 都有值，而且 WorkDate 在你选的范围内。")
    else:
        show_cols = [
            "sp_item_id", "work_date", "shift", "line", "pack_type",
            "minutes", "people", "finished_punnets", "waste_punnets", "downtime_minutes",
            "labour_hours", "punnets_per_labour_hour", "labour_cost_per_punnet",
            "note", "title"
        ]
        st.dataframe(df_calc[show_cols], use_container_width=True)

# ---- Tab2: Export ----
with tab2:
    st.subheader("Export Excel report (SharePoint list)")
    c1, c2 = st.columns(2)
    with c1:
        ex_start = st.date_input("Export start date", value=date.today().replace(day=1), key="ex_start")
    with c2:
        ex_end = st.date_input("Export end date", value=date.today(), key="ex_end")

    try:
        df_raw = sp_fetch_records_df(ex_start, ex_end)
    except Exception as e:
        st.error(str(e))
        df_raw = pd.DataFrame()

    if df_raw.empty:
        st.info("No data to export.")
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
