from datetime import datetime, date
import pandas as pd
import streamlit as st
import requests
import time
from io import BytesIO

def secrets_get(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default

# =========================
# Graph auth
# =========================
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
        raise Exception("Token empty")
    expires_in = int(js.get("expires_in", 3600))
    st.session_state["_graph_token_cache"] = {"access_token": token, "expires_at": now + expires_in}
    return token

def graph_headers():
    return {"Authorization": f"Bearer {graph_get_token()}", "Content-Type": "application/json"}

def graph_get_site_id(host: str, site_path: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get site failed: {r.status_code} {r.text}")
    return r.json().get("id", "")

def get_site_id():
    host = secrets_get("SP_HOST", "")
    site_path = secrets_get("SP_SITE_PATH", "")
    if not host or not site_path:
        raise Exception("Missing secrets: SP_HOST / SP_SITE_PATH")
    sid = st.session_state.get("_sp_site_id")
    if not sid:
        sid = graph_get_site_id(host, site_path)
        st.session_state["_sp_site_id"] = sid
    return sid

def graph_get_list_id(site_id: str, list_name: str) -> str:
    cache = st.session_state.get("_sp_list_id_cache", {})
    if cache.get(list_name):
        return cache[list_name]

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$top=200"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get lists failed: {r.status_code} {r.text}")
    for it in r.json().get("value", []):
        if it.get("displayName") == list_name:
            lid = it.get("id", "")
            cache[list_name] = lid
            st.session_state["_sp_list_id_cache"] = cache
            return lid
    raise Exception(f"List not found: {list_name}")

def graph_list_items_all(site_id: str, list_id: str, top: int = 2000):
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

def _parse_dt(v):
    if not v:
        return None
    s = str(v).strip()
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        d = pd.to_datetime(s, errors="coerce")
        if pd.notna(d):
            return d.to_pydatetime()
    except Exception:
        return None
    return None

# =========================
# Fetch
# =========================
def fetch_batches_df(start: date, end: date) -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_BATCHES", "P_Batches")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=5000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        wd = pd.to_datetime(f.get("WorkDate"), errors="coerce")
        if pd.isna(wd):
            continue
        wd = wd.date()
        if wd < start or wd > end:
            continue

        rows.append({
            "sp_item_id": it.get("id"),
            "batch_no": _to_text(f.get("Title")),
            "work_date": wd,
            # inputs
            "total_boxes": _to_float(f.get("TotalBoxes"), 0),
            "ct_per_box": _to_float(f.get("CtPerBox"), 0),
            "loose_ct": _to_float(f.get("LooseCT"), 0),
            "total_raw": _to_float(f.get("TotalRawMaterial"), 0),
            "raw_unit": _to_text(f.get("RawMaterialUnit")).lower(),
            "unit_weight_kg": _to_float(f.get("MaterialUnitWeightKg"), 0),
            "wastage": _to_float(f.get("Wastage"), 0),
            "wastage_unit": _to_text(f.get("WastageUnit")).lower(),
            "wage_per_hour": _to_float(f.get("WagePerHour"), 0),
            "material_cost": _to_float(f.get("MaterialCost"), 0),
            "include_extra": True if str(f.get("IncludeExtraCost")).lower() in ["true","yes","1"] else False,
            "extra_pct": _to_float(f.get("ExtraCostPct"), 0),
            "sell_price_per_ct": _to_float(f.get("SellPricePerCT"), 0),
        })
    return pd.DataFrame(rows)

def fetch_labour_df() -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_LABOURLINES", "P_LabourLines")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=8000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        batch_no = _to_text(f.get("Batch"))  # lookup text (Title)
        stt = _parse_dt(f.get("StartTime"))
        edt = _parse_dt(f.get("EndTime"))
        people = _to_float(f.get("People"), 0)
        rows.append({
            "batch_no": batch_no,
            "start_time": stt,
            "end_time": edt,
            "people": people,
        })
    df = pd.DataFrame(rows)
    return df

# =========================
# Calc (match your template)
# =========================
def convert_to_boxes(value, unit, unit_weight_kg):
    # convert kg/g to boxes using unit_weight_kg
    if value <= 0:
        return 0.0
    if unit == "boxes":
        return value
    if unit == "kg":
        return value / unit_weight_kg if unit_weight_kg > 0 else 0.0
    if unit == "g":
        return (value / 1000.0) / unit_weight_kg if unit_weight_kg > 0 else 0.0
    return 0.0

def labour_total_man_minutes(lab_df: pd.DataFrame, batch_no: str) -> float:
    if lab_df.empty:
        return 0.0
    x = lab_df[lab_df["batch_no"] == batch_no].copy()
    if x.empty:
        return 0.0
    total = 0.0
    for _, r in x.iterrows():
        stt = r["start_time"]
        edt = r["end_time"]
        ppl = float(r["people"] or 0)
        if not stt or not edt or edt <= stt or ppl <= 0:
            continue
        minutes = (edt - stt).total_seconds() / 60.0
        total += minutes * ppl
    return total

def calc_one_batch(row: dict, man_minutes: float) -> dict:
    total_output_ct = row["total_boxes"] * row["ct_per_box"] + row["loose_ct"]

    # wastage rate (convert to same base as total raw)
    # easiest: convert both total_raw and wastage into BOXES then rate
    total_raw_boxes = convert_to_boxes(row["total_raw"], row["raw_unit"], row["unit_weight_kg"])
    wastage_boxes = convert_to_boxes(row["wastage"], row["wastage_unit"], row["unit_weight_kg"])
    wastage_rate = (wastage_boxes / total_raw_boxes * 100.0) if total_raw_boxes > 0 else 0.0

    minutes_per_ct = (man_minutes / total_output_ct) if total_output_ct > 0 else 0.0
    labour_cost_per_ct = (row["wage_per_hour"] / 60.0) * minutes_per_ct if row["wage_per_hour"] > 0 else 0.0
    material_cost_per_ct = (row["material_cost"] / total_output_ct) if total_output_ct > 0 else 0.0

    base_cost_per_ct = labour_cost_per_ct + material_cost_per_ct
    extra_cost_per_ct = base_cost_per_ct * row["extra_pct"] / 100.0 if row["include_extra"] and row["extra_pct"] > 0 else 0.0
    total_cost_per_ct = base_cost_per_ct + extra_cost_per_ct

    profit_per_ct = row["sell_price_per_ct"] - total_cost_per_ct if row["sell_price_per_ct"] > 0 else 0.0
    profit_total = profit_per_ct * total_output_ct

    return {
        "TotalOutputCT": round(total_output_ct, 4),
        "TotalManMinutes": round(man_minutes, 4),
        "MinutesPerCT": round(minutes_per_ct, 6),
        "WastageRate": round(wastage_rate, 4),
        "LabourCostPerCT": round(labour_cost_per_ct, 6),
        "MaterialCostPerCT": round(material_cost_per_ct, 6),
        "ExtraCostPerCT": round(extra_cost_per_ct, 6),
        "TotalCostPerCT": round(total_cost_per_ct, 6),
        "ProfitPerCT": round(profit_per_ct, 6),
        "ProfitTotal": round(profit_total, 2),
    }

# =========================
# Write back to SharePoint
# =========================
def sp_update_batch_fields(batch_item_id: str, fields: dict):
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_BATCHES", "P_Batches")
    list_id = graph_get_list_id(site_id, list_name)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{batch_item_id}/fields"
    r = requests.patch(url, headers=graph_headers(), json=fields, timeout=30)
    if r.status_code not in [200, 204]:
        raise Exception(f"Update failed: {r.status_code} {r.text}")

# =========================
# UI
# =========================
st.set_page_config(page_title="Batch Calculator (Template Logic)", layout="wide")
st.title("Batch Calculator — same logic as your template")

with st.sidebar.expander("Settings"):
    write_back = st.checkbox("Write results back to P_Batches", value=True)

c1, c2 = st.columns(2)
with c1:
    start = st.date_input("Start date", value=date.today().replace(day=1))
with c2:
    end = st.date_input("End date", value=date.today())

if st.button("Load batches"):
    st.session_state.pop("_df_batches", None)
    st.session_state.pop("_df_labour", None)

df_batches = st.session_state.get("_df_batches")
df_labour = st.session_state.get("_df_labour")

if df_batches is None:
    try:
        df_batches = fetch_batches_df(start, end)
        st.session_state["_df_batches"] = df_batches
    except Exception as e:
        st.error(str(e))
        df_batches = pd.DataFrame()

if df_labour is None:
    try:
        df_labour = fetch_labour_df()
        st.session_state["_df_labour"] = df_labour
    except Exception as e:
        st.error(str(e))
        df_labour = pd.DataFrame()

if df_batches.empty:
    st.info("No batches found in this date range (check P_Batches WorkDate).")
else:
    # choose one batch
    batch_list = df_batches["batch_no"].tolist()
    sel = st.selectbox("Select BatchNo", batch_list)
    b = df_batches[df_batches["batch_no"] == sel].iloc[0].to_dict()

    man_minutes = labour_total_man_minutes(df_labour, sel)
    result = calc_one_batch(b, man_minutes)

    st.subheader("Inputs (from P_Batches)")
    st.write({k: b[k] for k in b if k not in ["sp_item_id"]})

    st.subheader("Calculated (template logic)")
    st.write(result)

    if st.button("Calculate + Save"):
        try:
            if write_back:
                sp_update_batch_fields(b["sp_item_id"], result)
            st.success("Calculated successfully ✅")
        except Exception as e:
            st.error(str(e))
