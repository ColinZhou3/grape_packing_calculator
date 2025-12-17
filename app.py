from datetime import datetime, date
import pandas as pd
import streamlit as st
import requests
import time

# =========================================================
# Helpers
# =========================================================
def secrets_get(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default

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
    """
    SharePoint/Graph: "2025-12-17T08:00:00Z" or other ISO formats
    """
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

def _parse_date(v):
    if not v:
        return None
    s = str(v).strip()
    try:
        # ISO datetime
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        d = pd.to_datetime(s, errors="coerce")
        if pd.notna(d):
            return d.date()
    except Exception:
        return None
    return None

# =========================================================
# Graph auth
# =========================================================
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

# =========================================================
# Graph: Site + List
# =========================================================
def graph_get_site_id(host: str, site_path: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get site failed: {r.status_code} {r.text}")
    site_id = r.json().get("id", "")
    if not site_id:
        raise Exception("Site id empty")
    return site_id

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
            if not lid:
                break
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

# =========================================================
# Generic PATCH write-back
# =========================================================
def sp_patch_item_fields(list_name: str, item_id: str, fields: dict):
    site_id = get_site_id()
    list_id = graph_get_list_id(site_id, list_name)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    r = requests.patch(url, headers=graph_headers(), json=fields, timeout=30)
    if r.status_code not in [200, 204]:
        raise Exception(f"Update failed ({list_name}): {r.status_code} {r.text}")

# =========================================================
# Fetch: P_Batches
# =========================================================
def fetch_batches_df(start: date, end: date) -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_BATCHES", "P_Batches")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=5000)

    rows = []
    for it in items:
        f = it.get("fields") or {}

        wd = _parse_date(f.get("WorkDate"))
        if not wd:
            continue
        if wd < start or wd > end:
            continue

        include_extra_raw = f.get("IncludeExtraCost")
        include_extra = str(include_extra_raw).lower() in ["true", "yes", "1"]

        rows.append({
            "sp_item_id": it.get("id"),                   # Graph item id (string)
            "batch_item_id_int": int(it.get("id")),       # for lookup id match
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

            "include_extra": include_extra,
            "extra_pct": _to_float(f.get("ExtraCostPct"), 0),

            "sell_price_per_ct": _to_float(f.get("SellPricePerCT"), 0),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["work_date", "batch_no"], ascending=[False, True])
    return df

# =========================================================
# Fetch: P_LabourLines
# =========================================================
def fetch_labour_df() -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_LABOURLINES", "P_LabourLines")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=8000)

    rows = []
    for it in items:
        f = it.get("fields") or {}

        # auto-detect Batch lookup id key: BatchLookupId / Batch0LookupId etc
        batch_lookup_id = None
        for k in f.keys():
            kl = k.lower()
            if kl.startswith("batch") and kl.endswith("lookupid"):
                batch_lookup_id = f.get(k)
                break

        rows.append({
            "sp_item_id": it.get("id"),  # labour line item id
            "batch_lookup_id": int(batch_lookup_id) if batch_lookup_id not in [None, ""] else None,
            "batch_text": _to_text(f.get("Batch")),  # lookup text if exists
            "start_time": _parse_dt(f.get("StartTime")),
            "end_time": _parse_dt(f.get("EndTime")),
            "people": _to_float(f.get("People"), 0),
            "duration_minutes": _to_float(f.get("DurationMinutes"), 0),
            "man_minutes": _to_float(f.get("ManMinutes"), 0),
        })

    return pd.DataFrame(rows)

# =========================================================
# Template calculation
# =========================================================
def convert_to_boxes(value, unit, unit_weight_kg):
    # convert kg/g to boxes using unit_weight_kg
    if value <= 0:
        return 0.0
    if unit == "boxes":
        return float(value)
    if unit == "kg":
        return float(value) / unit_weight_kg if unit_weight_kg > 0 else 0.0
    if unit == "g":
        return (float(value) / 1000.0) / unit_weight_kg if unit_weight_kg > 0 else 0.0
    return 0.0

def calc_one_batch(batch_row: dict, man_minutes: float) -> dict:
    total_output_ct = batch_row["total_boxes"] * batch_row["ct_per_box"] + batch_row["loose_ct"]

    # wastage rate: convert both total_raw and wastage to boxes then ratio
    total_raw_boxes = convert_to_boxes(batch_row["total_raw"], batch_row["raw_unit"], batch_row["unit_weight_kg"])
    wastage_boxes = convert_to_boxes(batch_row["wastage"], batch_row["wastage_unit"], batch_row["unit_weight_kg"])
    wastage_rate = (wastage_boxes / total_raw_boxes * 100.0) if total_raw_boxes > 0 else 0.0

    minutes_per_ct = (man_minutes / total_output_ct) if total_output_ct > 0 else 0.0
    labour_cost_per_ct = (batch_row["wage_per_hour"] / 60.0) * minutes_per_ct if batch_row["wage_per_hour"] > 0 else 0.0
    material_cost_per_ct = (batch_row["material_cost"] / total_output_ct) if total_output_ct > 0 else 0.0

    base_cost_per_ct = labour_cost_per_ct + material_cost_per_ct
    extra_cost_per_ct = base_cost_per_ct * batch_row["extra_pct"] / 100.0 if batch_row["include_extra"] and batch_row["extra_pct"] > 0 else 0.0
    total_cost_per_ct = base_cost_per_ct + extra_cost_per_ct

    profit_per_ct = batch_row["sell_price_per_ct"] - total_cost_per_ct if batch_row["sell_price_per_ct"] > 0 else 0.0
    profit_total = profit_per_ct * total_output_ct

    return {
        "TotalOutputCT": round(total_output_ct, 4),
        "TotalManMinutes": round(man_minutes, 2),
        "MinutesPerCT": round(minutes_per_ct, 6),
        "WastageRate": round(wastage_rate, 4),  # percent
        "LabourCostPerCT": round(labour_cost_per_ct, 6),
        "MaterialCostPerCT": round(material_cost_per_ct, 6),
        "ExtraCostPerCT": round(extra_cost_per_ct, 6),
        "TotalCostPerCT": round(total_cost_per_ct, 6),
        "ProfitPerCT": round(profit_per_ct, 6),
        "ProfitTotal": round(profit_total, 2),
    }

# =========================================================
# Labour -> auto calc Duration/ManMinutes and SUM
# =========================================================
def labour_total_man_minutes(lab_df: pd.DataFrame, batch_row: dict, write_back_lines: bool = True) -> float:
    if lab_df.empty:
        return 0.0

    bid = int(batch_row.get("batch_item_id_int"))
    bno = str(batch_row.get("batch_no"))

    # prefer lookup id match
    x = lab_df[lab_df["batch_lookup_id"] == bid].copy()
    if x.empty:
        x = lab_df[lab_df["batch_text"] == bno].copy()
    if x.empty:
        return 0.0

    total = 0.0

    for _, r in x.iterrows():
        stt = r["start_time"]
        edt = r["end_time"]
        ppl = float(r["people"] or 0)

        if (stt is None) or (edt is None) or (edt <= stt) or (ppl <= 0):
            continue

        duration = (edt - stt).total_seconds() / 60.0
        man = duration * ppl
        total += man

        # write back to labour line if empty/0
        if write_back_lines:
            dur_old = float(r.get("duration_minutes", 0) or 0)
            man_old = float(r.get("man_minutes", 0) or 0)
            need_update = (dur_old <= 0.01) or (man_old <= 0.01)
            if need_update:
                list_name = secrets_get("SP_LIST_LABOURLINES", "P_LabourLines")
                sp_patch_item_fields(
                    list_name=list_name,
                    item_id=str(r["sp_item_id"]),
                    fields={
                        "DurationMinutes": round(duration, 2),
                        "ManMinutes": round(man, 2),
                    }
                )

    return total

# =========================================================
# UI
# =========================================================
st.set_page_config(page_title="Batch Calculator — same logic as your template", layout="wide")
st.title("Batch Calculator — same logic as your template")

with st.sidebar.expander("Settings", expanded=False):
    write_back_batches = st.checkbox("Write results back to P_Batches", value=True)
    write_back_labour = st.checkbox("Auto write Duration/ManMinutes back to P_LabourLines", value=True)
    st.caption("建议两项都开着，这样 SharePoint 也能看到自动结果。")

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    start = st.date_input("Start date", value=date.today().replace(day=1))
with c2:
    end = st.date_input("End date", value=date.today())
with c3:
    if st.button("Load / Refresh"):
        st.session_state.pop("_df_batches", None)
        st.session_state.pop("_df_labour", None)

# load data
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
    st.info("No batches found in this date range. Check P_Batches WorkDate.")
    st.stop()

batch_list = df_batches["batch_no"].tolist()
sel = st.selectbox("Select BatchNo", batch_list)

b = df_batches[df_batches["batch_no"] == sel].iloc[0].to_dict()

# compute labour minutes (and optionally write back labour lines)
try:
    man_minutes = labour_total_man_minutes(df_labour, b, write_back_lines=write_back_labour)
except Exception as e:
    st.error(f"Labour calc/write error: {e}")
    man_minutes = 0.0

result = calc_one_batch(b, man_minutes)

left, right = st.columns([1, 1])

with left:
    st.subheader("Inputs (from P_Batches)")
    show_inputs = {
        "BatchNo": b["batch_no"],
        "WorkDate": str(b["work_date"]),
        "TotalBoxes": b["total_boxes"],
        "CtPerBox": b["ct_per_box"],
        "LooseCT": b["loose_ct"],
        "TotalRawMaterial": b["total_raw"],
        "RawMaterialUnit": b["raw_unit"],
        "MaterialUnitWeightKg": b["unit_weight_kg"],
        "Wastage": b["wastage"],
        "WastageUnit": b["wastage_unit"],
        "WagePerHour": b["wage_per_hour"],
        "MaterialCost": b["material_cost"],
        "IncludeExtraCost": b["include_extra"],
        "ExtraCostPct": b["extra_pct"],
        "SellPricePerCT": b["sell_price_per_ct"],
    }
    st.json(show_inputs)

    st.subheader("Labour lines (for this batch)")
    if df_labour.empty:
        st.info("No labour lines loaded.")
    else:
        # show by lookup id first
        x = df_labour[df_labour["batch_lookup_id"] == b["batch_item_id_int"]].copy()
        if x.empty:
            x = df_labour[df_labour["batch_text"] == b["batch_no"]].copy()

        if x.empty:
            st.warning("No labour lines matched this batch (check Batch lookup).")
        else:
            # show table
            x2 = x[["start_time", "end_time", "people", "duration_minutes", "man_minutes"]].copy()
            x2["start_time"] = x2["start_time"].astype(str)
            x2["end_time"] = x2["end_time"].astype(str)
            st.dataframe(x2, use_container_width=True)

with right:
    st.subheader("Calculated (template logic)")
    st.json(result)

    if st.button("Calculate + Save"):
        try:
            if write_back_batches:
                list_name_batches = secrets_get("SP_LIST_BATCHES", "P_Batches")
                sp_patch_item_fields(
                    list_name=list_name_batches,
                    item_id=str(b["sp_item_id"]),
                    fields=result
                )
            # clear cache so next view is fresh
            st.session_state.pop("_df_batches", None)
            st.session_state.pop("_df_labour", None)
            st.success("Calculated + Saved ✅ (refreshing data...)")
        except Exception as e:
            st.error(str(e))
