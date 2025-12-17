from __future__ import annotations

from datetime import datetime, date, timedelta, timezone
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st


# =========================================================
# Secrets helpers
# =========================================================
def secrets_get(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default


# =========================================================
# Graph auth (client credentials)
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
        raise Exception(f"Token is empty. Raw response: {r.text}")

    expires_in = int(js.get("expires_in", 3600))
    st.session_state["_graph_token_cache"] = {"access_token": token, "expires_at": now + expires_in}
    return token


def graph_headers() -> dict:
    token = graph_get_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# =========================================================
# Graph: site / list ids
# =========================================================
def graph_get_site_id(host: str, site_path: str) -> str:
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


def get_site_id() -> str:
    host = secrets_get("SP_HOST", "")
    site_path = secrets_get("SP_SITE_PATH", "")
    if not host or not site_path:
        raise Exception("Missing secrets: SP_HOST / SP_SITE_PATH")

    site_id = st.session_state.get("_sp_site_id")
    if not site_id:
        site_id = graph_get_site_id(host, site_path)
        st.session_state["_sp_site_id"] = site_id
    return site_id


def get_list_id_cached(list_name: str) -> str:
    site_id = get_site_id()
    cache = st.session_state.get("_sp_list_ids", {})
    if list_name in cache:
        return cache[list_name]
    list_id = graph_get_list_id(site_id, list_name)
    cache[list_name] = list_id
    st.session_state["_sp_list_ids"] = cache
    return list_id


# =========================================================
# Graph: list items read (auto paging)
# =========================================================
def graph_list_items_all(site_id: str, list_id: str, top: int = 2000) -> List[Dict[str, Any]]:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top={top}"
    out: List[Dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=graph_headers(), timeout=30)
        if r.status_code != 200:
            raise Exception(f"Get items failed: {r.status_code} {r.text}")
        js = r.json()
        out.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return out


def graph_patch_item_fields(site_id: str, list_id: str, item_id: str, fields_patch: Dict[str, Any]) -> None:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    r = requests.patch(url, headers=graph_headers(), data=json.dumps(fields_patch), timeout=30)
    if r.status_code not in (200, 204):
        raise Exception(f"PATCH fields failed: {r.status_code} {r.text}")


# =========================================================
# parsing helpers
# =========================================================
def _to_float(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _to_int(v, default=0) -> int:
    try:
        if v is None or v == "":
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


def _to_text(v) -> str:
    if v is None:
        return ""
    return str(v)


def _to_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "yes", "y", "1", "on")


def _parse_date(v) -> Optional[date]:
    if not v:
        return None
    s = str(v).strip()
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        if "-" in s and len(s) >= 10:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        if "/" in s:
            # SharePoint sometimes mm/dd/yyyy
            return datetime.strptime(s, "%m/%d/%Y").date()
    except Exception:
        return None
    return None


def _parse_dt(v) -> Optional[datetime]:
    if not v:
        return None
    s = str(v).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt
        # fallback: try common
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _get_any(fields: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in fields and fields.get(k) not in (None, ""):
            return fields.get(k)
    return default


# =========================================================
# List names
# =========================================================
LIST_P_BATCHES = secrets_get("SP_LIST_P_BATCHES", "P_Batches")
LIST_P_LABOUR = secrets_get("SP_LIST_P_LABOURLINES", "P_LabourLines")
LIST_PRODUCTS = secrets_get("SP_LIST_PRODUCTS", "Products")  # optional


# =========================================================
# Column names (try to be robust)
# =========================================================
# P_Batches (inputs)
COL_BATCHNO = ["BatchNo", "Title"]
COL_WORKDATE = ["WorkDate"]
COL_PACKTYPE = ["PackType"]

COL_TOTALBOXES = ["TotalBoxes"]
COL_CTPERBOX = ["CtPerBox"]
COL_LOOSECT = ["LooseCT"]

COL_TOTALRAW = ["TotalRawMaterial"]
COL_RAWUNIT = ["RawMaterialUnit"]
COL_UNITWEIGHT = ["MaterialUnitWeightKg"]

COL_WASTAGE = ["Wastage"]
COL_WASTAGEUNIT = ["WastageUnit"]

COL_WAGEPH = ["WagePerHour"]
COL_MATERIALCOST = ["MaterialCost"]

COL_INCLUDEEXTRA = ["IncludeExtraCost", "IncludeExtraCost1", "IncludeExtraCost2"]
COL_EXTRAPCT = ["ExtraCostPct", "ExtraCostPercentage", "OverheadPct", "OverheadPctDefault"]
COL_SELLPRICE = ["SellPricePerCT", "SellPricePerCt", "SellPricePerCT1"]

# P_Batches (outputs you added)
OUT_TOTALOUTPUT = "TotalOutputCT"
OUT_TOTALMANMIN = "TotalManMinutes"
OUT_MINPERCT = "MinutesPerCT"
OUT_WASTERATE = "WastageRatePct"
OUT_LABOURCOST = "LabourCostPerCT"
OUT_MATCOST = "MaterialCostPerCT"
OUT_EXTRACOST = "ExtraCostPerCT"
OUT_TOTALCOST = "TotalCostPerCT"
OUT_PROFITPERCT = "ProfitPerCT"
OUT_PROFITTOTAL = "ProfitTotal"

# optional debug/storage
OUT_RAWKG = "RawKg"
OUT_WASTAGEKG = "WastageKg"
OUT_CALCAT = "CalculatedAt"

# P_LabourLines
LAB_COL_BATCH_LOOKUP_ID = ["BatchLookupId"]  # best case
LAB_COL_BATCH_TEXT = ["Batch", "BatchNo", "Title"]  # fallback
LAB_COL_START = ["StartTime"]
LAB_COL_END = ["EndTime"]
LAB_COL_PEOPLE = ["People"]
LAB_COL_ROLE = ["Role"]

LAB_OUT_DURATION = "DurationMinutes"
LAB_OUT_MANMIN = "ManMinutes"


# =========================================================
# Fetchers
# =========================================================
def fetch_p_batches() -> List[Dict[str, Any]]:
    site_id = get_site_id()
    list_id = get_list_id_cached(LIST_P_BATCHES)
    return graph_list_items_all(site_id, list_id, top=2000)


def fetch_labour_lines() -> List[Dict[str, Any]]:
    site_id = get_site_id()
    list_id = get_list_id_cached(LIST_P_LABOUR)
    return graph_list_items_all(site_id, list_id, top=2000)


def fetch_products_map() -> Dict[str, Dict[str, Any]]:
    """
    key by ProductName or Title; value is fields dict.
    optional only (if list exists)
    """
    try:
        site_id = get_site_id()
        list_id = get_list_id_cached(LIST_PRODUCTS)
        items = graph_list_items_all(site_id, list_id, top=4000)
    except Exception:
        return {}

    mp: Dict[str, Dict[str, Any]] = {}
    for it in items:
        f = it.get("fields") or {}
        name = _get_any(f, ["ProductName", "Title"], "")
        if name:
            mp[str(name)] = f
    return mp


# =========================================================
# Core logic: unit conversion + calculations
# =========================================================
def normalize_unit(u: str) -> str:
    s = (u or "").strip().lower()
    s = s.replace(".", "")
    return s


def convert_to_kg(qty: float, unit: str, unit_weight_kg: float) -> float:
    u = normalize_unit(unit)

    # kg directly
    if u in ("kg", "kgs", "kilogram", "kilograms"):
        return float(qty)

    # common “unit-count” type -> multiply by unit weight
    if u in ("box", "boxes", "carton", "cartons", "crate", "crates", "ctn", "ctns"):
        return float(qty) * float(unit_weight_kg)

    # loose: usually user already gives kg, but sometimes they write “loose” with kg value
    if u in ("loose", "loosekg", "bulk"):
        return float(qty)

    # pack: if they put pack but qty is already kg, keep it
    if u in ("pack", "packs", "pkt", "pkts"):
        # if unit_weight_kg exists, treat as qty * unit_weight_kg, else assume kg
        if unit_weight_kg and unit_weight_kg > 0:
            return float(qty) * float(unit_weight_kg)
        return float(qty)

    # fallback: assume kg
    return float(qty)


def calc_for_batch(batch_fields: Dict[str, Any], labour_items: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # ---- Inputs
    total_boxes = _to_float(_get_any(batch_fields, COL_TOTALBOXES, 0), 0)
    ct_per_box = _to_float(_get_any(batch_fields, COL_CTPERBOX, 0), 0)
    loose_ct = _to_float(_get_any(batch_fields, COL_LOOSECT, 0), 0)

    total_raw = _to_float(_get_any(batch_fields, COL_TOTALRAW, 0), 0)
    raw_unit = _to_text(_get_any(batch_fields, COL_RAWUNIT, ""))
    unit_weight_kg = _to_float(_get_any(batch_fields, COL_UNITWEIGHT, 0), 0)

    wastage = _to_float(_get_any(batch_fields, COL_WASTAGE, 0), 0)
    wastage_unit = _to_text(_get_any(batch_fields, COL_WASTAGEUNIT, "kg"))

    wage_per_hour = _to_float(_get_any(batch_fields, COL_WAGEPH, 0), 0)
    material_cost = _to_float(_get_any(batch_fields, COL_MATERIALCOST, 0), 0)

    include_extra = _to_bool(_get_any(batch_fields, COL_INCLUDEEXTRA, False))
    extra_pct = _to_float(_get_any(batch_fields, COL_EXTRAPCT, 0), 0)

    sell_price_per_ct = _to_float(_get_any(batch_fields, COL_SELLPRICE, 0), 0)

    # ---- Output CT
    total_output_ct = total_boxes * ct_per_box + loose_ct

    # ---- Labour lines compute
    labour_rows: List[Dict[str, Any]] = []
    total_man_minutes = 0.0

    for it in labour_items:
        f = it.get("fields") or {}
        start_dt = _parse_dt(_get_any(f, LAB_COL_START, None))
        end_dt = _parse_dt(_get_any(f, LAB_COL_END, None))
        people = _to_float(_get_any(f, LAB_COL_PEOPLE, 0), 0)

        duration_minutes = 0.0
        if start_dt and end_dt:
            # if no tz info, assume UTC
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)

            if end_dt < start_dt:
                end_dt = end_dt + timedelta(days=1)

            duration_minutes = (end_dt - start_dt).total_seconds() / 60.0
            if duration_minutes < 0:
                duration_minutes = 0.0

        man_minutes = duration_minutes * people
        total_man_minutes += man_minutes

        labour_rows.append({
            "sp_item_id": it.get("id"),
            "start_time": start_dt.isoformat() if start_dt else "",
            "end_time": end_dt.isoformat() if end_dt else "",
            "people": people,
            "duration_minutes": round(duration_minutes, 2),
            "man_minutes": round(man_minutes, 2),
            "role": _to_text(_get_any(f, LAB_COL_ROLE, "")),
        })

    minutes_per_ct = (total_man_minutes / total_output_ct) if total_output_ct > 0 else 0.0

    # ---- Unit conversion
    raw_kg = convert_to_kg(total_raw, raw_unit, unit_weight_kg)
    wastage_kg = convert_to_kg(wastage, wastage_unit, unit_weight_kg)

    wastage_rate_pct = (wastage_kg / raw_kg * 100.0) if raw_kg > 0 else 0.0

    # ---- Costs
    labour_cost_per_ct = (minutes_per_ct * (wage_per_hour / 60.0)) if total_output_ct > 0 else 0.0
    material_cost_per_ct = (material_cost / total_output_ct) if total_output_ct > 0 else 0.0

    base_cost_per_ct = labour_cost_per_ct + material_cost_per_ct

    extra_cost_per_ct = (base_cost_per_ct * (extra_pct / 100.0)) if include_extra else 0.0
    total_cost_per_ct = base_cost_per_ct + extra_cost_per_ct

    profit_per_ct = sell_price_per_ct - total_cost_per_ct
    profit_total = profit_per_ct * total_output_ct

    calc = {
        OUT_TOTALOUTPUT: round(total_output_ct, 4),
        OUT_TOTALMANMIN: round(total_man_minutes, 4),
        OUT_MINPERCT: round(minutes_per_ct, 4),
        OUT_WASTERATE: round(wastage_rate_pct, 4),
        OUT_LABOURCOST: round(labour_cost_per_ct, 4),
        OUT_MATCOST: round(material_cost_per_ct, 4),
        OUT_EXTRACOST: round(extra_cost_per_ct, 4),
        OUT_TOTALCOST: round(total_cost_per_ct, 4),
        OUT_PROFITPERCT: round(profit_per_ct, 4),
        OUT_PROFITTOTAL: round(profit_total, 4),

        # optional
        OUT_RAWKG: round(raw_kg, 4),
        OUT_WASTAGEKG: round(wastage_kg, 4),
        OUT_CALCAT: datetime.now(timezone.utc).isoformat(),
    }

    return calc, labour_rows


# =========================================================
# UI
# =========================================================
st.set_page_config(page_title="Batch Calculator (SharePoint)", layout="wide")
st.title("Batch Calculator — same logic as your template")

with st.sidebar:
    st.header("Settings")
    st.caption("这版：P_Batches + P_LabourLines 自动计算 + 写回 SharePoint")
    if st.button("Test Graph connection"):
        try:
            token = graph_get_token()
            st.success(f"Token OK (len={len(token)})")
            site_id = get_site_id()
            st.success(f"Site OK: {site_id[:30]}...")
            lb = get_list_id_cached(LIST_P_BATCHES)
            st.success(f"P_Batches OK: {lb[:30]}...")
            ll = get_list_id_cached(LIST_P_LABOUR)
            st.success(f"P_LabourLines OK: {ll[:30]}...")
        except Exception as e:
            st.error(str(e))

# date range + load
c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    start_date = st.date_input("Start date", value=date.today().replace(day=1))
with c2:
    end_date = st.date_input("End date", value=date.today())
with c3:
    load_btn = st.button("Load / Refresh")

if load_btn:
    st.session_state.pop("_cache_batches", None)
    st.session_state.pop("_cache_labour", None)
    st.session_state.pop("_cache_products", None)

# load caches
if st.session_state.get("_cache_batches") is None:
    try:
        batches_items = fetch_p_batches()
        st.session_state["_cache_batches"] = batches_items
    except Exception as e:
        st.error(str(e))
        batches_items = []
else:
    batches_items = st.session_state.get("_cache_batches")

if st.session_state.get("_cache_labour") is None:
    try:
        labour_items_all = fetch_labour_lines()
        st.session_state["_cache_labour"] = labour_items_all
    except Exception as e:
        st.error(str(e))
        labour_items_all = []
else:
    labour_items_all = st.session_state.get("_cache_labour")

# optional products (not required for calc, but future-proof)
if st.session_state.get("_cache_products") is None:
    st.session_state["_cache_products"] = fetch_products_map()
products_map = st.session_state.get("_cache_products", {})

# filter batches by WorkDate
filtered_batches: List[Dict[str, Any]] = []
for it in batches_items:
    f = it.get("fields") or {}
    wd = _parse_date(_get_any(f, COL_WORKDATE, None))
    if not wd:
        continue
    if wd < start_date or wd > end_date:
        continue
    filtered_batches.append(it)

# build dropdown options
batch_options: List[Tuple[str, str]] = []  # (label, item_id)
for it in filtered_batches:
    f = it.get("fields") or {}
    bn = _get_any(f, COL_BATCHNO, "")
    wd = _parse_date(_get_any(f, COL_WORKDATE, None))
    label = f"{bn}".strip()
    if wd:
        label = f"{label}  ({wd.isoformat()})"
    batch_options.append((label, str(it.get("id"))))

if not batch_options:
    st.info("No batches in this date range (P_Batches).")
    st.stop()

selected_label = st.selectbox("Select BatchNo", options=[x[0] for x in batch_options], index=0)
selected_item_id = dict(batch_options).get(selected_label)

# find selected batch item
selected_batch_item = None
for it in filtered_batches:
    if str(it.get("id")) == str(selected_item_id):
        selected_batch_item = it
        break

if not selected_batch_item:
    st.error("Selected batch not found (unexpected).")
    st.stop()

batch_fields = selected_batch_item.get("fields") or {}
batch_no = _get_any(batch_fields, COL_BATCHNO, "")

# filter labour lines for this batch
labour_for_batch: List[Dict[str, Any]] = []
batch_lookup_id_int = _to_int(selected_item_id, 0)

for it in labour_items_all:
    f = it.get("fields") or {}

    # best: match BatchLookupId == batch item id
    lk = _to_int(_get_any(f, LAB_COL_BATCH_LOOKUP_ID, 0), 0)
    if lk and lk == batch_lookup_id_int:
        labour_for_batch.append(it)
        continue

    # fallback: match text
    bt = _to_text(_get_any(f, LAB_COL_BATCH_TEXT, ""))
    if bt and str(bt).strip() and str(bt).strip() == str(batch_no).strip():
        labour_for_batch.append(it)

# calculate
calc, labour_rows = calc_for_batch(batch_fields, labour_for_batch)

# show input + output
left, right = st.columns(2)

with left:
    st.subheader("Inputs (from P_Batches)")
    # show only key inputs
    inputs_preview = {
        "BatchNo": batch_no,
        "WorkDate": _to_text(_get_any(batch_fields, COL_WORKDATE, "")),
        "PackType": _to_text(_get_any(batch_fields, COL_PACKTYPE, "")),
        "TotalBoxes": _to_float(_get_any(batch_fields, COL_TOTALBOXES, 0), 0),
        "CtPerBox": _to_float(_get_any(batch_fields, COL_CTPERBOX, 0), 0),
        "LooseCT": _to_float(_get_any(batch_fields, COL_LOOSECT, 0), 0),
        "TotalRawMaterial": _to_float(_get_any(batch_fields, COL_TOTALRAW, 0), 0),
        "RawMaterialUnit": _to_text(_get_any(batch_fields, COL_RAWUNIT, "")),
        "MaterialUnitWeightKg": _to_float(_get_any(batch_fields, COL_UNITWEIGHT, 0), 0),
        "Wastage": _to_float(_get_any(batch_fields, COL_WASTAGE, 0), 0),
        "WastageUnit": _to_text(_get_any(batch_fields, COL_WASTAGEUNIT, "")),
        "WagePerHour": _to_float(_get_any(batch_fields, COL_WAGEPH, 0), 0),
        "MaterialCost": _to_float(_get_any(batch_fields, COL_MATERIALCOST, 0), 0),
        "IncludeExtraCost": _to_bool(_get_any(batch_fields, COL_INCLUDEEXTRA, False)),
        "ExtraCostPct": _to_float(_get_any(batch_fields, COL_EXTRAPCT, 0), 0),
        "SellPricePerCT": _to_float(_get_any(batch_fields, COL_SELLPRICE, 0), 0),
    }
    st.code(json.dumps(inputs_preview, indent=2, ensure_ascii=False), language="json")

    st.subheader("Labour lines (for this batch)")
    if labour_rows:
        df_lab = pd.DataFrame(labour_rows)
        st.dataframe(df_lab[["start_time", "end_time", "people", "duration_minutes", "man_minutes", "role"]], use_container_width=True)
    else:
        st.info("No labour lines found for this batch (P_LabourLines).")

with right:
    st.subheader("Calculated (template logic)")
    calc_preview = {
        OUT_TOTALOUTPUT: calc[OUT_TOTALOUTPUT],
        OUT_TOTALMANMIN: calc[OUT_TOTALMANMIN],
        OUT_MINPERCT: calc[OUT_MINPERCT],
        "WastageRate(%)": calc[OUT_WASTERATE],
        OUT_LABOURCOST: calc[OUT_LABOURCOST],
        OUT_MATCOST: calc[OUT_MATCOST],
        OUT_EXTRACOST: calc[OUT_EXTRACOST],
        OUT_TOTALCOST: calc[OUT_TOTALCOST],
        OUT_PROFITPERCT: calc[OUT_PROFITPERCT],
        OUT_PROFITTOTAL: calc[OUT_PROFITTOTAL],
    }
    st.code(json.dumps(calc_preview, indent=2, ensure_ascii=False), language="json")

    st.caption("Extra debug (unit conversion)")
    st.write(f"RawKg: {calc.get(OUT_RAWKG, 0)}, WastageKg: {calc.get(OUT_WASTAGEKG, 0)}")

    # buttons
    save_cols = st.columns([2, 1])
    with save_cols[0]:
        do_save = st.button("Calculate + Save to P_Batches", type="primary")
    with save_cols[1]:
        write_labour_back = st.checkbox("Also write labour Duration/ManMinutes", value=True)

    if do_save:
        try:
            site_id = get_site_id()
            pb_list_id = get_list_id_cached(LIST_P_BATCHES)
            pl_list_id = get_list_id_cached(LIST_P_LABOUR)

            # patch batch outputs
            patch_fields = {
                OUT_TOTALOUTPUT: calc[OUT_TOTALOUTPUT],
                OUT_TOTALMANMIN: calc[OUT_TOTALMANMIN],
                OUT_MINPERCT: calc[OUT_MINPERCT],
                OUT_WASTERATE: calc[OUT_WASTERATE],
                OUT_LABOURCOST: calc[OUT_LABOURCOST],
                OUT_MATCOST: calc[OUT_MATCOST],
                OUT_EXTRACOST: calc[OUT_EXTRACOST],
                OUT_TOTALCOST: calc[OUT_TOTALCOST],
                OUT_PROFITPERCT: calc[OUT_PROFITPERCT],
                OUT_PROFITTOTAL: calc[OUT_PROFITTOTAL],
            }

            # optional fields: only patch if your list has them (if not, it might error)
            # 你说你也加了，可直接开着
            patch_fields[OUT_RAWKG] = calc.get(OUT_RAWKG, 0)
            patch_fields[OUT_WASTAGEKG] = calc.get(OUT_WASTAGEKG, 0)
            patch_fields[OUT_CALCAT] = calc.get(OUT_CALCAT, "")

            graph_patch_item_fields(site_id, pb_list_id, str(selected_item_id), patch_fields)

            # patch labour lines duration + manminutes
            if write_labour_back and labour_rows:
                for row in labour_rows:
                    li_id = str(row["sp_item_id"])
                    graph_patch_item_fields(
                        site_id,
                        pl_list_id,
                        li_id,
                        {
                            LAB_OUT_DURATION: row["duration_minutes"],
                            LAB_OUT_MANMIN: row["man_minutes"],
                        }
                    )

            st.success("Saved ✅ (P_Batches outputs updated)")
        except Exception as e:
            st.error(str(e))
