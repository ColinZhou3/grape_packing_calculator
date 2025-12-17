from __future__ import annotations

from datetime import datetime, date, timezone
import time
import requests
import pandas as pd
import streamlit as st

# =========================================================
# REQUIRED SECRETS (st.secrets)
# TENANT_ID, CLIENT_ID, CLIENT_SECRET
# SP_HOST (e.g. "yourtenant.sharepoint.com")
# SP_SITE_PATH (e.g. "/sites/HealthyFresh")
# =========================================================

# =========================================================
# LIST NAMES (SharePoint displayName)
# =========================================================
LIST_P_BATCHES = "P_Batches"
LIST_P_LABOUR = "P_LabourLines"
LIST_PACKTYPES = "M_PackTypes"   # you created
LIST_PRODUCTS = "Products"

# =========================================================
# P_Batches column internal names (match your SP columns)
# =========================================================
B_COL_BATCHNO = "Title"              # BatchNo stored in Title
B_COL_WORKDATE = "WorkDate"

# Inputs (from your template fields)
B_COL_TOTALBOXES = "TotalBoxes"
B_COL_CTPERBOX = "CtPerBox"
B_COL_LOOSECT = "LooseCT"

B_COL_TOTALRAW = "TotalRawMaterial"
B_COL_RAWUNIT = "RawMaterialUnit"
B_COL_UNITWEIGHTKG = "MaterialUnitWeightKg"

B_COL_WASTAGE = "Wastage"
B_COL_WASTAGEUNIT = "WastageUnit"

B_COL_WAGEPERHOUR = "WagePerHour"
B_COL_MATERIALCOST = "MaterialCost"

B_COL_INCLUDEEXTRA = "IncludeExtraCost"
B_COL_EXTRAPCT = "ExtraCostPct"
B_COL_EXTRADESC = "ExtraCostDescription"

B_COL_SELLPRICE = "SellPricePerCT"

# Lookups (optional; depends how you named)
B_COL_PRODUCT = "ProductName"   # lookup to Products (yours)
B_COL_PACKTYPE = "PackType"     # lookup to M_PackTypes (you added)

# Outputs (to write back)
B_OUT_TOTALOUTPUTCT = "TotalOutputCT"
B_OUT_TOTALMANMIN = "TotalManMinutes"
B_OUT_MINPERCT = "MinutesPerCT"
B_OUT_WASTAGERATE = "WastageRate"
B_OUT_LABOURCOST = "LabourCostPerCT"
B_OUT_MATERIALCOST = "MaterialCostPerCT"
B_OUT_EXTRACOST = "ExtraCostPerCT"
B_OUT_TOTALCOST = "TotalCostPerCT"
B_OUT_PROFITPERCT = "ProfitPerCT"
B_OUT_PROFITTOTAL = "ProfitTotal"

# =========================================================
# P_LabourLines columns
# =========================================================
L_COL_BATCH = "Batch"           # lookup to P_Batches
L_COL_START = "StartTime"
L_COL_END = "EndTime"
L_COL_PEOPLE = "People"
L_COL_DURATION = "DurationMinutes"  # optional calc col in SP
L_COL_MANMIN = "ManMinutes"         # optional calc col in SP

# =========================================================
# M_PackTypes columns
# =========================================================
PT_COL_TITLE = "Title"
PT_COL_PRODUCT = "Product"              # lookup to Products (if you made)
PT_COL_CTPERBOX = "CtPerBox"
PT_COL_PACKWEIGHTKG = "PackUnitWeightKg"  # 0.7, 0.5 etc
PT_COL_SELLDEFAULT = "SellPricePerCTDefault"
PT_COL_ACTIVE = "Active"

# =========================================================
# Helpers
# =========================================================
def secrets_get(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return default

def _to_float(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        return float(v)
    except Exception:
        return float(default)

def _to_bool(v, default=False) -> bool:
    if v is None or v == "":
        return bool(default)
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ["1", "true", "yes", "y"]:
        return True
    if s in ["0", "false", "no", "n"]:
        return False
    return bool(default)

def _to_text(v) -> str:
    if v is None:
        return ""
    return str(v)

def _parse_date(v):
    if not v:
        return None
    s = str(v).strip()
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        if "-" in s and len(s) >= 10:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        if "/" in s:
            # mm/dd/yyyy
            return datetime.strptime(s, "%m/%d/%Y").date()
    except Exception:
        return None
    return None

def _parse_dt(v):
    if not v:
        return None
    s = str(v).strip()
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        # fallback
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _get_lookup_id(fields: dict, base: str):
    # Graph usually provides <ColName>LookupId for lookup columns
    k = f"{base}LookupId"
    if k in fields:
        return fields.get(k)
    return None

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
        raise Exception(f"Token empty. Raw: {r.text}")

    expires_in = int(js.get("expires_in", 3600))
    st.session_state["_graph_token_cache"] = {"access_token": token, "expires_at": now + expires_in}
    return token

def graph_headers() -> dict:
    return {"Authorization": f"Bearer {graph_get_token()}", "Content-Type": "application/json"}

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

def get_site_and_list_id(list_name: str):
    host = secrets_get("SP_HOST", "")
    site_path = secrets_get("SP_SITE_PATH", "")
    if not host or not site_path:
        raise Exception("Missing secrets: SP_HOST / SP_SITE_PATH")

    site_id = st.session_state.get("_sp_site_id")
    if not site_id:
        site_id = graph_get_site_id(host, site_path)
        st.session_state["_sp_site_id"] = site_id

    cache = st.session_state.get("_sp_list_ids", {})
    if list_name in cache:
        return site_id, cache[list_name]

    list_id = graph_get_list_id(site_id, list_name)
    cache[list_name] = list_id
    st.session_state["_sp_list_ids"] = cache
    return site_id, list_id

def graph_list_items_all(site_id: str, list_id: str, top: int = 2000) -> list[dict]:
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

def graph_patch_item_fields(site_id: str, list_id: str, item_id: str, fields_patch: dict):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    r = requests.patch(url, headers=graph_headers(), json=fields_patch, timeout=30)
    if r.status_code not in (200, 204):
        raise Exception(f"PATCH failed: {r.status_code} {r.text}")

# =========================================================
# Fetchers
# =========================================================
@st.cache_data(ttl=60)
def fetch_packtypes_map() -> dict:
    site_id, list_id = get_site_and_list_id(LIST_PACKTYPES)
    items = graph_list_items_all(site_id, list_id, top=2000)
    mp = {}
    for it in items:
        f = it.get("fields") or {}
        title = _to_text(f.get(PT_COL_TITLE))
        if not title:
            continue
        active = _to_bool(f.get(PT_COL_ACTIVE), True)
        mp[title] = {
            "active": active,
            "ct_per_box": _to_float(f.get(PT_COL_CTPERBOX), 0),
            "pack_weight_kg": _to_float(f.get(PT_COL_PACKWEIGHTKG), 0),
            "sell_default": _to_float(f.get(PT_COL_SELLDEFAULT), 0),
        }
    return mp

def fetch_batches_df(start: date, end: date) -> pd.DataFrame:
    site_id, list_id = get_site_and_list_id(LIST_P_BATCHES)
    items = graph_list_items_all(site_id, list_id, top=2000)
    rows = []
    for it in items:
        f = it.get("fields") or {}
        wd = _parse_date(f.get(B_COL_WORKDATE))
        if not wd:
            continue
        if wd < start or wd > end:
            continue

        rows.append({
            "sp_item_id": it.get("id"),
            "batch_no": _to_text(f.get(B_COL_BATCHNO)),
            "work_date": wd,
            "product_lookup_id": _get_lookup_id(f, B_COL_PRODUCT),
            "packtype_lookup_id": _get_lookup_id(f, B_COL_PACKTYPE),
            "pack_type": _to_text(f.get(B_COL_PACKTYPE)) or _to_text(f.get("PackType")),

            "total_boxes": _to_float(f.get(B_COL_TOTALBOXES), 0),
            "ct_per_box": _to_float(f.get(B_COL_CTPERBOX), 0),
            "loose_ct": _to_float(f.get(B_COL_LOOSECT), 0),

            "total_raw": _to_float(f.get(B_COL_TOTALRAW), 0),
            "raw_unit": _to_text(f.get(B_COL_RAWUNIT)).lower(),
            "unit_weight_kg": _to_float(f.get(B_COL_UNITWEIGHTKG), 0),

            "wastage": _to_float(f.get(B_COL_WASTAGE), 0),
            "wastage_unit": _to_text(f.get(B_COL_WASTAGEUNIT)).lower(),

            "wage_per_hour": _to_float(f.get(B_COL_WAGEPERHOUR), 0),
            "material_cost": _to_float(f.get(B_COL_MATERIALCOST), 0),

            "include_extra": _to_bool(f.get(B_COL_INCLUDEEXTRA), False),
            "extra_pct": _to_float(f.get(B_COL_EXTRAPCT), 0),
            "sell_price_per_ct": _to_float(f.get(B_COL_SELLPRICE), 0),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["work_date", "batch_no"], ascending=[False, True])
    return df

def fetch_labour_lines_df(batch_item_id: str) -> pd.DataFrame:
    # We fetch all and filter in python (simple + safe)
    site_id, list_id = get_site_and_list_id(LIST_P_LABOUR)
    items = graph_list_items_all(site_id, list_id, top=2000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        # lookup to P_Batches
        bid = _get_lookup_id(f, L_COL_BATCH)
        if str(bid) != str(batch_item_id):
            continue

        start_dt = _parse_dt(f.get(L_COL_START))
        end_dt = _parse_dt(f.get(L_COL_END))
        people = _to_float(f.get(L_COL_PEOPLE), 0)

        duration = _to_float(f.get(L_COL_DURATION), 0)
        manmin = _to_float(f.get(L_COL_MANMIN), 0)

        # if SP calculated columns not ready, compute here
        if duration <= 0 and start_dt and end_dt:
            duration = (end_dt - start_dt).total_seconds() / 60.0
            if duration < 0:
                # not handling cross-midnight yet
                duration = 0

        if manmin <= 0 and duration > 0 and people > 0:
            manmin = duration * people

        rows.append({
            "sp_item_id": it.get("id"),
            "start_time": start_dt,
            "end_time": end_dt,
            "people": people,
            "duration_minutes": round(duration, 2),
            "man_minutes": round(manmin, 2),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["start_time"], ascending=[True])

# =========================================================
# Conversion + calculations (template logic)
# =========================================================
def to_kg(qty: float, uom: str, kg_per_box: float, pack_unit_kg: float) -> float:
    u = (uom or "").strip().lower()
    if qty is None:
        return 0.0
    qty = float(qty)

    if u in ["kg", "kgs", "kilogram", "kilograms"]:
        return qty

    # treat carton/crate/box as "box-like"
    if u in ["box", "boxes", "carton", "cartons", "crate", "crates"]:
        return qty * float(kg_per_box or 0)

    # ct/pack: number of retail packs
    if u in ["ct", "count", "pack", "packs"]:
        return qty * float(pack_unit_kg or 0)

    # loose: usually already kg; if not, we still treat as kg to avoid blocking
    if u in ["loose", ""]:
        return qty

    # unknown -> assume kg (safe default)
    return qty

def calc_template(batch_row: dict, labour_df: pd.DataFrame, packtype_info: dict | None) -> dict:
    total_boxes = float(batch_row.get("total_boxes", 0))
    ct_per_box = float(batch_row.get("ct_per_box", 0))
    loose_ct = float(batch_row.get("loose_ct", 0))

    total_raw = float(batch_row.get("total_raw", 0))
    raw_unit = (batch_row.get("raw_unit") or "").lower()
    unit_weight_kg = float(batch_row.get("unit_weight_kg", 0))

    wastage = float(batch_row.get("wastage", 0))
    wastage_unit = (batch_row.get("wastage_unit") or "").lower()

    wage_per_hour = float(batch_row.get("wage_per_hour", 0))
    material_cost = float(batch_row.get("material_cost", 0))
    include_extra = bool(batch_row.get("include_extra", False))
    extra_pct = float(batch_row.get("extra_pct", 0))
    sell_price_per_ct = float(batch_row.get("sell_price_per_ct", 0))

    # packtype defaults (if provided)
    pack_unit_kg = 0.0
    if packtype_info:
        if ct_per_box <= 0 and packtype_info.get("ct_per_box", 0) > 0:
            ct_per_box = float(packtype_info["ct_per_box"])
        pack_unit_kg = float(packtype_info.get("pack_weight_kg", 0))
        if sell_price_per_ct <= 0 and packtype_info.get("sell_default", 0) > 0:
            sell_price_per_ct = float(packtype_info["sell_default"])

    # OUTPUT
    total_output_ct = total_boxes * ct_per_box + loose_ct

    # LABOUR
    total_man_minutes = float(labour_df["man_minutes"].sum()) if not labour_df.empty else 0.0
    minutes_per_ct = (total_man_minutes / total_output_ct) if total_output_ct > 0 else 0.0

    # WASTAGE RATE (convert to kg base)
    raw_kg = to_kg(total_raw, raw_unit, unit_weight_kg, pack_unit_kg)
    wastage_kg = to_kg(wastage, wastage_unit, unit_weight_kg, pack_unit_kg)
    wastage_rate = (wastage_kg / raw_kg * 100.0) if raw_kg > 0 else 0.0

    # COSTS
    labour_cost_per_ct = (wage_per_hour / 60.0) * minutes_per_ct if wage_per_hour > 0 else 0.0
    material_cost_per_ct = (material_cost / total_output_ct) if total_output_ct > 0 else 0.0

    base_cost = labour_cost_per_ct + material_cost_per_ct
    extra_cost_per_ct = (base_cost * extra_pct / 100.0) if include_extra and extra_pct > 0 else 0.0

    total_cost_per_ct = base_cost + extra_cost_per_ct

    # PROFIT
    profit_per_ct = sell_price_per_ct - total_cost_per_ct
    profit_total = profit_per_ct * total_output_ct

    return {
        "TotalOutputCT": round(total_output_ct, 4),
        "TotalManMinutes": round(total_man_minutes, 4),
        "MinutesPerCT": round(minutes_per_ct, 4),
        "WastageRate": round(wastage_rate, 4),
        "LabourCostPerCT": round(labour_cost_per_ct, 4),
        "MaterialCostPerCT": round(material_cost_per_ct, 4),
        "ExtraCostPerCT": round(extra_cost_per_ct, 4),
        "TotalCostPerCT": round(total_cost_per_ct, 4),
        "ProfitPerCT": round(profit_per_ct, 4),
        "ProfitTotal": round(profit_total, 4),
        "SellPricePerCT_used": round(sell_price_per_ct, 4),
        "CtPerBox_used": round(ct_per_box, 4),
        "PackUnitWeightKg_used": round(pack_unit_kg, 4),
        "RawKg": round(raw_kg, 4),
        "WastageKg": round(wastage_kg, 4),
    }

def save_results_to_batch(batch_item_id: str, calc: dict):
    site_id, list_id = get_site_and_list_id(LIST_P_BATCHES)
    patch = {
        B_OUT_TOTALOUTPUTCT: calc["TotalOutputCT"],
        B_OUT_TOTALMANMIN: calc["TotalManMinutes"],
        B_OUT_MINPERCT: calc["MinutesPerCT"],
        B_OUT_WASTAGERATE: calc["WastageRate"],
        B_OUT_LABOURCOST: calc["LabourCostPerCT"],
        B_OUT_MATERIALCOST: calc["MaterialCostPerCT"],
        B_OUT_EXTRACOST: calc["ExtraCostPerCT"],
        B_OUT_TOTALCOST: calc["TotalCostPerCT"],
        B_OUT_PROFITPERCT: calc["ProfitPerCT"],
        B_OUT_PROFITTOTAL: calc["ProfitTotal"],
    }
    graph_patch_item_fields(site_id, list_id, batch_item_id, patch)

# =========================================================
# UI
# =========================================================
st.set_page_config(page_title="Batch Calculator (SharePoint)", layout="wide")
st.title("Batch Calculator — same logic as your template (SharePoint)")

with st.sidebar:
    st.header("Settings")
    st.caption("Read P_Batches + P_LabourLines, calculate, then write results back to P_Batches.")
    if st.button("Test Graph connection"):
        try:
            _ = graph_get_token()
            st.success("Graph token OK")
            _ = get_site_and_list_id(LIST_P_BATCHES)
            _ = get_site_and_list_id(LIST_P_LABOUR)
            _ = get_site_and_list_id(LIST_PACKTYPES)
            st.success("Lists OK")
        except Exception as e:
            st.error(str(e))

# Date range
c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    start_date = st.date_input("Start date", value=date.today().replace(day=1))
with c2:
    end_date = st.date_input("End date", value=date.today())
with c3:
    load_btn = st.button("Load / Refresh")

if load_btn:
    st.session_state.pop("_batches_df", None)

batches_df = st.session_state.get("_batches_df")
if batches_df is None:
    try:
        batches_df = fetch_batches_df(start_date, end_date)
        st.session_state["_batches_df"] = batches_df
    except Exception as e:
        st.error(str(e))
        batches_df = pd.DataFrame()

if batches_df.empty:
    st.info("No batches in this date range (from P_Batches).")
    st.stop()

# Batch selector
batch_options = batches_df["batch_no"].tolist()
selected_batch = st.selectbox("Select BatchNo", batch_options, index=0)

row = batches_df[batches_df["batch_no"] == selected_batch].iloc[0].to_dict()
batch_item_id = str(row["sp_item_id"])

# PackType info
packtypes = fetch_packtypes_map()
packtype_name = (row.get("pack_type") or "").strip()
packtype_info = packtypes.get(packtype_name) if packtype_name else None

# Labour lines
labour_df = fetch_labour_lines_df(batch_item_id)

# Calculate
calc = calc_template(row, labour_df, packtype_info)

left, right = st.columns([1.2, 1])

with left:
    st.subheader("Inputs (from P_Batches)")
    show_inputs = {
        "BatchNo": row.get("batch_no"),
        "WorkDate": str(row.get("work_date")),
        "PackType": packtype_name,
        "TotalBoxes": row.get("total_boxes"),
        "CtPerBox": row.get("ct_per_box"),
        "LooseCT": row.get("loose_ct"),
        "TotalRawMaterial": row.get("total_raw"),
        "RawMaterialUnit": row.get("raw_unit"),
        "MaterialUnitWeightKg": row.get("unit_weight_kg"),
        "Wastage": row.get("wastage"),
        "WastageUnit": row.get("wastage_unit"),
        "WagePerHour": row.get("wage_per_hour"),
        "MaterialCost": row.get("material_cost"),
        "IncludeExtraCost": row.get("include_extra"),
        "ExtraCostPct": row.get("extra_pct"),
        "SellPricePerCT": row.get("sell_price_per_ct"),
    }
    st.json(show_inputs)

    st.subheader("Labour lines (for this batch)")
    if labour_df.empty:
        st.warning("No labour lines found in P_LabourLines for this batch.")
    else:
        st.dataframe(
            labour_df[["start_time", "end_time", "people", "duration_minutes", "man_minutes"]],
            use_container_width=True
        )

with right:
    st.subheader("Calculated (template logic)")
    st.json({
        "TotalOutputCT": calc["TotalOutputCT"],
        "TotalManMinutes": calc["TotalManMinutes"],
        "MinutesPerCT": calc["MinutesPerCT"],
        "WastageRate(%)": calc["WastageRate"],
        "LabourCostPerCT": calc["LabourCostPerCT"],
        "MaterialCostPerCT": calc["MaterialCostPerCT"],
        "ExtraCostPerCT": calc["ExtraCostPerCT"],
        "TotalCostPerCT": calc["TotalCostPerCT"],
        "ProfitPerCT": calc["ProfitPerCT"],
        "ProfitTotal": calc["ProfitTotal"],
    })

    st.caption("Extra debug (for unit conversion)")
    st.write(f"CtPerBox used: {calc['CtPerBox_used']}")
    st.write(f"PackUnitWeightKg used: {calc['PackUnitWeightKg_used']}")
    st.write(f"RawKg: {calc['RawKg']}, WastageKg: {calc['WastageKg']}")
    st.write(f"SellPricePerCT used: {calc['SellPricePerCT_used']}")

    if st.button("Calculate + Save to P_Batches"):
        try:
            save_results_to_batch(batch_item_id, calc)
            st.success("Saved ✅ (results written back to P_Batches)")
        except Exception as e:
            st.error("Save failed. Usually means output columns not created yet or internal name mismatch.")
            st.error(str(e))
