from datetime import datetime, date
import pandas as pd
import streamlit as st
from io import BytesIO
import requests
import time

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
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get site failed: {r.status_code} {r.text}")
    site_id = r.json().get("id", "")
    if not site_id:
        raise Exception(f"Site id empty. Raw: {r.text}")
    return site_id

def graph_get_list_id(site_id: str, list_name: str) -> str:
    # cache by list_name
    cache = st.session_state.get("_sp_list_id_cache", {})
    if cache.get(list_name):
        return cache[list_name]

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$top=200"
    r = requests.get(url, headers=graph_headers(), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Get lists failed: {r.status_code} {r.text}")

    for it in r.json().get("value", []):
        if it.get("displayName") == list_name:
            list_id = it.get("id", "")
            if list_id:
                cache[list_name] = list_id
                st.session_state["_sp_list_id_cache"] = cache
                return list_id

    raise Exception(f"List not found: {list_name}")

def get_site_id():
    host = secrets_get("SP_HOST", "")
    site_path = secrets_get("SP_SITE_PATH", "")
    if not host or not site_path:
        raise Exception("Missing secrets: SP_HOST / SP_SITE_PATH")
    site_id = st.session_state.get("_sp_site_id")
    if not site_id:
        site_id = graph_get_site_id(host, site_path)
        st.session_state["_sp_site_id"] = site_id
    return site_id

# ============================
# Graph: read list items (auto paging)
# ============================
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

# ============================
# helpers
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

def _parse_date_any(v):
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
        # dd/mm/yyyy or mm/dd/yyyy (NZ more likely dayfirst)
        if "/" in s:
            d = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.notna(d):
                return d.date()
    except Exception:
        return None
    return None

def _calc_amount(qty, qtykg, unitcost):
    if unitcost is None or unitcost == "":
        return 0.0
    uc = _to_float(unitcost, 0)
    qkg = _to_float(qtykg, 0)
    q = _to_float(qty, 0)
    if qkg > 0:
        return qkg * uc
    return q * uc

def _safe_div(a, b):
    a = float(a)
    b = float(b)
    return a / b if b != 0 else 0.0

# ============================
# fetch list dfs
# ============================
def fetch_products_df() -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_PRODUCTS", "Products")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=5000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        rows.append({
            "product_item_id": it.get("id"),
            "product_title": _to_text(f.get("Title")),
            "sku": _to_text(f.get("SKU")),
            "default_sell_price": _to_float(f.get("DefaultSellPrice"), 0),
            "sell_price_unit": _to_text(f.get("SellPriceUnit")),
            "default_hourly_rate": _to_float(f.get("DefaultHourlyRate"), 0),
            "kg_per_ct": _to_float(f.get("KgPerCT"), 0),
            "punnets_per_ct": _to_float(f.get("PunnetsPerCT"), 0),
            "kg_per_box": _to_float(f.get("KgPerBox"), 0),
            "overhead_pct_default": _to_float(f.get("OverheadPctDefault"), 0),
            "active": _to_text(f.get("Active")),
        })
    df = pd.DataFrame(rows)
    return df

def fetch_batches_df(start: date, end: date) -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_BATCHES", "P_Batches")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=5000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        wd = _parse_date_any(f.get("WorkDate"))
        if not wd:
            continue
        if wd < start or wd > end:
            continue

        # lookup fields: usually both text + LookupId exist
        product_name = _to_text(f.get("ProductName"))
        supplier_name = _to_text(f.get("SupplierName"))

        rows.append({
            "batch_item_id": it.get("id"),
            "batch_no": _to_text(f.get("Title")),
            "work_date": wd,
            "receive_date": _parse_date_any(f.get("ReceiveDate")),
            "product_name": product_name,
            "supplier_name": supplier_name,
            "line": _to_text(f.get("Line")),
            "shift": _to_text(f.get("Shift")),
            "operator": _to_text(f.get("Operator")),
            "overhead_pct": _to_float(f.get("OverheadPct"), 0),
            "overhead_note": _to_text(f.get("OverheadNote")),
            "status": _to_text(f.get("Status")),
            "note": _to_text(f.get("Note")),
        })
    return pd.DataFrame(rows)

def fetch_batchlines_df() -> pd.DataFrame:
    # lines usually no date, so we fetch all then join by batch list
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_BATCHLINES", "P_BatchLines")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=8000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        batch_no_text = _to_text(f.get("BatchNo"))
        batch_lookup_text = _to_text(f.get("Batch"))  # sometimes contains Title
        rows.append({
            "line_item_id": it.get("id"),
            "batch_no": batch_lookup_text if batch_lookup_text else batch_no_text,
            "line_type": _to_text(f.get("LineType")).upper(),
            "item_name": _to_text(f.get("ItemName")),
            "qty": _to_float(f.get("QTY"), 0),
            "uom": _to_text(f.get("UOM")).upper(),
            "qtykg": _to_float(f.get("QtyKg"), 0),
            "unit_cost": _to_float(f.get("UnitCost"), 0),
            "amount": _to_float(f.get("Amount"), 0),
            "sell_price": _to_float(f.get("SellPrice"), 0),
            "note": _to_text(f.get("Note")),
        })
    return pd.DataFrame(rows)

def fetch_packingrecords_df(start: date, end: date) -> pd.DataFrame:
    site_id = get_site_id()
    list_name = secrets_get("SP_LIST_PACKINGRECORDS", "PackingRecords")
    list_id = graph_get_list_id(site_id, list_name)
    items = graph_list_items_all(site_id, list_id, top=8000)

    rows = []
    for it in items:
        f = it.get("fields") or {}
        wd = _parse_date_any(f.get("WorkDate"))
        if not wd:
            continue
        if wd < start or wd > end:
            continue

        batch_no = _to_text(f.get("BatchNo"))  # lookup text usually appears here
        rows.append({
            "packing_item_id": it.get("id"),
            "work_date": wd,
            "batch_no": batch_no,
            "pack_type": _to_text(f.get("PackType")),
            "minutes": _to_float(f.get("Minutes"), 0),
            "people": _to_float(f.get("People"), 0),
            "finished_punnets": _to_float(f.get("FinishedPunnets"), 0),
            "waste_punnets": _to_float(f.get("WastePunnets"), 0),
            "downtime_minutes": _to_float(f.get("DowntimeMinutes"), 0),
            "note": _to_text(f.get("Note")),
        })
    return pd.DataFrame(rows)

# ============================
# price conversion
# ============================
def unit_price_for_output(product_row: dict, output_uom: str) -> float:
    """
    Return unit price that matches output_uom
    product_row: from Products
    output_uom: "PUNNET"/"CT"/"KG"
    """
    p = float(product_row.get("default_sell_price", 0) or 0)
    unit = str(product_row.get("sell_price_unit", "") or "").upper()
    kgct = float(product_row.get("kg_per_ct", 0) or 0)
    punct = float(product_row.get("punnets_per_ct", 0) or 0)

    if p <= 0 or not unit or not output_uom:
        return 0.0

    output_uom = output_uom.upper()

    if unit == output_uom:
        return p

    # derive kg per punnet if possible
    kg_per_punnet = (kgct / punct) if (kgct > 0 and punct > 0) else 0

    # conversions
    if unit == "PUNNET" and output_uom == "CT":
        return p * punct if punct > 0 else 0.0
    if unit == "CT" and output_uom == "PUNNET":
        return (p / punct) if punct > 0 else 0.0

    if unit == "KG" and output_uom == "CT":
        return p * kgct if kgct > 0 else 0.0
    if unit == "CT" and output_uom == "KG":
        return (p / kgct) if kgct > 0 else 0.0

    if unit == "KG" and output_uom == "PUNNET":
        return p * kg_per_punnet if kg_per_punnet > 0 else 0.0
    if unit == "PUNNET" and output_uom == "KG":
        return (p / kg_per_punnet) if kg_per_punnet > 0 else 0.0

    return 0.0

# ============================
# main profit calc
# ============================
def build_batch_profit(batches, lines, labour, products, default_hourly, default_overhead_pct):
    if batches.empty:
        return pd.DataFrame()

    prod_map = {}
    if not products.empty:
        for _, r in products.iterrows():
            prod_map[str(r["product_title"])] = r.to_dict()

    # labour hours per batch
    if labour.empty:
        labour_sum = pd.DataFrame(columns=["batch_no", "labour_hours"])
    else:
        labour["labour_hours"] = (labour["minutes"].astype(float) * labour["people"].astype(float)) / 60.0
        labour_sum = labour.groupby("batch_no", dropna=False)["labour_hours"].sum().reset_index()

    # lines
    if lines.empty:
        lines = pd.DataFrame(columns=["batch_no","line_type","qty","uom","qtykg","unit_cost","amount","sell_price"])

    # normalize amount
    lines = lines.copy()
    lines["amount_calc"] = lines.apply(
        lambda x: x["amount"] if float(x.get("amount", 0) or 0) > 0 else _calc_amount(x.get("qty"), x.get("qtykg"), x.get("unit_cost")),
        axis=1
    )

    out_rows = []
    for _, b in batches.iterrows():
        batch_no = str(b["batch_no"])
        prod_name = str(b.get("product_name", "") or "")

        prod = prod_map.get(prod_name, {})
        hourly_rate = float(prod.get("default_hourly_rate", 0) or 0)
        if hourly_rate <= 0:
            hourly_rate = float(default_hourly)

        overhead_pct = float(b.get("overhead_pct", 0) or 0)
        if overhead_pct <= 0:
            overhead_pct = float(prod.get("overhead_pct_default", 0) or 0)
        if overhead_pct <= 0:
            overhead_pct = float(default_overhead_pct)

        # lines for this batch
        bl = lines[lines["batch_no"] == batch_no]

        raw_cost = bl.loc[bl["line_type"] == "RAW_IN", "amount_calc"].sum()
        pack_cost = bl.loc[bl["line_type"] == "PACK_MAT", "amount_calc"].sum()

        # revenue from OUTPUT
        out_lines = bl[bl["line_type"] == "OUTPUT"].copy()
        revenue = 0.0
        missing_price_flag = False
        for _, ln in out_lines.iterrows():
            qty = float(ln.get("qty", 0) or 0)
            uom = str(ln.get("uom", "") or "").upper()
            sp = float(ln.get("sell_price", 0) or 0)
            if sp <= 0:
                sp = unit_price_for_output(prod, uom)
                if sp <= 0:
                    missing_price_flag = True
            revenue += qty * sp

        # labour
        lh_row = labour_sum[labour_sum["batch_no"] == batch_no]
        labour_hours = float(lh_row["labour_hours"].iloc[0]) if len(lh_row) else 0.0
        labour_cost = labour_hours * hourly_rate

        overhead_base = raw_cost + pack_cost + labour_cost
        overhead = overhead_base * overhead_pct / 100.0

        total_cost = overhead_base + overhead
        profit = revenue - total_cost
        margin = _safe_div(profit, revenue) if revenue > 0 else 0.0

        out_rows.append({
            "batch_no": batch_no,
            "work_date": b["work_date"],
            "product": prod_name,
            "supplier": b.get("supplier_name", ""),
            "revenue": round(revenue, 2),
            "raw_cost": round(raw_cost, 2),
            "pack_cost": round(pack_cost, 2),
            "labour_hours": round(labour_hours, 2),
            "hourly_rate": round(hourly_rate, 2),
            "labour_cost": round(labour_cost, 2),
            "overhead_pct": round(overhead_pct, 2),
            "overhead": round(overhead, 2),
            "total_cost": round(total_cost, 2),
            "profit": round(profit, 2),
            "margin": round(margin * 100, 2),
            "missing_price": "YES" if missing_price_flag else "",
        })

    df = pd.DataFrame(out_rows)
    df = df.sort_values(["work_date", "batch_no"], ascending=[False, False])
    return df

def to_excel_bytes(batches_df, lines_df, labour_df, profit_df) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        profit_df.to_excel(writer, index=False, sheet_name="Batch_Profit")
        batches_df.to_excel(writer, index=False, sheet_name="Batches")
        lines_df.to_excel(writer, index=False, sheet_name="BatchLines")
        labour_df.to_excel(writer, index=False, sheet_name="Labour")
    return output.getvalue()

# ============================
# UI
# ============================
st.set_page_config(page_title="Processing Profit (SharePoint)", layout="wide")
st.title("Processing system → Batch Profit (SharePoint)")

st.sidebar.header("Defaults (fallback)")
default_hourly = st.sidebar.number_input("Default hourly rate ($/hour)", min_value=0.0, value=30.0, step=0.5)
default_overhead_pct = st.sidebar.number_input("Default overhead %", min_value=0.0, value=10.0, step=0.5)

with st.sidebar.expander("Test SharePoint connection"):
    if st.button("Test Graph token + site"):
        try:
            token = graph_get_token()
            st.success(f"Token OK (len={len(token)})")
            site_id = get_site_id()
            st.success(f"Site OK: {site_id[:28]}...")
        except Exception as e:
            st.error(str(e))

tab1, tab2 = st.tabs(["Batch Profit", "Export Excel"])

with tab1:
    st.subheader("Batch Profit (by WorkDate)")
    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("Start date", value=date.today().replace(day=1))
    with c2:
        end = st.date_input("End date", value=date.today())

    if st.button("Refresh"):
        for k in ["_cache_products", "_cache_batches", "_cache_lines", "_cache_labour", "_cache_key"]:
            st.session_state.pop(k, None)

    cache_key = f"{start.isoformat()}_{end.isoformat()}"
    if st.session_state.get("_cache_key") != cache_key:
        try:
            products = fetch_products_df()
            batches = fetch_batches_df(start, end)
            lines = fetch_batchlines_df()
            labour = fetch_packingrecords_df(start, end)

            st.session_state["_cache_products"] = products
            st.session_state["_cache_batches"] = batches
            st.session_state["_cache_lines"] = lines
            st.session_state["_cache_labour"] = labour
            st.session_state["_cache_key"] = cache_key
        except Exception as e:
            st.error(str(e))
            products = pd.DataFrame()
            batches = pd.DataFrame()
            lines = pd.DataFrame()
            labour = pd.DataFrame()
    else:
        products = st.session_state.get("_cache_products", pd.DataFrame())
        batches = st.session_state.get("_cache_batches", pd.DataFrame())
        lines = st.session_state.get("_cache_lines", pd.DataFrame())
        labour = st.session_state.get("_cache_labour", pd.DataFrame())

    profit = build_batch_profit(batches, lines, labour, products, default_hourly, default_overhead_pct)

    if profit.empty:
        st.info("No batch profit data in this date range. Check: P_Batches has WorkDate, and BatchLines/ PackingRecords have batch linked.")
    else:
        # quick summary cards
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Batches", len(profit))
        c2.metric("Revenue", f"${profit['revenue'].sum():,.2f}")
        c3.metric("Total cost", f"${profit['total_cost'].sum():,.2f}")
        c4.metric("Profit", f"${profit['profit'].sum():,.2f}")

        st.dataframe(profit, use_container_width=True)

        st.caption("missing_price=YES means some OUTPUT lines have no SellPrice and product default price/unit conversion also missing.")

with tab2:
    st.subheader("Export Excel")
    c1, c2 = st.columns(2)
    with c1:
        ex_start = st.date_input("Export start date", value=date.today().replace(day=1), key="ex_start")
    with c2:
        ex_end = st.date_input("Export end date", value=date.today(), key="ex_end")

    try:
        products = fetch_products_df()
        batches = fetch_batches_df(ex_start, ex_end)
        lines = fetch_batchlines_df()
        labour = fetch_packingrecords_df(ex_start, ex_end)
        profit = build_batch_profit(batches, lines, labour, products, default_hourly, default_overhead_pct)
    except Exception as e:
        st.error(str(e))
        products = pd.DataFrame()
        batches = pd.DataFrame()
        lines = pd.DataFrame()
        labour = pd.DataFrame()
        profit = pd.DataFrame()

    if profit.empty:
        st.info("No data to export.")
    else:
        st.write("Preview:")
        st.dataframe(profit.head(50), use_container_width=True)

        file_bytes = to_excel_bytes(batches, lines, labour, profit)
        fname = f"batch_profit_{ex_start.isoformat()}_to_{ex_end.isoformat()}.xlsx"
        st.download_button(
            label="Download Excel",
            data=file_bytes,
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
