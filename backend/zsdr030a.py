# zsdr030a.py
from pathlib import Path
from datetime import date

import pandas as pd

from db import engine


def process_zsdr030a(
    file_path: Path, upload_batch_id: str, snapshot_date: date
) -> None:
    # Read Excel
    df = pd.read_excel(file_path)

    # Clean header names (remove trailing spaces, etc.)
    df.columns = df.columns.astype(str).str.strip()

    # Rename to internal names based on your SO layout
    rename_map = {
        "Channel": "channel",
        "Sales Office": "sales_office",
        "Sales doc.": "sales_doc",
        "Document Date": "document_date",
        "Creation Date": "creation_date",
        "SO Type": "so_type",
        "Sold to Number": "sold_to_number",
        "Sold to Name": "sold_to_name",
        "Sold to Country": "sold_to_country",
        "Ship to Number": "ship_to_number",
        "Ship to Name": "ship_to_name",
        "Ship to Country": "ship_to_country",
        "Bill to Number": "bill_to_number",
        "Bill to Name": "bill_to_name",
        "Bill to Country": "bill_to_country",
        "Item": "item",
        "Item type": "item_type",
        "PO number": "po_number",
        "PO Item number": "po_item_number",
        "Material": "material",
        "BRAND": "brand",
        "Mat. Desc.": "material_desc",
        "Storage Location": "storage_location",
        "Unit Price": "unit_price",
        "SO QTY": "so_qty",
        "DN QTY": "dn_qty",
        "PGI QTY": "pgi_qty",
        "To PGI QTY": "to_pgi_qty",
        "Invoiced QTY": "invoiced_qty",
        "To invoice QTY": "to_invoice_qty",
        "Open SO QTY": "open_so_qty",
        "SO Amount": "so_amount",
        "Delivered Amount": "delivered_amount",
        "Inv. Amount": "inv_amount",
        "Inv. Date": "inv_date",
        "SO Open amount": "so_open_amount",
        "FOC": "foc",
        "Cancel Reason": "cancel_reason",
        "Req. deliv.date": "req_deliv_date",
        "Planned GI date": "planned_gi_date",
        "Actual GI date": "actual_gi_date",
        "Item Deliv. status": "item_deliv_status",
        "Delivery status": "delivery_status",
        "Channel code": "channel_code",
        "AcctAssgGr": "acctassgr",
        "Inside Sales#": "inside_sales_no",
        "Inside Sales": "inside_sales",
        "Sales employee#": "sales_employee_no",
        "Sales employee": "sales_employee",
        "Payment term": "payment_term",
        "Delivery Block": "delivery_block",
        "Debit Down Payment": "debit_down_payment",
        "Cleared Down Payment": "cleared_down_payment",
        "Open DP Amount": "open_dp_amount",
        "CRM ID": "crm_id",
        "Related order": "related_order",
        "Related order item": "related_order_item",
        "combination#": "combination_no",
        "Incompl.due to": "incompl_due_to",
        "GLT D&I Fee Item": "glt_di_fee_item",
        "GLT D&I Fee Header": "glt_di_fee_header",
        "MODEL No.": "model_no",
        "Product Series": "product_series",
        "Product Category": "product_category",
        "FOB/Stdprice": "fob_stdprice",
        "Moving Price": "moving_price",
        "Price CTL": "price_ctl",
        "Contract": "contract",
        "Profit%": "profit_percent",
        "Project": "project",
        "Order Comments Header": "order_comments_header",
        "Currency": "currency",
    }

    df = df.rename(columns=rename_map)

    # Normalized helper fields
    # No plant in this layout, so leave werks as NULL
    df["werks"] = pd.NA

    df["lgort"] = (
        df.get("storage_location", pd.NA)
        .astype("string")
        .str.strip()
    )
    df["matnr"] = (
        df.get("material", pd.NA)
        .astype("string")
        .str.strip()
    )
    df["mat_desc"] = (
        df.get("material_desc", pd.NA)
        .astype("string")
        .str.strip()
    )

    # Open quantity = open SO quantity
    df["open_qty"] = pd.to_numeric(df.get("open_so_qty"), errors="coerce")

    # Parse dates (source is dd/mm/yyyy in your sample)
    date_cols = [
        "document_date",
        "creation_date",
        "inv_date",
        "req_deliv_date",
        "planned_gi_date",
        "actual_gi_date",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Numeric columns
    numeric_cols = [
        "unit_price",
        "so_qty",
        "dn_qty",
        "pgi_qty",
        "to_pgi_qty",
        "invoiced_qty",
        "to_invoice_qty",
        "open_so_qty",
        "so_amount",
        "delivered_amount",
        "inv_amount",
        "so_open_amount",
        "debit_down_payment",
        "cleared_down_payment",
        "open_dp_amount",
        "fob_stdprice",
        "moving_price",
        "profit_percent",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Meta columns
    df["upload_batch_id"] = upload_batch_id
    df["snapshot_date"] = snapshot_date
    df["source"] = "ZSDR030A"

    # Columns to persist in the raw table
    raw_cols = [
        # SO / header-level fields
        "channel",
        "sales_office",
        "sales_doc",
        "document_date",
        "creation_date",
        "so_type",
        "sold_to_number",
        "sold_to_name",
        "sold_to_country",
        "ship_to_number",
        "ship_to_name",
        "ship_to_country",
        "bill_to_number",
        "bill_to_name",
        "bill_to_country",
        "item",
        "item_type",
        "po_number",
        "po_item_number",
        "material",
        "brand",
        "material_desc",
        "storage_location",
        "unit_price",
        "so_qty",
        "dn_qty",
        "pgi_qty",
        "to_pgi_qty",
        "invoiced_qty",
        "to_invoice_qty",
        "open_so_qty",
        "so_amount",
        "delivered_amount",
        "inv_amount",
        "inv_date",
        "so_open_amount",
        "foc",
        "cancel_reason",
        "req_deliv_date",
        "planned_gi_date",
        "actual_gi_date",
        "item_deliv_status",
        "delivery_status",
        "channel_code",
        "acctassgr",
        "inside_sales_no",
        "inside_sales",
        "sales_employee_no",
        "sales_employee",
        "payment_term",
        "delivery_block",
        "debit_down_payment",
        "cleared_down_payment",
        "open_dp_amount",
        "crm_id",
        "related_order",
        "related_order_item",
        "combination_no",
        "incompl_due_to",
        "glt_di_fee_item",
        "glt_di_fee_header",
        "model_no",
        "product_series",
        "product_category",
        "fob_stdprice",
        "moving_price",
        "price_ctl",
        "contract",
        "profit_percent",
        "project",
        "order_comments_header",
        "currency",
        # normalized helpers
        "werks",
        "lgort",
        "matnr",
        "mat_desc",
        "open_qty",
        # meta
        "upload_batch_id",
        "snapshot_date",
        "source",
    ]

    # Keep only columns that actually exist (in case future files miss some)
    available_raw_cols = [c for c in raw_cols if c in df.columns]

    raw_df = df[available_raw_cols].copy()
    raw_df.to_sql("raw_zsdr030a", engine, if_exists="append", index=False)

    # For now, fact table = same as raw
    fact_df = raw_df.copy()
    fact_df.to_sql("fact_zsdr030a", engine, if_exists="append", index=False)