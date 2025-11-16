from pathlib import Path
from datetime import date

import pandas as pd

from db import engine


def _find_col(df: pd.DataFrame, *candidates: str):
    """
    Return the real column name in df that matches any of the candidate
    names (case-insensitive, trimmed). If nothing matches, return None.
    """
    normalize = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in normalize:
            return normalize[key]
    return None


def _get_series(df: pd.DataFrame, *candidates: str) -> pd.Series:
    """
    Return a Series for the first matching column.
    If not found, return a Series of None so downstream code still works.
    """
    col = _find_col(df, *candidates)
    if col is None:
        return pd.Series([None] * len(df))
    return df[col]


def process_zsdr004(
    file_path: Path, upload_batch_id: str, snapshot_date: date
) -> None:
    # Read Excel as-is
    df = pd.read_excel(file_path)

    # Optional sales fields
    df["sales_org"] = _get_series(df, "Sales Organization")
    df["sales_office"] = _get_series(df, "Sales Office")
    df["sales_group"] = _get_series(df, "Sales Group")

    # Material & description (but DO NOT create new 'material' column)
    material_series = _get_series(df, "Material")
    material_desc_series = _get_series(
        df,
        "Material Description",
        "Description(EN)",
        "Description (EN)",
        "Material description",
        "Mat. Description",
    )

    # Plant / werks
    plant_series = _get_series(df, "Plant", "Plant.")
    if plant_series.notna().any():
        df["werks"] = plant_series.astype(str).str.strip()
    else:
        df["werks"] = None

    # Numeric fields
    df["item_net_value_usd"] = _get_series(
        df,
        "Item net value (USD)",
        "PO Price",      # fallback
        "Unit Price",    # second fallback
    )
    df["order_quantity"] = _get_series(
        df,
        "Order quantity",
        "Quantity",      # your file
    )
    df["sales_quantity"] = _get_series(
        df,
        "SLS qty",
        "Invoice Qty",   # your file
    )

    df["item_net_value_usd_num"] = pd.to_numeric(
        df["item_net_value_usd"], errors="coerce"
    )
    df["order_quantity_num"] = pd.to_numeric(
        df["order_quantity"], errors="coerce"
    )
    df["sales_quantity_num"] = pd.to_numeric(
        df["sales_quantity"], errors="coerce"
    )

    # Dates
    df["billing_date_raw"] = _get_series(df, "Billing date")
    df["billing_date"] = pd.to_datetime(
        df["billing_date_raw"], errors="coerce"
    ).dt.date

    # Normalized material info (does not conflict with Excel columns)
    df["matnr"] = material_series.astype(str).str.strip()
    df["mat_desc"] = material_desc_series.astype(str).str.strip()

    # Meta columns
    df["upload_batch_id"] = upload_batch_id
    df["snapshot_date"] = snapshot_date
    df["source"] = "ZSDR004"

    # Store raw data (original Excel columns + meta/normalized fields)
    df.to_sql("raw_zsdr004", engine, if_exists="append", index=False)

    # Fact table
    fact_df = pd.DataFrame(
        {
            "upload_batch_id": df["upload_batch_id"],
            "sales_org": df["sales_org"],
            "sales_office": df["sales_office"],
            "sales_group": df["sales_group"],
            "werks": df["werks"],
            "matnr": df["matnr"],
            "mat_desc": df["mat_desc"],
            "billing_date": df["billing_date"],
            "item_net_value_usd": df["item_net_value_usd_num"],
            "order_quantity": df["order_quantity_num"],
            "sales_quantity": df["sales_quantity_num"],
            "snapshot_date": df["snapshot_date"],
            "source": df["source"],
        }
    )

    fact_df.to_sql("fact_zsdr004", engine, if_exists="append", index=False)
