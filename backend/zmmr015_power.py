# zmmr015_power.py
from pathlib import Path
from datetime import date

import pandas as pd

from db import engine


def process_zmmr015_power(
    file_path: Path, upload_batch_id: str, snapshot_date: date
) -> None:
    df = pd.read_excel(file_path)

    # Normalize column names (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]

    # --- Choose correct column names depending on file layout ---

    # Material: old files might use "Material", new ones "Material No."
    if "Material" in df.columns:
        material_col = "Material"
    elif "Material No." in df.columns:
        material_col = "Material No."
    else:
        raise ValueError("ZMMR015 Power: no 'Material' or 'Material No.' column found")

    # Date of income: can be "Date Of Income" or "Date of income"
    date_income_col = None
    for c in ["Date Of Income", "Date of income"]:
        if c in df.columns:
            date_income_col = c
            break

    # Aging Qty: can be "Aging Qty" or "Aging Qty.j"
    aging_qty_col = None
    for c in ["Aging Qty", "Aging Qty.j"]:
        if c in df.columns:
            aging_qty_col = c
            break

    # Aging Val: can be "Aging Val", "Aging Val.j", or "Aging Value"
    aging_val_col = None
    for c in ["Aging Val", "Aging Val.j", "Aging Value"]:
        if c in df.columns:
            aging_val_col = c
            break

    # --- Map to unified fields ---

    # Plant / werks
    df["werks"] = df["Plant"].astype(str).str.strip()

    # Material number and description
    df["matnr"] = df[material_col].astype(str).str.strip()
    df["mat_desc"] = df["Description"].astype(str).str.strip()

    # Numeric quantities/values
    if aging_qty_col is not None:
        df["aging_qty"] = pd.to_numeric(df[aging_qty_col], errors="coerce")
    else:
        df["aging_qty"] = pd.NA

    if aging_val_col is not None:
        df["aging_val"] = pd.to_numeric(df[aging_val_col], errors="coerce")
    else:
        df["aging_val"] = pd.NA

    # Date of income
    if date_income_col is not None:
        df["date_of_income"] = pd.to_datetime(
            df[date_income_col], errors="coerce"
        ).dt.date
    else:
        df["date_of_income"] = pd.NaT

    # Common metadata
    df["upload_batch_id"] = upload_batch_id
    df["snapshot_date"] = snapshot_date
    df["source"] = "ZMMR015_POWER"

    # --- Write raw table ---
    df.to_sql("raw_zmmr015_power", engine, if_exists="append", index=False)

    # --- Write fact table (only the fields we need) ---
    fact_df = pd.DataFrame(
        {
            "upload_batch_id": df["upload_batch_id"],
            "werks": df["werks"],
            "matnr": df["matnr"],
            "mat_desc": df["mat_desc"],
            "date_of_income": df["date_of_income"],
            "aging_qty": df["aging_qty"],
            "aging_val": df["aging_val"],
            "snapshot_date": df["snapshot_date"],
            "source": df["source"],
        }
    )

    fact_df.to_sql("fact_zmmr015_power", engine, if_exists="append", index=False)