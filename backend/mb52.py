# mb52.py
from pathlib import Path
from datetime import date

import pandas as pd

from db import engine, ensure_core_tables


def process_mb52(file_path: Path, upload_batch_id: str, snapshot_date: date) -> None:
    """
    Load MB52 Excel, clean it, and insert into raw_mb52 and fact_inventory_snapshot.
    """
    ensure_core_tables()

    df = pd.read_excel(file_path)

    column_map = {
        "Company Code": "bukrs",
        "Plant": "werks",
        "Storage Location": "lgort",
        "Material": "matnr",
        "Material Description": "mat_desc",
        "Batch": "charg",
        "Unrestricted": "labst",
        "Value Unrestricted": "value_unrestricted",
        "Base Unit of Measure": "meins",
    }
    df = df.rename(columns=column_map)

    for col in [
        "bukrs",
        "werks",
        "lgort",
        "matnr",
        "mat_desc",
        "charg",
        "labst",
        "value_unrestricted",
        "meins",
    ]:
        if col not in df.columns:
            df[col] = None

    for col in ["labst", "value_unrestricted"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    raw_df = pd.DataFrame(
        {
            "upload_batch_id": upload_batch_id,
            "bukrs": df.get("bukrs"),
            "werks": df.get("werks"),
            "lgort": df.get("lgort"),
            "matnr": df.get("matnr"),
            "mat_desc": df.get("mat_desc"),
            "charg": df.get("charg"),
            "labst": df.get("labst"),
            "value_unrestricted": df.get("value_unrestricted"),
            "meins": df.get("meins"),
            "snapshot_date": snapshot_date,
        }
    )

    raw_df.to_sql("raw_mb52", engine, if_exists="append", index=False)

    fact_df = pd.DataFrame(
        {
            "upload_batch_id": upload_batch_id,
            "bukrs": raw_df["bukrs"],
            "werks": raw_df["werks"],
            "lgort": raw_df["lgort"],
            "matnr": raw_df["matnr"],
            "mat_desc": raw_df["mat_desc"],
            "charg": raw_df["charg"],
            "qty": raw_df["labst"].fillna(0),
            "value_unrestricted": raw_df["value_unrestricted"].fillna(0),
            "meins": raw_df["meins"],
            "snapshot_date": raw_df["snapshot_date"],
            "source": "MB52",
            "total_value": raw_df["value_unrestricted"].fillna(0),
        }
    )

    fact_df.to_sql("fact_inventory_snapshot", engine, if_exists="append", index=False)
