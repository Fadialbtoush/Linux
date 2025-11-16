# zmmr014.py

import os
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np   # <-- NEW
from sqlalchemy import create_engine

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://sapuser:sap_password@db:3306/sap_reporting",
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def process_zmmr014(file_path: Path, upload_batch_id: str, snapshot_date: date) -> None:
    """
    Load ZMMR014 Excel and write to:
      - raw_zmmr014 (detail)
      - fact_inventory_snapshot (fact table compatible with MB52)
      - fact_aging (full aging fact table)
    """

    df = pd.read_excel(file_path)

    # Map your exact header names to internal names
    df = df.rename(
        columns={
            # keys
            "Plant": "plant",
            "Material#": "material",
            "Material #": "material",
            "Material": "material",
            "Model No.": "model_no",
            "Prod. Hierachy": "prod_hierarchy",
            "Prod. Hierarchy": "prod_hierarchy",
            "Material Type": "material_type",
            "Description": "description",
            "Prod. Group": "prod_group",
            "Prod. Cat..": "prod_cat",
            "Prod. Cat.": "prod_cat",
            "Prod. Line": "prod_line",
            "MOVEMENT TYPE": "movement_type",
            "Movement type": "movement_type",
            "MOVEMENT DESC.": "movement_desc",
            "Movement Desc.": "movement_desc",
            "Date of Income": "date_of_income_raw",
            "Date of income": "date_of_income_raw",
            "Days": "days_raw",
            "Aging Qty.": "aging_qty_raw",
            "Std. Price": "std_price_raw",
            "Std Price": "std_price_raw",
            "Std price": "std_price_raw",
            "Currency": "currency",
            "Aging Val.": "aging_val_raw",
            "Report Date": "report_date_raw",
            "Report Time": "report_time",
            "ZMMR015 Power": "zmmr015_power",
            "Odoo": "odoo",
            "Final Aging": "final_aging",
        }
    )

    # ---- generic keys for fact table ----
    df["werks"] = df["plant"].astype(str).str.strip()
    df["matnr"] = df["material"].astype(str).str.strip()
    df["mat_desc"] = df["description"].astype(str).str.strip()

    # ---- numeric columns ----
    df["aging_qty"] = pd.to_numeric(df.get("aging_qty_raw"), errors="coerce")
    df["aging_val"] = pd.to_numeric(df.get("aging_val_raw"), errors="coerce")
    df["std_price"] = pd.to_numeric(df.get("std_price_raw"), errors="coerce")
    df["days"] = pd.to_numeric(df.get("days_raw"), errors="coerce").astype("Int64")

    # ---- date columns ----
    df["date_of_income"] = pd.to_datetime(
        df.get("date_of_income_raw"), errors="coerce", dayfirst=True
    ).dt.date
    df["report_date"] = pd.to_datetime(
        df.get("report_date_raw"), errors="coerce", dayfirst=True
    ).dt.date

    # ---- build raw_zmmr014 ----
    raw_df = pd.DataFrame(
        {
            "upload_batch_id": upload_batch_id,
            "bukrs": None,
            "werks": df["werks"],
            "lgort": None,
            "matnr": df["matnr"],
            "mat_desc": df["mat_desc"],
            "charg": None,
            "qty": df["aging_qty"],
            "value_unrestricted": df["aging_val"],
            "meins": None,
            "plant": df["plant"],
            "material": df["material"],
            "model_no": df.get("model_no"),
            "prod_hierarchy": df.get("prod_hierarchy"),
            "material_type": df.get("material_type"),
            "description": df["description"],
            "prod_group": df.get("prod_group"),
            "prod_cat": df.get("prod_cat"),
            "prod_line": df.get("prod_line"),
            "movement_type": df.get("movement_type"),
            "movement_desc": df.get("movement_desc"),
            "date_of_income": df["date_of_income"],
            "days": df["days"],
            "aging_qty": df["aging_qty"],
            "std_price": df["std_price"],
            "currency": df.get("currency"),
            "aging_val": df["aging_val"],
            "report_date": df["report_date"],
            "report_time": df.get("report_time"),
            "zmmr015_power": df.get("zmmr015_power"),
            "odoo": df.get("odoo"),
            "final_aging": df.get("final_aging"),
            "snapshot_date": snapshot_date,
            "source": "ZMMR014",
        }
    )

    raw_df.to_sql("raw_zmmr014", engine, if_exists="append", index=False)

    # ---- insert to common fact_inventory_snapshot ----
    fact_df = pd.DataFrame(
        {
            "upload_batch_id": upload_batch_id,
            "bukrs": raw_df["bukrs"],
            "werks": raw_df["werks"],
            "lgort": raw_df["lgort"],
            "matnr": raw_df["matnr"],
            "mat_desc": raw_df["mat_desc"],
            "charg": raw_df["charg"],
            "qty": raw_df["qty"].fillna(0),
            "value_unrestricted": raw_df["value_unrestricted"].fillna(0),
            "meins": raw_df["meins"],
            "snapshot_date": raw_df["snapshot_date"],
            "source": "ZMMR014",
            "total_value": raw_df["value_unrestricted"].fillna(0),
        }
    )

    fact_df.to_sql("fact_inventory_snapshot", engine, if_exists="append", index=False)

    # ---- NEW: build fact_aging ----
    fact_aging_df = pd.DataFrame(
        {
            "source": raw_df["source"],
            "upload_batch_id": raw_df["upload_batch_id"],
            "snapshot_date": raw_df["snapshot_date"],
            "bukrs": raw_df["bukrs"],
            "werks": raw_df["werks"],
            "lgort": raw_df["lgort"],
            "matnr": raw_df["matnr"],
            "mat_desc": raw_df["mat_desc"],
            "date_of_income": raw_df["date_of_income"],
            "days": raw_df["days"],
            "aging_qty": raw_df["aging_qty"],
            "std_price": raw_df["std_price"],
            "currency": raw_df["currency"],
            "aging_val": raw_df["aging_val"],
        }
    )

    # derive aging_years and aging_bucket
    fact_aging_df["aging_years"] = fact_aging_df["days"].astype(float) / 365.0

    conditions = [
        fact_aging_df["aging_years"] <= 1,
        (fact_aging_df["aging_years"] > 1) & (fact_aging_df["aging_years"] <= 2),
        (fact_aging_df["aging_years"] > 2) & (fact_aging_df["aging_years"] <= 5),
        (fact_aging_df["aging_years"] > 5) & (fact_aging_df["aging_years"] <= 7),
        (fact_aging_df["aging_years"] > 7) & (fact_aging_df["aging_years"] <= 10),
    ]
    choices = ["0-1Y", "1-2Y", "3-5Y", "5-7Y", "7-10Y"]

    fact_aging_df["aging_bucket"] = np.select(
        conditions, choices, default="10+Y"
    )

    fact_aging_df.to_sql("fact_aging", engine, if_exists="append", index=False)