import pandas as pd
from pathlib import Path
from datetime import date
from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://sapuser:sap_password@db:3306/sap_reporting",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def _process_zmm345e(file_path: Path, upload_batch_id: str, snapshot_date: date) -> None:
    """
    Process ZMM345E (Material Master by Plant / SLoc).
    Produces:
      - raw_zmm345e
      - fact_zmm345e
    """
    df = pd.read_excel(file_path)

    # Normalize SAP column names
    df = df.rename(columns={
        "Material": "material",
        "Indst. Sector": "industry_sector",
        "Matr type": "mat_type",
        "Plant": "plant",
        "SLocation": "sloc",
        "Sales Org.": "sales_org",
        "Dist. Channel": "dist_channel",
        "Description": "description",
        "Base UOM": "base_uom",
        "Matr Group": "mat_group",
        "Old part No.": "old_part_no",
        "Division": "division",
        "Item cate.(BASIC)": "item_category_basic",
    })

    # Standard SAP keys
    df["werks"] = df["plant"].astype(str).str.strip()

    # Convert sloc to proper 4-character text
    def _sloc_to_str(v):
        if pd.isna(v):
            return None
        try:
            return f"{int(v):04d}"
        except Exception:
            return str(v).strip()

    df["lgort"] = df["sloc"].apply(_sloc_to_str)

    df["matnr"] = df["material"].astype(str).str.strip()
    df["mat_desc"] = df["description"].astype(str).str.strip()

    # Metadata
    df["upload_batch_id"] = upload_batch_id
    df["snapshot_date"] = snapshot_date
    df["source"] = "ZMM345E"

    # --------------- RAW TABLE ---------------
    raw_cols = [
        "upload_batch_id", "material", "industry_sector", "mat_type",
        "plant", "sloc", "sales_org", "dist_channel",
        "description", "base_uom", "mat_group", "old_part_no",
        "division", "item_category_basic",
        "snapshot_date", "source"
    ]
    raw_cols = [c for c in raw_cols if c in df.columns]

    raw_df = df[raw_cols].copy()
    raw_df.to_sql("raw_zmm345e", engine, if_exists="append", index=False)

    # --------------- FACT TABLE ---------------
    fact_df = pd.DataFrame({
        "upload_batch_id": upload_batch_id,
        "werks": df["werks"],
        "lgort": df["lgort"],
        "matnr": df["matnr"],
        "mat_desc": df["mat_desc"],
        "material_type": df.get("mat_type"),
        "material_group": df.get("mat_group"),
        "base_uom": df.get("base_uom"),
        "snapshot_date": df["snapshot_date"],
        "source": df["source"],
    })

    fact_df.to_sql("fact_zmm345e", engine, if_exists="append", index=False)
