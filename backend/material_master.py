# material_master.py Fadi
from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import text

from db import engine


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------


def _read_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)


def _ensure_columns(df: pd.DataFrame, required: list[str], source_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{source_name}: missing required columns: {', '.join(missing)}"
        )


def _normalize_str(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .replace({"": pd.NA})
    )


# ------------------------------------------------------
# Main builder
# ------------------------------------------------------


def build_material_master(
    zmm345e_path: Path,
    storage_location_path: Path,
    material_group_path: Path,
    material_type_path: Path,
    mkvz_path: Path,
    upload_batch_id: str,
    snapshot_date: date,
) -> None:
    """
    Build unified dim_material_master from:
      - ZMM345E (main material master)
      - Storage Location table
      - Material Group table
      - Material Type table
      - MKVZ vendor table
    """

    # ---------------- ZMM345E (main material) ----------------
    zmm = _read_excel(zmm345e_path)
    zmm.columns = zmm.columns.astype(str).str.strip()

    required_zmm_cols = [
        "Material",
        "Description",
        "Base UOM",
        "Matr Group",
        "SLocation",
        "Serial Number Profile",
        "Brand",
        "ProductLine",
        "ProductGroup",
        "ProductSeries",
        "Stand. Price",
        "Price contl",
        "Old part No.",
        "Vendor",
        "MTyp",
    ]
    _ensure_columns(zmm, required_zmm_cols, "ZMM345E")

    zmm_renamed = zmm.rename(
        columns={
            "Material": "material_code",
            "Indst. Sector": "industry_sector",
            "Matr type": "material_type_code",
            "Plant": "plant",
            "SLocation": "sloc",
            "Sales Org.": "sales_org",
            "Dist. Channel": "dist_channel",
            "Description": "description",
            "Base UOM": "base_uom",
            "Matr Group": "material_group_code",
            "Old part No.": "old_part_no",
            "Division": "division",
            "Item cate.(BASIC)": "item_category_basic",
            "Product hierarchy": "product_hierarchy",
            "Model number": "model_number",
            "Delivery Plant": "delivery_plant",
            "Tax data": "tax_data",
            "Matr Stat Group": "material_status_group",
            "Material Pricing Group": "material_pricing_group",
            "Account Assignment Group": "account_assignment_group",
            "Item cate.(SALES)": "item_category_sales",
            "Availability check": "availability_check",
            "Profit Center": "profit_center",
            "Serial Number Profile": "serial_number_profile",
            "Purch. group": "purchasing_group",
            "Plant Status": "plant_status",
            "Auto PO": "auto_po",
            "MRP Group": "mrp_group",
            "MRP Type": "mrp_type",
            "MRP Controller": "mrp_controller",
            "Lot Size": "lot_size",
            "Procu. Type": "procurement_type",
            "Issue SLoc.": "issue_sloc",
            "SLocation for EP": "sloc_ep",
            "InhseProd Time": "inhouse_production_time",
            "Planned del.time": "planned_delivery_time",
            "GR proc time": "gr_processing_time",
            "Schdl. Margin key": "schedule_margin_key",
            "Safety Stock": "safety_stock",
            "Strategy Group": "strategy_group",
            "Consumption Mode": "consumption_mode",
            "Consumption period: back.": "consumption_period_back",
            "Consumption period: for.": "consumption_period_for",
            "Individual/Coll": "individual_collective",
            "Valua. class": "valuation_class",
            "Price contl": "price_control",
            "Price unit": "price_unit",
            "Stand. Price": "standard_price",
            "Material Origin": "material_origin",
            "Overhead Group": "overhead_group",
            "Storage Bin": "storage_bin",
            "Cross-Plant Material Status": "cross_plant_material_status",
            "Basic view DF": "basic_view_df",
            "Plant view DF": "plant_view_df",
            "Brand": "brand",
            "ProductLine": "product_line",
            "ProductGroup": "product_group",
            "ProductSeries": "product_series",
            "Vendor": "vendor_code",
        }
    )

    # Normalize key/string fields
    zmm_renamed["material_code"] = _normalize_str(zmm_renamed["material_code"])
    zmm_renamed["material_group_code"] = _normalize_str(
        zmm_renamed["material_group_code"]
    )
    zmm_renamed["material_type_code"] = _normalize_str(
        zmm_renamed["material_type_code"]
    )
    zmm_renamed["sloc"] = _normalize_str(zmm_renamed["sloc"])
    zmm_renamed["vendor_code"] = _normalize_str(zmm_renamed["vendor_code"])
    zmm_renamed["brand"] = _normalize_str(zmm_renamed["brand"])
    zmm_renamed["product_line"] = _normalize_str(zmm_renamed["product_line"])
    zmm_renamed["product_group"] = _normalize_str(zmm_renamed["product_group"])
    zmm_renamed["product_series"] = _normalize_str(zmm_renamed["product_series"])

    # Numeric fields
    zmm_renamed["standard_price"] = pd.to_numeric(
        zmm_renamed.get("standard_price"), errors="coerce"
    )
    zmm_renamed["price_control"] = _normalize_str(zmm_renamed.get("price_control"))

    # ---------------- Storage Location ----------------
    sloc_df = _read_excel(storage_location_path)
    sloc_df.columns = sloc_df.columns.astype(str).str.strip()

    required_sloc_cols = ["SLoc", "Description", "Storage Group"]
    _ensure_columns(sloc_df, required_sloc_cols, "Storage Location")

    sloc_df = sloc_df.rename(
        columns={
            "SLoc": "sloc",
            "Description": "sloc_description",
            "Storage Group": "storage_group",
        }
    )
    sloc_df["sloc"] = _normalize_str(sloc_df["sloc"])
    sloc_df["sloc_description"] = _normalize_str(sloc_df["sloc_description"])
    sloc_df["storage_group"] = _normalize_str(sloc_df["storage_group"])

    # ---------------- Material Group ----------------
    mg_df = _read_excel(material_group_path)
    mg_df.columns = mg_df.columns.astype(str).str.strip()

    required_mg_cols = ["Matl Group"]
    _ensure_columns(mg_df, required_mg_cols, "Material Group")

    mg_df = mg_df.rename(
        columns={
            "Matl Group": "material_group_code",
            "Material Group Desc.": "material_group_desc",
            "Description 2 for the material group": "material_group_desc2",
            "Display Description": "material_group_display_desc",
        }
    )
    mg_df["material_group_code"] = _normalize_str(mg_df["material_group_code"])
    mg_df["material_group_desc"] = _normalize_str(mg_df.get("material_group_desc"))
    mg_df["material_group_desc2"] = _normalize_str(mg_df.get("material_group_desc2"))
    mg_df["material_group_display_desc"] = _normalize_str(
        mg_df.get("material_group_display_desc")
    )

    # ---------------- Material Type ----------------
    mt_df = _read_excel(material_type_path)
    mt_df.columns = mt_df.columns.astype(str).str.strip()

    required_mt_cols = ["MTyp"]
    _ensure_columns(mt_df, required_mt_cols, "Material Type")

    mt_df = mt_df.rename(
        columns={
            "MTyp": "material_type_code",
            "Material type description": "material_type_desc",
            "Material type Group": "material_type_group",
        }
    )
    mt_df["material_type_code"] = _normalize_str(mt_df["material_type_code"])
    mt_df["material_type_desc"] = _normalize_str(mt_df.get("material_type_desc"))
    mt_df["material_type_group"] = _normalize_str(mt_df.get("material_type_group"))

    # ---------------- MKVZ (Vendor) ----------------
    mkvz_df = _read_excel(mkvz_path)
    mkvz_df.columns = mkvz_df.columns.astype(str).str.strip()

    required_mkvz_cols = ["Vendor"]
    _ensure_columns(mkvz_df, required_mkvz_cols, "MKVZ")

    mkvz_df = mkvz_df.rename(
        columns={
            "Vendor": "vendor_code",
            "Name of vendor": "vendor_name",
            "Street": "vendor_street",
            "Country": "vendor_country",
            "Postal Code": "vendor_postal_code",
            "City": "vendor_city",
            "Account group": "vendor_account_group",
            "Search term": "vendor_search_term",
            "Central purchasing block": "vendor_purch_block",
            "Central deletion flag": "vendor_deletion_flag",
            "One-time account": "vendor_one_time",
            "Purch. Organization": "purch_org",
            "Purch. Org. Descr.": "purch_org_desc",
            "Terms of Payment": "terms_payment",
            "Incoterms": "incoterms",
            "Incoterms (Part 2)": "incoterms_part2",
            "Order currency": "order_currency",
            "Salesperson": "salesperson",
            "Telephone": "telephone",
        }
    )

    mkvz_df["vendor_code"] = _normalize_str(mkvz_df["vendor_code"])
    mkvz_df["vendor_country"] = _normalize_str(mkvz_df.get("vendor_country"))
    mkvz_df["vendor_postal_code"] = _normalize_str(mkvz_df.get("vendor_postal_code"))
    mkvz_df["vendor_search_term"] = _normalize_str(mkvz_df.get("vendor_search_term"))

    # ---------------- Merge everything ----------------
    merged = zmm_renamed.merge(
        sloc_df[["sloc", "sloc_description", "storage_group"]],
        how="left",
        on="sloc",
    )

    merged = merged.merge(
        mg_df[
            [
                "material_group_code",
                "material_group_desc",
                "material_group_desc2",
                "material_group_display_desc",
            ]
        ],
        how="left",
        on="material_group_code",
    )

    merged = merged.merge(
        mt_df[
            ["material_type_code", "material_type_desc", "material_type_group"]
        ],
        how="left",
        on="material_type_code",
    )

    merged = merged.merge(
        mkvz_df[
            [
                "vendor_code",
                "vendor_country",
                "vendor_postal_code",
                "vendor_search_term",
            ]
        ],
        how="left",
        on="vendor_code",
    )

    # ---------------- Serialized flag logic ----------------
   # --- Serialization flags ----------------------------------------

# Normalize the serial number profile column from ZMM345E
df["serial_number_profile"] = (
    df.get("serial_number_profile", pd.NA)
      .astype("string")
      .str.strip()
)

# Treat as NOT serialized when:
# - blank ("")
# - NULL
# - equal to "Z002" (any case, with/without spaces)
upper_profile = df["serial_number_profile"].fillna("").str.strip().str.upper()

df["is_serialized"] = ~(
    (upper_profile == "") | (upper_profile == "Z002")
)
    # ---------------- Final dim table shape ----------------
    dim = merged[
        [
            "material_code",
            "description",
            "brand",
            "product_line",
            "product_group",
            "product_series",
            "material_group_code",
            "material_group_desc",
            "material_group_desc2",
            "material_group_display_desc",
            "material_type_code",
            "material_type_desc",
            "material_type_group",
            "sloc",
            "sloc_description",
            "storage_group",
            "old_part_no",
            "serial_number_profile",
            "is_serialized",
            "standard_price",
            "price_control",
            "vendor_code",
            "vendor_country",
            "vendor_postal_code",
            "vendor_search_term",
        ]
    ].copy()

    dim["upload_batch_id"] = upload_batch_id
    dim["snapshot_date"] = snapshot_date
    dim["source"] = "MATERIAL_MASTER"

    # Ensure booleans
    dim["is_serialized"] = dim["is_serialized"].astype(bool)

    # Write to DB
    dim.to_sql(
        "dim_material_master",
        engine,
        if_exists="append",
        index=False,
    )

    # ---------------- Indexes (created once) ----------------
    _ensure_material_master_indexes()


# ------------------------------------------------------
# Index + diagnostics
# ------------------------------------------------------


def _index_exists(index_name: str) -> bool:
    query = text(
        """
        SELECT COUNT(1)
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'dim_material_master'
          AND index_name = :idx
        """
    )
    with engine.connect() as conn:
        return bool(conn.execute(query, {"idx": index_name}).scalar())


def _ensure_material_master_indexes() -> None:
    indexes = {
        "idx_dim_mm_material_code": "CREATE INDEX idx_dim_mm_material_code ON dim_material_master (material_code)",
        "idx_dim_mm_is_serialized": "CREATE INDEX idx_dim_mm_is_serialized ON dim_material_master (is_serialized)",
        "idx_dim_mm_brand_group": "CREATE INDEX idx_dim_mm_brand_group ON dim_material_master (brand, material_group_code)",
    }

    with engine.begin() as conn:
        for name, stmt in indexes.items():
            if not _index_exists(name):
                conn.execute(text(stmt))


def get_material_master_stats() -> dict:
    """
    Simple diagnostics helper for FastAPI:
    - total rows
    - serialized vs not serialized
    - distinct brands, material groups, vendors
    """
    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM dim_material_master")
        ).scalar()

        serialized = conn.execute(
            text(
                "SELECT COUNT(*) FROM dim_material_master WHERE is_serialized = 1"
            )
        ).scalar()

        not_serialized = conn.execute(
            text(
                "SELECT COUNT(*) FROM dim_material_master WHERE is_serialized = 0"
            )
        ).scalar()

        distinct_brands = conn.execute(
            text("SELECT COUNT(DISTINCT brand) FROM dim_material_master")
        ).scalar()

        distinct_groups = conn.execute(
            text("SELECT COUNT(DISTINCT material_group_code) FROM dim_material_master")
        ).scalar()

        distinct_vendors = conn.execute(
            text("SELECT COUNT(DISTINCT vendor_code) FROM dim_material_master")
        ).scalar()

    return {
        "total_rows": int(total or 0),
        "serialized_rows": int(serialized or 0),
        "not_serialized_rows": int(not_serialized or 0),
        "distinct_brands": int(distinct_brands or 0),
        "distinct_material_groups": int(distinct_groups or 0),
        "distinct_vendors": int(distinct_vendors or 0),
    }
