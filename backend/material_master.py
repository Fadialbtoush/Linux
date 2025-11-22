# material_master.py
from pathlib import Path
from datetime import date

import pandas as pd

from db import engine


# ------------------------
# Load helpers for each file
# ------------------------


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str).str.strip()
    return df


def _load_zmm345e(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize the main material master data (ZMM345E extract).

    We mainly keep the fields needed for the unified material master.
    """
    df = pd.read_excel(file_path)
    _clean_columns(df)

    col_map = {
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
        "Purch. group": "purch_group",
        "Plant Status": "plant_status",
        "Auto PO": "auto_po",
        "MRP Group": "mrp_group",
        "MRP Type": "mrp_type",
        "MRP Controller": "mrp_controller",
        "Lot Size": "lot_size",
        "Procu. Type": "procurement_type",
        "Issue SLoc.": "issue_sloc",
        "SLocation for EP": "sloc_for_ep",
        "InhseProd Time": "inhouse_production_time",
        "Planned del.time": "planned_delivery_time",
        "GR proc time": "gr_processing_time",
        "Schdl. Margin key": "schedule_margin_key",
        "Safety Stock": "safety_stock",
        "Strategy Group": "strategy_group",
        "Consumption Mode": "consumption_mode",
        "Consumption period: back.": "consumption_period_back",
        "Consumption period: for.": "consumption_period_forward",
        "Individual/Coll": "individual_coll",
        "Valua. class": "valuation_class",
        "Price contl": "price_control",
        "Price unit": "price_unit",
        "Stand. Price": "standard_price",
        "Material Origin": "material_origin",
        "Overhead Group": "overhead_group",
        "Storage Bin": "storage_bin",
        "Cross-Plant Material Status": "cross_plant_status",
        "Basic view DF": "basic_view_df",
        "Plant view DF": "plant_view_df",
        "Brand": "brand",
        "ProductLine": "product_line",
        "ProductGroup": "product_group",
        "ProductSeries": "product_series",
        "Vendor": "vendor",
    }

    # Apply renaming only for existing columns
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Make sure critical columns exist
    for col in [
        "material",
        "description",
        "old_part_no",
        "model_number",
        "brand",
        "product_line",
        "product_group",
        "product_series",
        "serial_number_profile",
        "mat_type",
        "mat_group",
        "sloc",
        "vendor",
        "mrp_group",
        "price_control",
        "standard_price",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    # Normalize key columns as string (preserve leading zeros, strip spaces)
    for col in ["material", "mat_type", "mat_group", "sloc", "vendor", "mrp_group"]:
        df[col] = df[col].astype("string").str.strip()

    return df


def _load_storage_location(file_path: Path) -> pd.DataFrame:
    """
    Storage Location master:
      - SLoc (primary key)
      - Description
      - Storage Group
    """
    df = pd.read_excel(file_path)
    _clean_columns(df)

    col_map = {
        "SLoc": "sloc",
        "Description": "sloc_description",
        "Storage Group": "storage_group",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["sloc", "sloc_description", "storage_group"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["sloc"] = df["sloc"].astype("string").str.strip()
    return df[["sloc", "sloc_description", "storage_group"]].drop_duplicates()


def _load_material_group(file_path: Path) -> pd.DataFrame:
    """
    Material Group master:
      - Matl Group (primary key)
      - Material Group Desc.
      - Description 2 for the material group
      - Display Description
    """
    df = pd.read_excel(file_path)
    _clean_columns(df)

    col_map = {
        "Matl Group": "mat_group",
        "Material Group Desc.": "mat_group_desc",
        "Description 2 for the material group": "mat_group_desc2",
        "Display Description": "mat_group_display_desc",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["mat_group", "mat_group_desc", "mat_group_desc2", "mat_group_display_desc"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["mat_group"] = df["mat_group"].astype("string").str.strip()
    return df[
        ["mat_group", "mat_group_desc", "mat_group_desc2", "mat_group_display_desc"]
    ].drop_duplicates()


def _load_material_type(file_path: Path) -> pd.DataFrame:
    """
    Material Type master:
      - MTyp (primary key)
      - Material type description
      - Material type Group
    """
    df = pd.read_excel(file_path)
    _clean_columns(df)

    col_map = {
        "MTyp": "mat_type",
        "Material type description": "mat_type_desc",
        "Material type Group": "mat_type_group",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["mat_type", "mat_type_desc", "mat_type_group"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["mat_type"] = df["mat_type"].astype("string").str.strip()
    return df[["mat_type", "mat_type_desc", "mat_type_group"]].drop_duplicates()


def _load_mkvz(file_path: Path) -> pd.DataFrame:
    """
    MKVZ vendor master:
      - Vendor (primary key)
      - Country
      - Postal Code
      - Search term
    """
    df = pd.read_excel(file_path)
    _clean_columns(df)

    col_map = {
        "Vendor": "vendor",
        "Country": "vendor_country",
        "Postal Code": "vendor_postal_code",
        "Search term": "vendor_search_term",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["vendor", "vendor_country", "vendor_postal_code", "vendor_search_term"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["vendor"] = df["vendor"].astype("string").str.strip()
    return df[
        ["vendor", "vendor_country", "vendor_postal_code", "vendor_search_term"]
    ].drop_duplicates()


# ------------------------
# Build unified material master
# ------------------------


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
    Build the unified material master (dim_material_master) from
    the five input tables/files.

    - Primary key: material
    - Only includes the fields you requested (include/map/PK).
    """

    # Load sources
    zmm = _load_zmm345e(zmm345e_path)
    sloc = _load_storage_location(storage_location_path)
    mg = _load_material_group(material_group_path)
    mt = _load_material_type(material_type_path)
    mkvz = _load_mkvz(mkvz_path)

    # Start from ZMM345E as the core
    master = zmm.copy()

    # Join material type
    master = master.merge(mt, on="mat_type", how="left", suffixes=("", "_mt"))

    # Join material group
    master = master.merge(mg, on="mat_group", how="left", suffixes=("", "_mg"))

    # Join storage location
    master = master.merge(sloc, on="sloc", how="left", suffixes=("", "_sloc"))

    # Join vendor
    master = master.merge(mkvz, on="vendor", how="left", suffixes=("", "_vendor"))

    # Serial number profile rule:
    #  - blank/NaN -> "Not Serialized"
    #  - anything else -> "Serialized"
    ser_raw = master["serial_number_profile"].astype("string")
    master["is_serialized"] = "Not Serialized"
    mask_serial = ser_raw.notna() & (ser_raw.str.strip() != "")
    master.loc[mask_serial, "is_serialized"] = "Serialized"

    # Meta
    master["upload_batch_id"] = upload_batch_id
    master["snapshot_date"] = snapshot_date
    master["source"] = "MATERIAL_MASTER"

    # Final columns (only what you care about)
    final_cols = [
        # keys / core
        "material",
        "description",
        "old_part_no",
        "model_number",
        # brand hierarchy
        "brand",
        "product_line",
        "product_group",
        "product_series",
        # serialization
        "serial_number_profile",
        "is_serialized",
        # material type
        "mat_type",
        "mat_type_desc",
        "mat_type_group",
        # material group
        "mat_group",
        "mat_group_desc",
        "mat_group_desc2",
        "mat_group_display_desc",
        # storage location
        "sloc",
        "sloc_description",
        "storage_group",
        # vendor
        "vendor",
        "vendor_country",
        "vendor_postal_code",
        "vendor_search_term",
        # MRP group (kept as code only)
        "mrp_group",
        # pricing
        "price_control",
        "standard_price",
        # meta
        "upload_batch_id",
        "snapshot_date",
        "source",
    ]

    # Ensure all final columns exist
    for col in final_cols:
        if col not in master.columns:
            master[col] = pd.NA

    final_df = master[final_cols].copy()

    # Optional: enforce uniqueness by material (latest row wins)
    final_df = (
        final_df.sort_values(["material"])
        .drop_duplicates(subset=["material"], keep="last")
    )

    # Write to DB
    final_df.to_sql("dim_material_master", engine, if_exists="append", index=False)
