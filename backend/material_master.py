#Fadi Hamid Khalil AL Btoush
# material_master.py
from pathlib import Path
from datetime import date

import pandas as pd

from db import engine


def _load_zmm345e(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize the main material master data (ZMM345E extract).

    Expected key columns (SAP headers):
      - Material
      - Indst. Sector
      - Matr type
      - Plant
      - SLocation
      - Sales Org.
      - Dist. Channel
      - Description
      - Base UOM
      - Matr Group
      - Old part No.
      - Division
      - Item cate.(BASIC)
      - Model number
      - Serial Number Profile
      - MRP Group
      - Price contl
      - Stand. Price
      - Brand
      - ProductLine
      - ProductGroup
      - ProductSeries
      - Vendor
    """

    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    # Rename to internal normalized names
    df = df.rename(
        columns={
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
            "Model number": "model_number",
            "Serial Number Profile": "serial_number_profile",
            "MRP Group": "mrp_group",
            "Price contl": "price_control",
            "Stand. Price": "standard_price",
            "Brand": "brand",
            "ProductLine": "product_line",
            "ProductGroup": "product_group",
            "ProductSeries": "product_series",
            "Vendor": "vendor",
        }
    )

    # Strip text fields
    text_cols = [
        "material",
        "industry_sector",
        "mat_type",
        "plant",
        "sloc",
        "sales_org",
        "dist_channel",
        "description",
        "base_uom",
        "mat_group",
        "old_part_no",
        "division",
        "item_category_basic",
        "model_number",
        "serial_number_profile",
        "mrp_group",
        "price_control",
        "brand",
        "product_line",
        "product_group",
        "product_series",
        "vendor",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Numeric fields
    df["standard_price"] = pd.to_numeric(
        df.get("standard_price"), errors="coerce"
    )

    # Serial number indication: blank = not serialized
    df["is_serialized"] = df["serial_number_profile"].apply(
        lambda x: "Yes" if isinstance(x, str) and x.strip() != "" else "No"
    )

    # Keep only the relevant subset for the base material table
    base_cols = [
        "material",
        "industry_sector",
        "mat_type",
        "plant",
        "sloc",
        "sales_org",
        "dist_channel",
        "description",
        "base_uom",
        "mat_group",
        "old_part_no",
        "division",
        "item_category_basic",
        "model_number",
        "mrp_group",
        "price_control",
        "standard_price",
        "brand",
        "product_line",
        "product_group",
        "product_series",
        "vendor",
        "is_serialized",
    ]
    base = df[base_cols].copy()

    # One row per material (keep first occurrence)
    base = (
        base.dropna(subset=["material"])
        .drop_duplicates(subset=["material"])
        .reset_index(drop=True)
    )

    return base


def _load_storage_location(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize Storage Location table.

    Expected columns:
      - SLoc
      - Description
      - Storage Group
    """
    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    df = df.rename(
        columns={
            "SLoc": "sloc",
            "Description": "sloc_description",
            "Storage Group": "storage_group",
        }
    )

    for col in ["sloc", "sloc_description", "storage_group"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # One row per SLoc
    df = (
        df.dropna(subset=["sloc"])
        .drop_duplicates(subset=["sloc"])
        .reset_index(drop=True)
    )

    return df[["sloc", "sloc_description", "storage_group"]]


def _load_material_group(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize Material Group table.

    Expected columns:
      - Matl Group
      - Material Group Desc.
      - Description 2 for the material group
      - Display Description
    """
    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    df = df.rename(
        columns={
            "Matl Group": "mat_group",
            "Material Group Desc.": "mat_group_desc",
            "Description 2 for the material group": "mat_group_desc2",
            "Display Description": "mat_group_display_desc",
        }
    )

    for col in [
        "mat_group",
        "mat_group_desc",
        "mat_group_desc2",
        "mat_group_display_desc",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    df = (
        df.dropna(subset=["mat_group"])
        .drop_duplicates(subset=["mat_group"])
        .reset_index(drop=True)
    )

    return df[
        [
            "mat_group",
            "mat_group_desc",
            "mat_group_desc2",
            "mat_group_display_desc",
        ]
    ]


def _load_material_type(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize Material Type table.

    Expected columns:
      - MTyp
      - Material type description
      - Material type Group
    """
    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    df = df.rename(
        columns={
            "MTyp": "mat_type",
            "Material type description": "mat_type_desc",
            "Material type Group": "mat_type_group",
        }
    )

    for col in ["mat_type", "mat_type_desc", "mat_type_group"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    df = (
        df.dropna(subset=["mat_type"])
        .drop_duplicates(subset=["mat_type"])
        .reset_index(drop=True)
    )

    return df[["mat_type", "mat_type_desc", "mat_type_group"]]


def _load_mkvz(file_path: Path) -> pd.DataFrame:
    """
    Load and normalize MKVZ vendor table.

    From your mapping we keep:
      - Vendor (PK)
      - Country
      - Postal Code
      - Search term
    """
    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    df = df.rename(
        columns={
            "Vendor": "vendor",
            "Country": "vendor_country",
            "Postal Code": "vendor_postal_code",
            "Search term": "vendor_search_term",
        }
    )

    for col in [
        "vendor",
        "vendor_country",
        "vendor_postal_code",
        "vendor_search_term",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    df = (
        df.dropna(subset=["vendor"])
        .drop_duplicates(subset=["vendor"])
        .reset_index(drop=True)
    )

    return df[
        ["vendor", "vendor_country", "vendor_postal_code", "vendor_search_term"]
    ]


def process_material_master(
    zmm345e_path: Path,
    storage_location_path: Path,
    material_group_path: Path,
    material_type_path: Path,
    mkvz_path: Path,
    upload_batch_id: str,
    snapshot_date: date,
) -> None:
    """
    Build the consolidated material dimension table (dim_material)
    from the 5 monthly extracts.

    This table will later be used to map with:
      - MB52 (stock)
      - Sales (ZSDR030A / others)
      - Purchasing
      - Aging
    """

    # Load/normalize source tables
    base = _load_zmm345e(zmm345e_path)
    sloc_dim = _load_storage_location(storage_location_path)
    mat_group_dim = _load_material_group(material_group_path)
    mat_type_dim = _load_material_type(material_type_path)
    vendor_dim = _load_mkvz(mkvz_path)

    # Start with base materials
    dim = base.copy()

    # Join storage location info (many materials may share same SLoc)
    dim = dim.merge(sloc_dim, on="sloc", how="left")

    # Join material group info
    dim = dim.merge(mat_group_dim, on="mat_group", how="left")

    # Join material type info
    dim = dim.merge(mat_type_dim, on="mat_type", how="left")

    # Join vendor info
    dim = dim.merge(vendor_dim, on="vendor", how="left")

    # Meta columns
    dim["upload_batch_id"] = upload_batch_id
    dim["snapshot_date"] = snapshot_date
    dim["source"] = "MATERIAL_MASTER"

    # Final column ordering (you can adjust as you like)
    final_cols = [
        # keys
        "material",
        # high-level attributes
        "description",
        "brand",
        "product_line",
        "product_group",
        "product_series",
        "model_number",
        # type & group
        "mat_type",
        "mat_type_desc",
        "mat_type_group",
        "mat_group",
        "mat_group_desc",
        "mat_group_desc2",
        "mat_group_display_desc",
        # stock-related
        "plant",
        "sloc",
        "sloc_description",
        "storage_group",
        "base_uom",
        # MRP group (mapped via material group in your concept)
        "mrp_group",
        # pricing
        "price_control",
        "standard_price",
        # serial
        "is_serialized",
        # vendor
        "vendor",
        "vendor_country",
        "vendor_postal_code",
        "vendor_search_term",
        # other identifiers / flags
        "industry_sector",
        "division",
        "item_category_basic",
        "old_part_no",
        "sales_org",
        "dist_channel",
        # metadata
        "upload_batch_id",
        "snapshot_date",
        "source",
    ]

    # Keep only columns that actually exist (to avoid KeyError if some are missing)
    final_cols = [c for c in final_cols if c in dim.columns]
    dim = dim[final_cols].copy()

    # Write to DB. You can change to "replace" if you prefer always overwriting.
    dim.to_sql("dim_material", engine, if_exists="append", index=False)
