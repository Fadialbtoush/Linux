from pathlib import Path
from datetime import date, datetime

import pandas as pd

from db import engine


def process_odoo_aging(
    file_path: Path, upload_batch_id: str, snapshot_date: date
) -> None:
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Standardize column names
    df = df.rename(
        columns={
            "Product/Internal Reference": "product_code",
            "Product": "product_name",
            "Last incoming": "last_incoming_raw",
            "Last outgoing": "last_outgoing_raw",
        }
    )

    # Parse date columns (dayfirst=True for DD/MM/YYYY style)
    df["last_incoming"] = pd.to_datetime(
        df["last_incoming_raw"], errors="coerce", dayfirst=True
    ).dt.date
    df["last_outgoing"] = pd.to_datetime(
        df["last_outgoing_raw"], errors="coerce", dayfirst=True
    ).dt.date

    # Add metadata
    df["upload_batch_id"] = upload_batch_id
    df["snapshot_date"] = snapshot_date
    df["source"] = "ODOO_AGING"

    # Raw table projection
    raw_df = df[
        [
            "upload_batch_id",
            "product_code",
            "product_name",
            "last_incoming",
            "last_outgoing",
            "snapshot_date",
            "source",
        ]
    ].copy()

    # Write raw data
    raw_df.to_sql("raw_odoo_aging", engine, if_exists="append", index=False)

    # Build fact table
    fact_df = raw_df.copy()

    def _days_since(d):
        # Handle NaT / None / NaN gracefully
        if pd.isna(d):
            return None

        # If passed as datetime/Timestamp, convert to date
        if isinstance(d, datetime):
            d = d.date()

        if isinstance(d, date):
            return (snapshot_date - d).days

        return None

    fact_df["days_since_last_incoming"] = fact_df["last_incoming"].apply(_days_since)
    fact_df["days_since_last_outgoing"] = fact_df["last_outgoing"].apply(_days_since)

    # Write fact data
    fact_df.to_sql("fact_odoo_aging", engine, if_exists="append", index=False)