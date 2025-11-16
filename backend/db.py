# db.py
import os
from datetime import datetime, date

from sqlalchemy import create_engine, text

# -------------------------------------------------------------------
# Database configuration
# -------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://sapuser:sap_password@db:3306/sap_reporting",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def ensure_core_tables() -> None:
    """
    Create core / shared tables if they do not exist.
    For now we enforce the MB52-like raw + fact tables.
    """
    create_raw_mb52 = """
    CREATE TABLE IF NOT EXISTS raw_mb52 (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        upload_batch_id VARCHAR(36) NOT NULL,
        bukrs VARCHAR(4) NULL,
        werks VARCHAR(4) NULL,
        lgort VARCHAR(10) NULL,
        matnr VARCHAR(40) NULL,
        mat_desc VARCHAR(255) NULL,
        charg VARCHAR(20) NULL,
        labst DECIMAL(18,3) NULL,
        value_unrestricted DECIMAL(18,2) NULL,
        meins VARCHAR(10) NULL,
        snapshot_date DATE NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_fact_inventory_snapshot = """
    CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        upload_batch_id VARCHAR(36) NOT NULL,
        bukrs VARCHAR(4) NULL,
        werks VARCHAR(4) NULL,
        lgort VARCHAR(10) NULL,
        matnr VARCHAR(40) NULL,
        mat_desc VARCHAR(255) NULL,
        charg VARCHAR(20) NULL,
        qty DECIMAL(18,3) NULL,
        value_unrestricted DECIMAL(18,2) NULL,
        meins VARCHAR(10) NULL,
        snapshot_date DATE NOT NULL,
        source VARCHAR(20) NOT NULL,
        total_value DECIMAL(18,2) NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(create_raw_mb52))
        conn.execute(text(create_fact_inventory_snapshot))


def parse_snapshot_date(snapshot_date_str: str | None) -> date:
    """
    Parse snapshot_date from string (expected format: YYYY-MM-DD).
    If empty or None, use today's date.
    """
    if not snapshot_date_str or str(snapshot_date_str).strip() == "":
        return date.today()
    return datetime.strptime(snapshot_date_str.strip(), "%Y-%m-%d").date()
