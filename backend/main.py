#Fadi
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from db import engine, ensure_core_tables, parse_snapshot_date
from mb52 import process_mb52
from zmmr014 import process_zmmr014
from zmmr015_power import process_zmmr015_power
from odoo_aging import process_odoo_aging
from zsdr030a import process_zsdr030a
from zsdr004 import process_zsdr004
from ZMM345E import _process_zmm345e
from material_master import build_material_master  # <-- important


app = FastAPI(title="SAP Reporting Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "message": "SAP reporting backend running"}


@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ------------------------------------------------------
# Helper: Save uploaded file
# ------------------------------------------------------


def _save_upload(file: UploadFile):
    batch_id = str(uuid.uuid4())
    upload_dir = Path("/tmp/uploads")
    upload_dir.mkdir(parents=True, exist_ok=True)

    upload_path = upload_dir / f"{batch_id}_{file.filename}"

    content = file.file.read()
    with open(upload_path, "wb") as f:
        f.write(content)

    return batch_id, upload_path


# ------------------------------------------------------
# MB52
# ------------------------------------------------------


@app.post("/upload_MB52")
async def upload_mb52(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_mb52(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "MB52",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# ZMMR014
# ------------------------------------------------------


@app.post("/upload_ZMMR014")
async def upload_zmmr014(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_zmmr014(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ZMMR014",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# ZMMR015 Power
# ------------------------------------------------------


@app.post("/upload_ZMMR015_Power")
async def upload_zmmr015_power(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_zmmr015_power(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ZMMR015_POWER",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# Odoo Aging
# ------------------------------------------------------


@app.post("/upload_Odoo_Aging")
async def upload_odoo_aging(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_odoo_aging(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ODOO_AGING",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# ZSDR030A
# ------------------------------------------------------


@app.post("/upload_ZSDR030A")
async def upload_zsdr030a(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_zsdr030a(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ZSDR030A",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# ZSDR004
# ------------------------------------------------------


@app.post("/upload_ZSDR004")
async def upload_zsdr004(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    process_zsdr004(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ZSDR004",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# ZMM345E
# ------------------------------------------------------


@app.post("/upload_ZMM345E")
async def upload_zmm345e(
    file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    batch_id, upload_path = _save_upload(file)
    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    _process_zmm345e(upload_path, batch_id, snapshot_date_obj)

    return {
        "status": "ok",
        "source": "ZMM345E",
        "batch_id": batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }


# ------------------------------------------------------
# MATERIAL MASTER (ZMM345E + StorageLoc + MatGroup + MatType + MKVZ)
# ------------------------------------------------------


@app.post("/upload_material_master")
async def upload_material_master(
    zmm345e_file: UploadFile = File(...),
    storage_location_file: UploadFile = File(...),
    material_group_file: UploadFile = File(...),
    material_type_file: UploadFile = File(...),
    mkvz_file: UploadFile = File(...),
    snapshot_date: Optional[str] = Form(None),
):
    ensure_core_tables()

    # One unified batch ID for the combined master table
    unified_batch_id = str(uuid.uuid4())

    # Save all files to disk
    _, zmm_path = _save_upload(zmm345e_file)
    _, sloc_path = _save_upload(storage_location_file)
    _, mg_path = _save_upload(material_group_file)
    _, mt_path = _save_upload(material_type_file)
    _, mkvz_path = _save_upload(mkvz_file)

    snapshot_date_obj = parse_snapshot_date(snapshot_date)

    # Build material master table
    build_material_master(
        zmm345e_path=zmm_path,
        storage_location_path=sloc_path,
        material_group_path=mg_path,
        material_type_path=mt_path,
        mkvz_path=mkvz_path,
        upload_batch_id=unified_batch_id,
        snapshot_date=snapshot_date_obj,
    )

    return {
        "status": "ok",
        "source": "MATERIAL_MASTER",
        "batch_id": unified_batch_id,
        "snapshot_date": snapshot_date_obj.isoformat(),
    }
