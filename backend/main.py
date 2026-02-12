from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
import os
import tempfile
import re
import logging
import time
import csv
from io import StringIO

# ---------------- CONFIG ----------------

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- APP ----------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- DB ----------------

def get_db_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

# ---------------- UTILS ----------------

def safe_table_name(filename: str) -> str:
    name = filename.lower().replace(".csv", "")
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return f"data_{name}"

# ---------------- HEALTH ----------------

@app.get("/")
def health():
    return {"status": "backend running"}

# ---------------- CSV UPLOAD ----------------

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV allowed")

    table = safe_table_name(file.filename)
    logger.info(f"Uploading CSV → {table}")

    conn = get_db_conn()
    cur = conn.cursor()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Create table
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table}" (
                id BIGSERIAL PRIMARY KEY,
                school_code VARCHAR(50),
                school_name TEXT,
                employee_name TEXT,
                employee_code VARCHAR(50),
                designation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Replace data
        cur.execute(f'TRUNCATE TABLE "{table}"')

        with open(tmp_path, "r", encoding="latin1") as f:
            cur.copy_expert(f'''
                COPY "{table}" (
                    school_code,
                    school_name,
                    employee_name,
                    employee_code,
                    designation
                )
                FROM STDIN
                WITH CSV HEADER
            ''', f)

        # Count rows
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        row_count = cur.fetchone()[0]

        # Register dataset (single source of truth)
        cur.execute("""
            INSERT INTO dataset_registry (table_name, original_filename, row_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET
                original_filename = EXCLUDED.original_filename,
                row_count = EXCLUDED.row_count,
                uploaded_at = CURRENT_TIMESTAMP
        """, (table, file.filename, row_count))

        conn.commit()

        return {
            "table": table,
            "rows": row_count
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()
        os.remove(tmp_path)

# ---------------- DATASET LIST ----------------

@app.get("/datasets")
def list_datasets():
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name, original_filename, row_count, uploaded_at
        FROM dataset_registry
        ORDER BY uploaded_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "table": r[0],
            "filename": r[1],
            "rows": r[2],
            "uploaded_at": r[3]
        }
        for r in rows
    ]

# ---------------- DELETE DATASET ----------------

class DeletePayload(BaseModel):
    table: str

@app.post("/datasets/delete")
def delete_dataset(payload: DeletePayload):
    table = payload.table

    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name")

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute(
            "DELETE FROM dataset_registry WHERE table_name = %s",
            (table,)
        )
        conn.commit()
        return {"status": "deleted"}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()

# ---------------- DOWNLOAD DATASET ----------------

@app.get("/download/{table}")
def download_table(table: str):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name")

    conn = get_db_conn()
    cur = conn.cursor()

    def stream():
        buffer = StringIO()

        try:
            cur.copy_expert(
                f'''
                COPY (
                    SELECT
                        school_code,
                        school_name,
                        employee_name,
                        employee_code,
                        designation
                    FROM "{table}"
                )
                TO STDOUT WITH CSV HEADER
                ''',
                buffer
            )
            buffer.seek(0)

            while True:
                chunk = buffer.read(8192)
                if not chunk:
                    break
                yield chunk

        finally:
            buffer.close()
            cur.close()
            conn.close()

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{table}.csv"',
            "Cache-Control": "no-store"
        }
    )
