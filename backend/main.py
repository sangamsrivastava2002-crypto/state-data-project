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

def safe_table_name(name: str) -> str:
    """
    Convert filename or table hint to safe SQL table name
    """
    name = name.lower().replace(".csv", "")
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return f"data_{name}"

# ---------------- HEALTH ----------------

@app.get("/")
def health():
    return {"status": "backend running"}

# ---------------- LIST DATA TABLES (SEARCH UI) ----------------

@app.get("/tables")
def list_tables():
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE 'data_%'
            ORDER BY tablename
        """)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

# ---------------- SEARCH ----------------

@app.get("/search")
def search(table: str, school_code: str):
    table = safe_table_name(table)

    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                school_code,
                school_name,
                employee_name,
                employee_code,
                designation
            FROM {table}
            WHERE school_code = %s
            ORDER BY employee_name
        """, (school_code,))
        return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ---------------- CSV UPLOAD (ADD / REPLACE) ----------------

@app.post("/upload-csv")
async def upload_csv(files: list[UploadFile] = File(...)):
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for file in files:
            if not file.filename.lower().endswith(".csv"):
                continue

            table = safe_table_name(file.filename)
            logger.info(f"Uploading CSV → {table}")
            start = time.time()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(await file.read())
                tmp_path = tmp.name

            try:
                # Create table if not exists
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id BIGSERIAL PRIMARY KEY,
                        school_code VARCHAR(50),
                        school_name TEXT,
                        employee_name TEXT,
                        employee_code VARCHAR(50),
                        designation TEXT DEFAULT '',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Replace old data
                cur.execute(f"TRUNCATE TABLE {table}")

                # COPY CSV
                with open(tmp_path, "r", encoding="latin1") as f:
                    cur.copy_expert(f"""
                        COPY {table} (
                            school_code,
                            school_name,
                            employee_name,
                            employee_code,
                            designation
                        )
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            HEADER,
                            DELIMITER ',',
                            QUOTE '"',
                            ESCAPE '"'
                        )
                    """, f)

                # Index
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table}_school
                    ON {table} (school_code);
                """)

                # Row count
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()[0]

                # Register dataset (ADD or REPLACE)
                cur.execute("""
                    INSERT INTO dataset_registry (
                        table_name,
                        original_filename,
                        row_count
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (table_name)
                    DO UPDATE SET
                        original_filename = EXCLUDED.original_filename,
                        row_count = EXCLUDED.row_count,
                        uploaded_at = CURRENT_TIMESTAMP
                """, (table, file.filename, row_count))

                conn.commit()
                logger.info(
                    f"{table} uploaded successfully "
                    f"({row_count} rows, {time.time() - start:.2f}s)"
                )

            except Exception as e:
                conn.rollback()
                logger.exception(f"Upload failed for {table}")
                raise HTTPException(status_code=500, detail=str(e))

            finally:
                os.remove(tmp_path)

        return {"message": "All CSV files uploaded successfully"}

    finally:
        conn.close()

# ---------------- DATASET LIST (BACK OFFICE) ----------------

@app.get("/datasets")
def list_datasets():
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                table_name,
                original_filename,
                row_count,
                uploaded_at
            FROM dataset_registry
            ORDER BY uploaded_at DESC
        """)
        rows = cur.fetchall()

        return [
            {
                "table": r[0],
                "filename": r[1],
                "rows": r[2],
                "uploaded_at": r[3]
            }
            for r in rows
        ]
    finally:
        conn.close()

# ---------------- DELETE DATASET ----------------

@app.delete("/datasets/{table}")
def delete_dataset(table: str):
    table = safe_table_name(table)

    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(
            "DELETE FROM dataset_registry WHERE table_name = %s",
            (table,)
        )
        conn.commit()
        return {"message": "Dataset deleted successfully"}
    finally:
        conn.close()

# ---------------- DOWNLOAD FULL DATASET ----------------

@app.get("/download/{table}")
def download_table(table: str):
    table = safe_table_name(table)

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute(f"""
        COPY (
            SELECT
                school_code,
                school_name,
                employee_name,
                employee_code,
                designation
            FROM {table}
        )
        TO STDOUT WITH CSV HEADER
    """)

    def stream():
        while True:
            data = cur.fetchone()
            if data is None:
                break
            yield data

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={table}.csv"
        }
    )



class DeletePayload(BaseModel):
    table: str

@app.post("/datasets/delete")
def delete_dataset(payload: DeletePayload):
    table = payload.table

    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        cur.execute("DELETE FROM datasets_meta WHERE table_name = %s", (table,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {"status": "deleted"}


@app.get("/download/{table}")
def download_table(table: str):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        cur.execute(f'SELECT * FROM "{table}"')
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
    except Exception:
        raise HTTPException(status_code=404, detail="Table not found")
    finally:
        cur.close()
        conn.close()

    def csv_generator():
        yield ",".join(headers) + "\n"
        for row in rows:
            yield ",".join(map(str, row)) + "\n"

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{table}.csv"'
        }
    )
