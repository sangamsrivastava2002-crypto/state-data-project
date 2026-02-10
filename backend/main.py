from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

def safe_table_name(filename: str) -> str:
    """
    Convert filename to safe table name.
    example: 'Assam Final-2024.csv' -> 'data_assam_final_2024'
    """
    name = filename.lower().replace(".csv", "")
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return f"data_{name}"

# ---------------- HEALTH ----------------

@app.get("/")
def health():
    return {"status": "backend running"}

# ---------------- LIST TABLES ----------------

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
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

# ---------------- SEARCH ----------------

@app.get("/search")
def search(table: str, school_code: str):
    table = safe_table_name(table)

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        query = f"""
            SELECT
                school_code,
                school_name,
                employee_name,
                employee_code,
                designation
            FROM {table}
            WHERE school_code = %s
            ORDER BY employee_name
        """

        cur.execute(query, (school_code,))
        return cur.fetchall()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()

# ---------------- CSV UPLOAD ----------------

@app.post("/upload-csv")
async def upload_csv(files: list[UploadFile] = File(...)):
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for file in files:
            if not file.filename.lower().endswith(".csv"):
                continue

            table = safe_table_name(file.filename)
            logger.info(f"Uploading → {table}")
            start = time.time()

            # Save CSV temporarily
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
                        designation TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Clear old data (replace upload)
                cur.execute(f"TRUNCATE TABLE {table}")

                # COPY data
                with open(tmp_path, "r", encoding="latin1") as f:
                    cur.copy_expert(
                        f"""
                        COPY {table} (
                            school_code,
                            school_name,
                            employee_name,
                            employee_code,
                            designation
                        )
                        FROM STDIN
                        WITH CSV HEADER
                        """,
                        f
                    )

                # Index for fast search
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table}_school
                    ON {table} (school_code);
                """)

                conn.commit()
                logger.info(
                    f"{table} uploaded in {time.time() - start:.2f}s"
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
