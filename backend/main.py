from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
import tempfile
import logging
import time

# ------------------ CONFIG ------------------

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ APP ------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ DB HELPER ------------------

def get_db_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

# ------------------ HEALTH ------------------

@app.get("/")
def health():
    return {"status": "backend running"}

# ------------------ STATES ------------------

@app.get("/states")
def get_states():
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT state
            FROM state_data
            ORDER BY state
            """
        )

        rows = cur.fetchall()
        return [row[0] for row in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()

# ------------------ SEARCH ------------------

@app.get("/search")
def search_by_state_and_school(state: str, school_code: str):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                school_code,
                school_name,
                employee_name,
                employee_code,
                designation
            FROM state_data
            WHERE state = %s
              AND school_code = %s
            ORDER BY employee_name
            """,
            (state.upper(), school_code)
        )

        return cur.fetchall()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()

# ------------------ CSV UPLOAD (FINAL FIX) ------------------

@app.post("/upload-csv")
async def upload_csv(files: list[UploadFile] = File(...)):
    conn = None

    try:
        conn = get_db_conn()
        cur = conn.cursor()

        for file in files:
            if not file.filename.lower().endswith(".csv"):
                continue

            state = file.filename.replace(".csv", "").upper()
            logger.info(f"Starting upload for state: {state}")
            start_time = time.time()

            # Save file to temp
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                contents = await file.read()
                tmp.write(contents)
                tmp_path = tmp.name

            try:
                # Replace data atomically per state
                cur.execute(
                    "DELETE FROM state_data WHERE state = %s",
                    (state,)
                )

                with open(tmp_path, "r", encoding="latin1") as f:
                    cur.copy_expert(
                        """
                        COPY state_data (
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

                # Inject state value
                cur.execute(
                    "UPDATE state_data SET state = %s WHERE state IS NULL",
                    (state,)
                )

                conn.commit()

                logger.info(
                    f"{state} upload completed in "
                    f"{time.time() - start_time:.2f}s"
                )

            except Exception as e:
                conn.rollback()
                logger.exception(f"Upload failed for {state}")
                raise HTTPException(status_code=500, detail=str(e))

            finally:
                os.remove(tmp_path)

        return {"message": "All CSV files uploaded successfully"}

    finally:
        if conn:
            conn.close()
