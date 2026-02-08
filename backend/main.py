from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pandas as pd
import os

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

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

# ------------------ SEARCH API ------------------


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


@app.get("/search")
def search_by_state_and_school(
    state: str,
    school_code: str
):
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

        rows = cur.fetchall()
        return rows

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()


# ------------------ BULK CSV UPLOAD ------------------

@app.post("/upload-csv")
def upload_csv(files: list[UploadFile] = File(...)):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        for file in files:
            if not file.filename.lower().endswith(".csv"):
                continue

            state = file.filename.replace(".csv", "").upper()

            df = pd.read_csv(file.file, encoding="latin1")

            # Normalize headers
            df.columns = [
                c.strip().lower().replace(" ", "_")
                for c in df.columns
            ]

            required = {
                "school_code",
                "school_name",
                "employee_name",
                "employee_code",
                "designation"
            }

            if not required.issubset(df.columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"{file.filename} has invalid columns: {df.columns.tolist()}"
                )

            # Replace state data
            cur.execute(
                "DELETE FROM state_data WHERE state = %s",
                (state,)
            )

            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO state_data (
                        state,
                        school_code,
                        school_name,
                        employee_name,
                        employee_code,
                        designation
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        state,
                        str(row["school_code"]),
                        str(row["school_name"]),
                        str(row["employee_name"]),
                        str(row["employee_code"]),
                        str(row["designation"])
                    )
                )

        conn.commit()
        return {"message": "All CSV files uploaded successfully"}

    except HTTPException:
        if conn:
            conn.rollback()
        raise

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()
