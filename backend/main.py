from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pandas as pd
import os

# ------------------ DB CONNECTION ------------------

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)

# ------------------ APP SETUP ------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ HEALTH CHECK ------------------

@app.get("/")
def health():
    return {"status": "backend running"}

# ------------------ SEARCH API (FIELD USERS) ------------------

@app.get("/data/{state}")
def get_state_data(state: str):
    try:
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
            ORDER BY school_name, employee_name
            """,
            (state.upper(),)
        )
        rows = cur.fetchall()
        return rows

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------ BULK CSV UPLOAD (OFFICE USERS) ------------------

@app.post("/upload-csv")
def upload_csv(files: list[UploadFile] = File(...)):
    try:
        cur = conn.cursor()

        for file in files:
            if not file.filename.lower().endswith(".csv"):
                continue

            state = file.filename.replace(".csv", "").upper()

            # Read CSV safely (Excel / Windows friendly)
            df = pd.read_csv(file.file, encoding="latin1")

            # Normalize column names
            df.columns = [
                c.strip().lower().replace(" ", "_")
                for c in df.columns
            ]

            required_columns = {
                "school_code",
                "school_name",
                "employee_name",
                "employee_code",
                "designation"
            }

            if not required_columns.issubset(df.columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"{file.filename} has invalid columns: {df.columns.tolist()}"
                )

            # Delete old data for this state
            cur.execute(
                "DELETE FROM state_data WHERE state = %s",
                (state,)
            )

            # Insert new data
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
        conn.rollback()
        raise

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
