from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pandas as pd
import os

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health():
    return {"status": "backend running"}

@app.get("/data/{state}")
def get_state_data(state: str):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT district, value1, value2, remarks
        FROM state_data
        WHERE state = %s
        ORDER BY district
        """,
        (state.upper(),)
    )
    return cur.fetchall()

@app.post("/upload/{state}")
def upload_state_data(state: str, file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV allowed")

    df = pd.read_csv(file.file, encoding="latin1")

    required = {"district", "value1", "value2", "remarks"}
    if not required.issubset(df.columns):
        raise HTTPException(
            status_code=400,
            detail="CSV must have district, value1, value2, remarks"
        )

    cur = conn.cursor()
    cur.execute(
        "DELETE FROM state_data WHERE state = %s",
        (state.upper(),)
    )

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO state_data (state, district, value1, value2, remarks)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                state.upper(),
                row["district"],
                int(row["value1"]),
                int(row["value2"]),
                row["remarks"]
            )
        )

    conn.commit()
    return {"message": f"{state.upper()} data replaced"}
