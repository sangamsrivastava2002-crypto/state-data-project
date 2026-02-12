from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
import os
import tempfile
import csv
import re
from io import StringIO
from datetime import datetime

# ================= CONFIG =================

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

# ================= DB =================

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

# ================= CSV SAFE OPEN =================

def open_csv_safely(path: str):
    try:
        print("📄 CSV opened as utf-8")
        return open(path, "r", encoding="utf-8-sig", newline="")
    except UnicodeDecodeError:
        print("📄 CSV opened as latin1")
        return open(path, "r", encoding="latin1", newline="")

# ================= SCHEMA DEFINITIONS =================

TEACHER_COLUMNS = [
    "school_code",
    "school_name",
    "employee_name",
    "employee_code",
    "designation",
]

SCHOOL_COLUMNS = [
    "school_code",
    "school_name",
    "block_name",
    "district_name",
    "lowest_class",
    "highest_class",
]

# ================= UTILS =================

def normalize(col: str) -> str:
    col = col.strip().lstrip("\ufeff")
    return re.sub(r"[^a-z0-9_]+", "_", col.lower())

def detect_schema(headers: list[str]) -> str:
    header_set = set(headers)

    if set(TEACHER_COLUMNS).issubset(header_set):
        return "teacher"

    if set(SCHOOL_COLUMNS).issubset(header_set):
        return "school"

    raise HTTPException(status_code=400, detail="Unrecognized CSV schema")

def build_table_name(schema: str, filename: str) -> str:
    name = filename.lower().replace(".csv", "")
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{schema}_{name}_{ts}"

# ================= HEALTH =================

@app.get("/")
def health():
    return {"status": "backend running"}

# ================= UPLOAD CSV =================

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV allowed")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    # ===== METHOD 1: FIND BAD ROWS =====
    with open_csv_safely(tmp_path) as f:
        reader = csv.reader(f)
        headers = next(reader)
        expected_cols = len(headers)

        for i, row in enumerate(reader, start=2):
            if len(row) != expected_cols:
                print("❌ CSV COLUMN MISMATCH")
                print("Row:", i)
                print("Expected:", expected_cols)
                print("Actual:", len(row))
                print("Data:", row)
                raise HTTPException(
                    status_code=400,
                    detail=f"Malformed CSV at row {i}"
                )

    # ===== READ HEADERS FOR SCHEMA =====
    with open_csv_safely(tmp_path) as f:
        reader = csv.reader(f)
        headers = [normalize(h) for h in next(reader)]

    schema = detect_schema(headers)
    table = build_table_name(schema, file.filename)

    conn = get_db()
    cur = conn.cursor()

    try:
        if schema == "teacher":
            cur.execute(f'''
                CREATE TABLE "{table}" (
                    id BIGSERIAL PRIMARY KEY,
                    school_code TEXT,
                    school_name TEXT,
                    employee_name TEXT,
                    employee_code TEXT,
                    designation TEXT
                )
            ''')
            copy_cols = TEACHER_COLUMNS
        else:
            cur.execute(f'''
                CREATE TABLE "{table}" (
                    id BIGSERIAL PRIMARY KEY,
                    school_code TEXT,
                    school_name TEXT,
                    block_name TEXT,
                    district_name TEXT,
                    lowest_class TEXT,
                    highest_class TEXT
                )
            ''')
            copy_cols = SCHOOL_COLUMNS

        # ===== CLEAN CSV REWRITE =====
        output = StringIO()

        with open_csv_safely(tmp_path) as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [normalize(h) for h in reader.fieldnames]

            writer = csv.DictWriter(
                output,
                fieldnames=copy_cols,
                extrasaction="ignore",
                quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()

            for row in reader:
                clean_row = {c: (row.get(c) or "").strip() for c in copy_cols}
                writer.writerow(clean_row)

        output.seek(0)

        cur.copy_expert(
            f'''
            COPY "{table}" ({",".join(copy_cols)})
            FROM STDIN WITH CSV HEADER
            ''',
            output
        )

        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        rows = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO dataset_registry (table_name, dataset_type, row_count)
            VALUES (%s, %s, %s)
        """, (table, schema, rows))

        conn.commit()
        return {"table": table, "type": schema, "rows": rows}

    except Exception as e:
        conn.rollback()
        print("🔥 UPLOAD ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()
        os.remove(tmp_path)

# ================= LIST DATASETS =================

@app.get("/datasets")
def list_datasets():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name, dataset_type, row_count, uploaded_at
        FROM dataset_registry
        ORDER BY uploaded_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "table": r[0],
            "type": r[1],
            "rows": r[2],
            "uploaded_at": r[3],
        }
        for r in rows
    ]

# ================= DELETE DATASET =================

class DeletePayload(BaseModel):
    table: str

@app.post("/datasets/delete")
def delete_dataset(payload: DeletePayload):
    if not payload.table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table")

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute(f'DROP TABLE IF EXISTS "{payload.table}"')
        cur.execute("DELETE FROM dataset_registry WHERE table_name = %s", (payload.table,))
        conn.commit()
        return {"status": "deleted"}
    finally:
        cur.close()
        conn.close()

# ================= DOWNLOAD =================

@app.get("/download/{table}")
def download(table: str):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table")

    conn = get_db()
    cur = conn.cursor()

    def stream():
        buf = StringIO()
        try:
            cur.copy_expert(f'COPY "{table}" TO STDOUT WITH CSV HEADER', buf)
            buf.seek(0)
            while True:
                chunk = buf.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            buf.close()
            cur.close()
            conn.close()

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table}.csv"'}
    )

# ================= SEARCH =================

@app.get("/search")
def search(table: str, field: str, value: str):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table")
    if field not in {"school_code", "employee_code"}:
        raise HTTPException(status_code=400, detail="Invalid field")

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute(
            f'SELECT * FROM "{table}" WHERE "{field}"=%s',
            (value.strip(),)
        )
        rows = cur.fetchall()
        if not rows:
            return {"message": "Code not found", "rows": []}

        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows]}
    finally:
        cur.close()
        conn.close()
