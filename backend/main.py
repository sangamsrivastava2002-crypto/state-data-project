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

# ================= SCHEMA =================

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
    name = re.sub(r"[^a-z0-9_]+", "_", filename.lower().replace(".csv", ""))
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{schema}_{name}_{ts}"

def decode_csv_bytes(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8-sig")
        print("📄 CSV decoded as UTF-8")
        return text
    except UnicodeDecodeError:
        text = raw.decode("latin1")
        print("📄 CSV decoded as Latin-1")
        return text

# ================= HEALTH =================

@app.get("/")
def health():
    return {"status": "backend running"}

# ================= UPLOAD =================

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV allowed")

    raw_bytes = await file.read()
    csv_text = decode_csv_bytes(raw_bytes)

    csv_buffer = StringIO(csv_text)
    reader = csv.reader(csv_buffer)

    try:
        raw_headers = next(reader)
    except StopIteration:
        raise HTTPException(status_code=400, detail="Empty CSV")

    headers = [normalize(h) for h in raw_headers]
    expected_cols = len(headers)

    # ---------- ROW VALIDATION (METHOD 1) ----------
    for i, row in enumerate(reader, start=2):
        if len(row) != expected_cols:
            print("❌ CSV COLUMN MISMATCH")
            print("Row:", i)
            print("Expected:", expected_cols)
            print("Actual:", len(row))
            print("Row data:", row)
            raise HTTPException(
                status_code=400,
                detail=f"Malformed CSV at row {i}"
            )
    # ----------------------------------------------

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

        # ---------- CLEAN CSV REWRITE ----------
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        reader.fieldnames = [normalize(h) for h in reader.fieldnames]

        output = StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=copy_cols,
            extrasaction="ignore"
        )
        writer.writeheader()

        for row in reader:
            clean = {c: (row.get(c) or "").strip() for c in copy_cols}
            writer.writerow(clean)

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

# ================= DATASETS =================

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

# ================= DELETE =================

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
