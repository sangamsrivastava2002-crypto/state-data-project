from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from charset_normalizer import from_bytes

import psycopg2
import os
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
    col = col.replace("\ufeff", "").strip()
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
    for enc in ("utf-16", "utf-8-sig"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    result = from_bytes(raw).best()
    if result:
        return str(result)
    raise HTTPException(status_code=400, detail="Unsupported CSV encoding")

# --- SAFE FIELD HANDLING ---
MAX_FIELD_BYTES = 100_000

def safe_cell(value: str) -> str:
    if not value:
        return ""
    data = value.encode("utf-8", errors="ignore")
    if len(data) <= MAX_FIELD_BYTES:
        return value
    return data[:MAX_FIELD_BYTES].decode("utf-8", errors="ignore")

def clean_text(value: str) -> str:
    # TAB and newline must be removed for TEXT COPY
    return safe_cell(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")

# ================= HEALTH =================

@app.get("/")
def health():
    return {"status": "backend running"}

# ================= UPLOAD =================

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV allowed")

    conn = get_db()
    cur = conn.cursor()

    try:
        raw = await file.read()
        text = decode_csv_bytes(raw)
        reader = csv.DictReader(StringIO(text))

        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="Empty CSV")

        headers = [normalize(h) for h in reader.fieldnames]
        reader.fieldnames = headers

        schema = detect_schema(headers)
        table = build_table_name(schema, file.filename)

        if schema == "teacher":
            cols = TEACHER_COLUMNS
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
        else:
            cols = SCHOOL_COLUMNS
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

        buffer = StringIO()
        BATCH_SIZE = 5000
        count = 0
        total = 0

        for row in reader:
            values = [
                clean_text((row.get(c) or "").strip())
                for c in cols
            ]
            buffer.write("\t".join(values) + "\n")
            count += 1
            total += 1

            if count >= BATCH_SIZE:
                buffer.seek(0)
                cur.copy_expert(
                    f'''
                    COPY "{table}" ({",".join(cols)})
                    FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
                    ''',
                    buffer
                )
                buffer = StringIO()
                count = 0

        if count > 0:
            buffer.seek(0)
            cur.copy_expert(
                f'''
                COPY "{table}" ({",".join(cols)})
                FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
                ''',
                buffer
            )

        cur.execute(
            "INSERT INTO dataset_registry (table_name, dataset_type, row_count) VALUES (%s,%s,%s)",
            (table, schema, total)
        )

        conn.commit()
        return {"table": table, "type": schema, "rows": total}

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
    return [{"table": r[0], "type": r[1], "rows": r[2], "uploaded_at": r[3]} for r in rows]

# ================= DELETE =================

class DeletePayload(BaseModel):
    table: str

@app.post("/datasets/delete")
def delete_dataset(payload: DeletePayload):
    if not payload.table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table")
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{payload.table}"')
    cur.execute("DELETE FROM dataset_registry WHERE table_name=%s", (payload.table,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted"}

# ================= DOWNLOAD =================

@app.get("/download/{table}")
def download(table: str):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table")

    conn = get_db()
    cur = conn.cursor()

    def stream():
        buf = StringIO()
        cur.copy_expert(f'COPY "{table}" TO STDOUT WITH CSV HEADER', buf)
        buf.seek(0)
        yield from buf
        buf.close()
        cur.close()
        conn.close()

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table}.csv"'}
    )
