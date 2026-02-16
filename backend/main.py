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
    col = col.replace("\ufeff", "")
    col = col.strip()
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
    # 1️⃣ UTF-16 (Excel favorite)
    try:
        text = raw.decode("utf-16")
        print("📄 CSV decoded as utf-16")
        return text
    except UnicodeDecodeError:
        pass

    # 2️⃣ UTF-8 with BOM
    try:
        text = raw.decode("utf-8-sig")
        print("📄 CSV decoded as utf-8-sig")
        return text
    except UnicodeDecodeError:
        pass

    # 3️⃣ Charset auto-detection (Latin-1, Windows-1252)
    result = from_bytes(raw).best()
    if result:
        print(f"📄 CSV decoded as {result.encoding}")
        return str(result)

    raise HTTPException(
        status_code=400,
        detail="Unable to decode CSV file (unsupported encoding)"
    )

MAX_FIELD_BYTES = 120_000  # safely below Postgres COPY limit (131072)

def safe_cell(value: str) -> str:
    if not value:
        return ""
    data = value.encode("utf-8", errors="ignore")
    if len(data) <= MAX_FIELD_BYTES:
        return value
    return data[:MAX_FIELD_BYTES].decode("utf-8", errors="ignore")



# ================= HEALTH =================

@app.api_route("/", methods=["GET", "HEAD"])
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
        # Read & decode ONCE
        raw = await file.read()
        text = decode_csv_bytes(raw)
        stream = StringIO(text)

        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="Empty CSV")

        headers = [normalize(h) for h in reader.fieldnames]
        reader.fieldnames = headers

        schema = detect_schema(headers)
        table = build_table_name(schema, file.filename)

        if schema == "teacher":
            copy_cols = TEACHER_COLUMNS
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
            copy_cols = SCHOOL_COLUMNS
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

        # --- CHUNKED COPY ---
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(copy_cols)  # header

        BATCH_SIZE = 5000
        row_count = 0
        total_rows = 0

        for row in reader:
            writer.writerow([safe_cell((row.get(c) or "").strip())for c in copy_cols])

            row_count += 1
            total_rows += 1

            if row_count >= BATCH_SIZE:
                buffer.seek(0)
                cur.copy_expert(
                    f'''
                    COPY "{table}" ({",".join(copy_cols)})
                    FROM STDIN WITH CSV
                    ''',
                    buffer
                )
                buffer = StringIO()
                writer = csv.writer(buffer)
                row_count = 0

        # Flush remaining rows
        if row_count > 0:
            buffer.seek(0)
            cur.copy_expert(
                f'''
                COPY "{table}" ({",".join(copy_cols)})
                FROM STDIN WITH CSV
                ''',
                buffer
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
