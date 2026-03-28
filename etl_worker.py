# etl_worker.py
"""
ETL worker for NAV 2017 -> Neon(Postgres) using EtlJob queue (production) or dev-run (development).

Implements:
- EXEC_MODE=production|development
- Production: claim EtlJob rows and update status
- Development: run a local synthetic job, no EtlJob reads/writes
- Job dispatch: supports IMPORT_COA, IMPORT_GL_ENTRIES, IMPORT_BUDGET

IMPORT_COA loads:
  ExternalChartOfAccounts (header)
  ExternalAccount (tree attributes: parentExternalId, orderIndex, isPosting, accountType)

IMPORT_GL_ENTRIES loads:
  1) ImportBatch (one per run)
  2) DetailedTransaction (from NAV G_L Entry) incremental by Entry No_
  3) Transaction summary (aggregated from DetailedTransaction)
"""

import os
import time
import json
import signal
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

# Optional: load .env
try:
    import importlib
    spec = importlib.util.find_spec("dotenv")
except Exception:
    spec = None
if spec is not None:
    try:
        dotenv = importlib.import_module("dotenv")
        dotenv.load_dotenv()
    except Exception:
        pass


# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etl-worker")


# -------------------------
# Env config
# -------------------------
EXEC_MODE = os.getenv("EXEC_MODE", "production").lower()
if EXEC_MODE not in ("development", "production"):
    raise RuntimeError("EXEC_MODE must be 'development' or 'production'")

POLL_SECONDS = float(os.getenv("ETL_POLL_SECONDS", "5"))
HEARTBEAT_SECONDS = float(os.getenv("ETL_HEARTBEAT_SECONDS", "20"))
MAX_JOBS = int(os.getenv("ETL_MAX_JOBS", "0"))  # production only

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


# -------------------------
# Engines
# -------------------------
def create_engine_from_env(url: str, name: str) -> Engine:
    return sa.create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=int(os.getenv(f"{name}_POOL_RECYCLE", "1800")),
        pool_size=int(os.getenv(f"{name}_POOL_SIZE", "5")),
        max_overflow=int(os.getenv(f"{name}_MAX_OVERFLOW", "10")),
        future=True,
    )

pg_engine: Engine = create_engine_from_env(require_env("PG_DATABASE_URL"), "PG")
src_engine: Engine = create_engine_from_env(require_env("SRC_DATABASE_URL"), "SRC")


# -------------------------
# Graceful shutdown
# -------------------------
_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    logger.warning("Shutdown signal received")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# -------------------------
# Job queue SQL (production only)
# -------------------------
CLAIM_JOB_SQL = sa.text("""
    UPDATE "EtlJob"
    SET "status" = 'running',
        "startedAt" = now(),
        "updatedAt" = now()
    WHERE "id" = (
      SELECT "id" FROM "EtlJob"
      WHERE "status" = 'pending'
      ORDER BY "createdAt"
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING *;
""")

MARK_SUCCESS_SQL = sa.text("""
    UPDATE "EtlJob"
    SET "status" = 'success',
        "finishedAt" = now(),
        "updatedAt" = now(),
        "meta" = :meta,
        "errorMessage" = NULL
    WHERE "id" = :id
""")

MARK_FAILED_SQL = sa.text("""
    UPDATE "EtlJob"
    SET "status" = 'failed',
        "finishedAt" = now(),
        "updatedAt" = now(),
        "errorMessage" = :error,
        "meta" = :meta
    WHERE "id" = :id
""")

HEARTBEAT_SQL = sa.text("""
    UPDATE "EtlJob"
    SET "updatedAt" = now()
    WHERE "id" = :id
""")


# -------------------------
# Helpers
# -------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def json_safe(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)

def parse_meta(job: Dict[str, Any]) -> Dict[str, Any]:
    meta = job.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {"raw_meta": meta}
    if not isinstance(meta, dict):
        meta = {"raw_meta": str(meta)}
    return meta


# -------------------------
# Production job handling
# -------------------------
def get_pending_job() -> Optional[Dict[str, Any]]:
    with pg_engine.begin() as conn:
        row = conn.execute(CLAIM_JOB_SQL).mappings().first()
        return dict(row) if row else None

def heartbeat(job_id: str):
    with pg_engine.begin() as conn:
        conn.execute(HEARTBEAT_SQL, {"id": job_id})

def mark_success(job_id: str, meta: Dict[str, Any]):
    with pg_engine.begin() as conn:
        conn.execute(MARK_SUCCESS_SQL, {"id": job_id, "meta": json_safe(meta)})

def mark_failed(job_id: str, error: str, meta: Dict[str, Any]):
    with pg_engine.begin() as conn:
        conn.execute(
            MARK_FAILED_SQL,
            {
                "id": job_id,
                "error": (error or "")[:2000],
                "meta": json_safe(meta),
            },
        )


# -------------------------
# NAV helpers
# -------------------------
def nav_table(company_name: str, base_table: str) -> str:
    """
    NAV SQL Server table naming: [<Company Name>$<Table Name>]
    Example: [HVG$G_L Account]
    """
    return f"[{company_name}${base_table}]"

def try_select_columns(engine: Engine, table_full: str, preferred_cols: List[str]) -> List[str]:
    """
    Probe which columns exist. Returns the subset that exists.
    """
    tf = table_full.strip("[]")
    q = sa.text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"table_name": tf}).fetchall()
    existing = {r[0] for r in rows}
    return [c for c in preferred_cols if c in existing]


# -------------------------
# Mapping helpers
# -------------------------
def map_nav_account_type(row: pd.Series) -> str:
    """
    Determine AccountType using:
    1) NAV 'Account Category' (preferred)
    2) Account number prefix fallback (1xxx, 2xxx, etc.)
    """

    cat = str(row.get("Account Category", "") or "").strip().upper()
    if cat:
        if "ASSET" in cat:
            return "ASSET"
        if "LIABIL" in cat:
            return "LIABILITY"
        if "EQUITY" in cat:
            return "EQUITY"
        if "INCOME" in cat and "OTHER" not in cat:
            return "INCOME"
        if "COST" in cat or "COGS" in cat:
            return "COGS"
        if "EXPENSE" in cat and "OTHER" not in cat:
            return "EXPENSE"
        if "OTHER" in cat and "INCOME" in cat:
            return "OTHER_INCOME"
        if "OTHER" in cat and "EXPENSE" in cat:
            return "OTHER_EXPENSE"

    code = str(row.get("code", "") or "").strip()
    first_digit = code[0] if code and code[0].isdigit() else None

    if first_digit == "1":
        return "ASSET"
    if first_digit == "2":
        return "LIABILITY"
    if first_digit == "3":
        return "EQUITY"
    if first_digit == "4":
        return "INCOME"
    if first_digit == "5":
        return "COGS"
    if first_digit == "6":
        return "EXPENSE"
    if first_digit == "7":
        return "OTHER_INCOME"
    if first_digit == "8":
        return "OTHER_EXPENSE"

    return "EXPENSE"


def nav_is_posting(row: pd.Series) -> bool:
    acct_type = str(row.get("Account Type", "") or "").strip().upper()
    if acct_type:
        return acct_type == "POSTING"
    return True


def compute_parent_by_indent(df: pd.DataFrame, indent_col: str) -> List[Optional[str]]:
    parents: List[Optional[str]] = []
    stack: List[Tuple[int, str]] = []  # (indent, external_account_id)

    for _, r in df.iterrows():
        indent = int(r[indent_col]) if pd.notna(r[indent_col]) else 0
        cur_id = r["id"]

        while stack and stack[-1][0] >= indent:
            stack.pop()

        parent_id = stack[-1][1] if stack else None
        parents.append(parent_id)

        stack.append((indent, cur_id))

    return parents


# -------------------------
# IMPORT_COA
# -------------------------
def etl_import_coa(job: Dict[str, Any], meta: Dict[str, Any], heartbeat_cb) -> Dict[str, Any]:
    external_coa_id = meta.get("externalCoaId")
    organization_id = meta.get("organizationId")
    nav_company = meta.get("navCompanyName")
    external_coa_name = meta.get("externalCoaName") or f"NAV CoA ({nav_company})"
    source_system = meta.get("sourceSystem") or "NAV2017"

    if not external_coa_id or not organization_id or not nav_company:
        raise RuntimeError("IMPORT_COA requires meta: organizationId, externalCoaId, navCompanyName")

    table = nav_table(nav_company, "G_L Account")

    preferred = [
        "No_",
        "Name",
        "Account Type",
        "Account Category",
        "Blocked",
    ]
    existing_cols = try_select_columns(src_engine, table, preferred)

    if "No_" not in existing_cols or "Name" not in existing_cols:
        raise RuntimeError(f"Required columns not found in {table}. Found: {existing_cols}")

    select_list = ", ".join([f"[{c}]" for c in existing_cols])
    sql = f"""
        SELECT {select_list}
        FROM {table}
        WHERE [No_] IS NOT NULL
          AND LTRIM(RTRIM([No_])) <> ''
    """
    df = pd.read_sql_query(sql, src_engine)

    heartbeat_cb()

    if df.empty:
        meta["rows_extracted"] = 0
        meta["rows_loaded"] = 0
        meta["note"] = "No accounts found."
        return meta

    df.rename(columns={"No_": "code", "Name": "name"}, inplace=True)
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()

    # if "Blocked" in df.columns:
    #     df = df[df["Blocked"] == 0]

    # POC: indentation = length(code)
    df["indent"] = df["code"].str.len().fillna(0).astype(int)

    df.sort_values(by=["code"], inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)

    df["id"] = df["code"].apply(lambda c: f"{external_coa_id}:{c}")

    df["accountType"] = df.apply(map_nav_account_type, axis=1)
    df["isPosting"] = df.apply(nav_is_posting, axis=1)

    df["orderIndex"] = (df.index + 1) * 10
    df["parentExternalId"] = compute_parent_by_indent(df, "indent")

    load_df = pd.DataFrame({
        "id": df["id"],
        "externalCoaId": external_coa_id,
        "code": df["code"],
        "name": df["name"],
        "accountType": df["accountType"],
        "isPosting": df["isPosting"].astype(bool),
        "parentExternalId": df["parentExternalId"],
        "orderIndex": df["orderIndex"].astype(int),
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc),
    })

    meta["rows_extracted"] = int(len(load_df))

    with pg_engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO "ExternalChartOfAccounts" ("id","organizationId","name","sourceSystem","createdAt","updatedAt")
            VALUES (:id,:org,:name,:src, now(), now())
            ON CONFLICT ("id") DO UPDATE
              SET "organizationId" = EXCLUDED."organizationId",
                  "name" = EXCLUDED."name",
                  "sourceSystem" = EXCLUDED."sourceSystem",
                  "updatedAt" = now();
        """), {"id": external_coa_id, "org": organization_id, "name": external_coa_name, "src": source_system})

        conn.execute(sa.text("""DELETE FROM "ExternalAccount" WHERE "externalCoaId" = :id"""), {"id": external_coa_id})

        insert_sql = sa.text("""
            INSERT INTO "ExternalAccount"
            ("id","externalCoaId","code","name","accountType","isPosting","parentExternalId","orderIndex","createdAt","updatedAt")
            VALUES
            (:id,:externalCoaId,:code,:name,:accountType,:isPosting,:parentExternalId,:orderIndex,:createdAt,:updatedAt)
        """)

        records = load_df.to_dict(orient="records")
        chunk_size = 5000
        for i in range(0, len(records), chunk_size):
            conn.execute(insert_sql, records[i:i + chunk_size])

    heartbeat_cb()

    meta["rows_loaded"] = int(len(load_df))
    meta["externalCoaId"] = external_coa_id
    meta["navCompanyName"] = nav_company
    meta["note"] = "External CoA snapshot imported."
    return meta


# -------------------------
# IMPORT_GL_ENTRIES
# -------------------------

def create_import_batch(organization_id: str, source_system: str, source_reference: str) -> str:
    """
    Creates ImportBatch row and returns its id.
    We rely on ImportBatch.status default (DRAFT) to avoid enum mismatch.
    """
    batch_id = str(uuid.uuid4())
    with pg_engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO "ImportBatch" ("id","organizationId","sourceSystem","sourceReference","message","importedAt", "status")
            VALUES (:id, :org, :src, :ref, :msg, now(), 'DRAFT');
        """), {
            "id": batch_id,
            "org": organization_id,
            "src": source_system,
            "ref": source_reference,
            "msg": "ETL import started",
        })
    return batch_id


def update_import_batch_message(batch_id: str, message: str) -> None:
    with pg_engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE "ImportBatch"
            SET "message" = :msg
            WHERE "id" = :id
        """), {"id": batch_id, "msg": message[:1000]})


def build_gl_entry_query(nav_company: str, date_from: str = None, date_to: str = None) -> str:
    """
    Minimal NAV query:
    - Pulls from G_L Entry
    - Joins G_L Account for name
    - Joins Customer/Vendor/Bank Account for source name
    - Optional date range filter via date_from / date_to (YYYY-MM-DD strings)
    """
    gle = nav_table(nav_company, "G_L Entry")
    gla = nav_table(nav_company, "G_L Account")
    cust = "Customer"
    vend = "Vendor"
    bank = nav_table(nav_company, "Bank Account")

    # gle = nav_table(nav_company, "G_L Entry")
    # gla = nav_table(nav_company, "G_L Account")
    # cust = nav_table(nav_company, "Customer")
    # vend = nav_table(nav_company, "Vendor")
    # bank = nav_table(nav_company, "Bank Account")

    # Note: Some NAV DBs name "No_ Comprobante Fiscal" differently; keep as optional later if needed.
    # For POC we select it if exists, else empty string in pandas.
    return f"""
        SELECT
            GLE.[Entry No_]               AS entry_no,
            GLE.[Transaction No_]         AS transaction_no,
            GLE.[Dimension Set ID]        AS dimension_set_id,

            GLE.[G_L Account No_]         AS gl_account_no,
            GLA.[Name]                    AS gl_account_name,

            GLE.[Posting Date]            AS posting_date,
            GLE.[Document Date]           AS document_date,

            GLE.[Document Type]           AS document_type,
            GLE.[Document No_]            AS document_no,
            GLE.[External Document No_]   AS external_document_no,

            GLE.[Description]             AS description,
            GLE.[Source Code]             AS source_code,
            GLE.[Source Type]             AS source_type,
            GLE.[Source No_]              AS source_no,

            COALESCE(
                CASE WHEN GLE.[Source Type] = 1 THEN C.[Name] END,
                CASE WHEN GLE.[Source Type] = 2 THEN V.[Name] END,
                CASE WHEN GLE.[Source Type] = 3 THEN B.[Name] END,
                ''
            ) AS source_name,

            GLE.[Amount]                  AS amount,
            GLE.[Debit Amount]            AS debit_amount,
            GLE.[Credit Amount]           AS credit_amount,

            GLE.[Additional-Currency Amount]      AS usd_amount,
            GLE.[Add_-Currency Debit Amount]      AS usd_debit_amount,
            GLE.[Add_-Currency Credit Amount]     AS usd_credit_amount

        FROM {gle} AS GLE
        LEFT JOIN {gla} AS GLA
            ON GLE.[G_L Account No_] = GLA.[No_]
        LEFT JOIN {cust} AS C
            ON GLE.[Source Type] = 1 AND GLE.[Source No_] = C.[No_]
        LEFT JOIN {vend} AS V
            ON GLE.[Source Type] = 2 AND GLE.[Source No_] = V.[No_]
        LEFT JOIN {bank} AS B
            ON GLE.[Source Type] = 3 AND GLE.[Source No_] = B.[No_]

        WHERE 1=1
            {"AND GLE.[Posting Date] >= '" + date_from + "'" if date_from else ""}
            {"AND GLE.[Posting Date] <  '" + date_to   + "'" if date_to   else ""}

        ORDER BY GLE.[Entry No_];
    """


def document_type_name_from_int(x: Any) -> str:
    """
    POC mapping based on your older query.
    NAV often uses:
      0=" " (blank), 1=Payment, 2=Invoice, 3=Credit Memo, 4=Finance Charge Memo
    """
    try:
        v = int(x)
    except Exception:
        return ""
    if v == 1:
        return "PAGO"
    if v == 2:
        return "FACTURA"
    if v == 3:
        return "NOTA CREDITO"
    if v == 4:
        return "CARGO FINANCIERO"
    return ""


def _pg_copy_insert(table, conn, keys, data_iter):
    """
    Bulk-load rows via PostgreSQL COPY FROM STDIN (CSV).
    Replaces method='multi' to avoid per-parameter SQL limits that blow up on full loads.
    None values are written as the literal \\N which COPY treats as NULL.
    """
    import csv as _csv
    from io import StringIO

    buf = StringIO()
    writer = _csv.writer(buf)
    for row in data_iter:
        writer.writerow([r"\N" if v is None else v for v in row])
    buf.seek(0)

    cols = ", ".join(f'"{k}"' for k in keys)
    tbl = f'"{table.name}"'
    with conn.connection.cursor() as cur:
        cur.copy_expert(
            f"COPY {tbl} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
            buf,
        )


def etl_import_gl_entries(job: Dict[str, Any], meta: Dict[str, Any], heartbeat_cb) -> Dict[str, Any]:
    organization_id = meta.get("organizationId")
    external_coa_id = meta.get("externalCoaId")
    nav_company = meta.get("navCompanyName")
    source_system = meta.get("sourceSystem") or "OTHER"

    if not organization_id or not external_coa_id or not nav_company:
        raise RuntimeError("IMPORT_GL_ENTRIES requires meta: organizationId, externalCoaId, navCompanyName")

    # Create ImportBatch for this run
    source_reference = f"etl:{source_system}:{nav_company}:gl_entries"
    import_batch_id = create_import_batch(organization_id, source_system, source_reference)

    meta["importBatchId"] = import_batch_id

    logger.info(f"[IMPORT_GL_ENTRIES] org={organization_id} company={nav_company}")

    # Optional date range from meta (e.g. "2024-08-01" / "2024-09-01"); omit for full load
    date_from = meta.get("dateFrom")
    date_to   = meta.get("dateTo")

    # Extract
    sql = build_gl_entry_query(nav_company, date_from=date_from, date_to=date_to)
    logger.info("[IMPORT_GL_ENTRIES] Starting NAV extract in chunks...")

    # Stream in chunks to avoid memory blowups
    chunksize = 50000  # adjust if needed
    rows_loaded = 0

    # NOTE: caller guarantees destination tables are empty before each run (no de-dupe needed).
    start_time = time.time()
    last_progress_log = start_time
    progress_every_seconds = float(os.getenv("ETL_PROGRESS_SECONDS", "10"))
    
    for chunk_idx, df in enumerate(pd.read_sql_query(sql, src_engine, chunksize=chunksize), start=1):
        if _shutdown:
            raise RuntimeError("Shutdown requested")

        heartbeat_cb()

        if df.empty:
            logger.info(f"[IMPORT_GL_ENTRIES] chunk={chunk_idx} empty")
            continue

        # Transform
        df["accountNo"] = df["gl_account_no"].astype(str).str.strip().apply(lambda c: f"{external_coa_id}:{c}")
        df["accountName"] = df.get("gl_account_name", "").fillna("").astype(str)

        df["postingDate"] = pd.to_datetime(df["posting_date"], errors="coerce")
        df["documentDate"] = pd.to_datetime(df["document_date"], errors="coerce")

        df["documentTypeName"] = df["document_type"].apply(document_type_name_from_int)

        # Strings normalize (avoid None to satisfy NOT NULL if you have constraints)
        df["documentNo"] = df.get("document_no", "").fillna("").astype(str)
        df["externalDocumentNo"] = df.get("external_document_no", "").fillna("").astype(str)
        df["description"] = df.get("description", "").fillna("").astype(str)
        df["sourceCode"] = df.get("source_code", "").fillna("").astype(str)
        df["sourceNo"] = df.get("source_no", "").fillna("").astype(str)
        df["sourceName"] = df.get("source_name", "").fillna("").astype(str)

        # Numeric fields — parse then invert signs (source system uses opposite convention)
        for col in ["amount", "debit_amount", "credit_amount", "usd_amount", "usd_debit_amount", "usd_credit_amount"]:
            if col in df.columns:
                df[col] = -pd.to_numeric(df[col], errors="coerce")

        # NCF / navUrl not implemented for now (POC)
        df["ncf"] = ""
        df["navUrl"] = ""

        df_out = pd.DataFrame({
            "id": [str(uuid.uuid4()) for _ in range(len(df))],
            "organizationId": organization_id,
            "transactionNo": df["transaction_no"].fillna(0).astype(int),
            "entryNo": df["entry_no"].fillna(0).astype(int),
            "dimensionSetId": df["dimension_set_id"].fillna(0).astype(int),
            "importBatchId": import_batch_id,
            "accountNo": df["accountNo"],
            "accountName": df["accountName"],
            "postingDate": df["postingDate"].apply(
                lambda x: x.to_pydatetime() if pd.notna(x) else datetime(1970, 1, 1, tzinfo=timezone.utc)
            ),
            "documentDate": df["documentDate"].apply(
                lambda x: x.to_pydatetime() if pd.notna(x) else datetime(1970, 1, 1, tzinfo=timezone.utc)
            ),
            "documentTypeName": df["documentTypeName"],
            "documentNo": df["documentNo"],
            "externalDocumentNo": df["externalDocumentNo"],
            "description": df["description"],
            "sourceCode": df["sourceCode"],
            "sourceType": df["source_type"].fillna(0).astype(int),
            "sourceNo": df["sourceNo"],
            "sourceName": df["sourceName"],
            "ncf": df["ncf"],
            "navUrl": df["navUrl"],
            "amount": df.get("amount"),
            "debitAmount": df.get("debit_amount"),
            "creditAmount": df.get("credit_amount"),
            "usdAmount": df.get("usd_amount"),
            "usdDebitAmount": df.get("usd_debit_amount"),
            "usdCreditAmount": df.get("usd_credit_amount"),
            "createdAt": datetime.now(timezone.utc),
        })

        # Bulk insert via COPY FROM STDIN — no per-parameter SQL limits
        df_out.to_sql(
            name="DetailedTransaction",
            con=pg_engine,
            if_exists="append",
            index=False,
            method=_pg_copy_insert,
        )

        rows_loaded += len(df_out)
        elapsed = max(time.time() - start_time, 0.001)
        rate = rows_loaded / elapsed
        logger.info(
            f"[IMPORT_GL_ENTRIES] chunk={chunk_idx} inserted {len(df_out)} rows (total={rows_loaded}, rate={rate:.1f} rows/s)"
        )

        now_ts = time.time()
        if progress_every_seconds > 0 and (now_ts - last_progress_log) >= progress_every_seconds:
            logger.info(
                f"[IMPORT_GL_ENTRIES] progress total={rows_loaded} rows, rate={rate:.1f} rows/s"
            )
            last_progress_log = now_ts

    heartbeat_cb()

    # Diagnostic: verify DetailedTransaction rows are visible before building summary
    with pg_engine.connect() as conn:
        dt_count = conn.execute(
            sa.text('SELECT COUNT(*) FROM "DetailedTransaction" WHERE "importBatchId"::text = :batchId'),
            {"batchId": import_batch_id},
        ).scalar()
    logger.info(f"[IMPORT_GL_ENTRIES] DetailedTransaction visible rows for batch={import_batch_id}: {dt_count}")
    if dt_count == 0:
        logger.warning("[IMPORT_GL_ENTRIES] No DetailedTransaction rows found — Transaction summary will be empty")

    # Create Transaction summaries for this batch
    # POC summary: group by (externalAccountId, postingDate) from DetailedTransaction
    # externalAccountId = DetailedTransaction.accountNo
    logger.info(f"[IMPORT_GL_ENTRIES] Building Transaction summary for batch={import_batch_id}")
    with pg_engine.begin() as conn:
        result = conn.execute(sa.text("""
            INSERT INTO "Transaction" (
              "id",
              "organizationId",
              "externalAccountId",
              "importBatchId",
              "postingDate",
              "amount",
              "usdAmount",
              "description",
              "isElimination",
              "createdAt"
            )
            SELECT
              gen_random_uuid() AS "id",
              dt."organizationId",
              dt."accountNo" AS "externalAccountId",
              dt."importBatchId",
              DATE(dt."postingDate") AS "postingDate",
              CAST(COALESCE(SUM(dt."amount"), 0) AS decimal(18,4)) AS "amount",
              CAST(COALESCE(SUM(dt."usdAmount"), 0) AS decimal(18,4)) AS "usdAmount",
              NULL::text AS "description",
              false AS "isElimination",
              now() AS "createdAt"
            FROM "DetailedTransaction" dt
            WHERE dt."importBatchId"::text = :batchId
            GROUP BY dt."organizationId", dt."accountNo", dt."importBatchId", DATE(dt."postingDate");
        """), {"batchId": import_batch_id})
    logger.info(f"[IMPORT_GL_ENTRIES] Transaction summary created for batch={import_batch_id}: {result.rowcount} rows")

    # Update ImportBatch message
    update_import_batch_message(
        import_batch_id,
        f"Imported DetailedTransaction={rows_loaded}. Summary Transaction created."
    )

    meta["rows_loaded_detailed"] = rows_loaded
    meta["note"] = "GL Entries imported (full load) + summary built."
    return meta


# -------------------------
# IMPORT_BUDGET
# -------------------------

def build_budget_query(nav_company: str) -> str:
    be = nav_table(nav_company, "G_L Budget Entry")
    bn = nav_table(nav_company, "G_L Budget Name")
    return f"""
        SELECT
            BE.[Entry No_]                        AS entry_no,
            BE.[Budget Name]                      AS budget_name,
            COALESCE(BN.[Name], BE.[Budget Name]) AS scenario,
            BE.[G_L Account No_]                  AS gl_account_no,
            BE.[Date]                             AS budget_date,
            BE.[Amount]                           AS amount
        FROM {be} AS BE
        LEFT JOIN {bn} AS BN
            ON BE.[Budget Name] = BN.[Name]
        WHERE BE.[G_L Account No_] IS NOT NULL
          AND LTRIM(RTRIM(BE.[G_L Account No_])) <> ''
          AND BE.[Date] IS NOT NULL
        ORDER BY BE.[Entry No_]
    """


def etl_import_budget(job: Dict[str, Any], meta: Dict[str, Any], heartbeat_cb) -> Dict[str, Any]:
    organization_id = meta.get("organizationId")
    external_coa_id = meta.get("externalCoaId")
    nav_company     = meta.get("navCompanyName")

    if not organization_id or not external_coa_id or not nav_company:
        raise RuntimeError("IMPORT_BUDGET requires meta: organizationId, externalCoaId, navCompanyName")

    logger.info(f"[IMPORT_BUDGET] org={organization_id} company={nav_company}")

    # Extract
    sql = build_budget_query(nav_company)
    df = pd.read_sql_query(sql, src_engine)

    heartbeat_cb()

    meta["rows_extracted"] = int(len(df))

    if df.empty:
        meta["rows_loaded"] = 0
        meta["note"] = "No budget entries found."
        return meta

    # Transform
    df["externalAccountId"] = df["gl_account_no"].astype(str).str.strip().apply(
        lambda c: f"{external_coa_id}:{c}"
    )
    budget_dates = pd.to_datetime(df["budget_date"], errors="coerce")
    df["year"]  = budget_dates.dt.year.fillna(0).astype(int)
    df["month"] = budget_dates.dt.month.fillna(0).astype(int)
    df["scenario"] = df["scenario"].fillna("Original").astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    load_df = pd.DataFrame({
        "id":                         [str(uuid.uuid4()) for _ in range(len(df))],
        "organizationId":             organization_id,
        "externalChartOfAccountsId":  external_coa_id,
        "externalAccountId":          df["externalAccountId"],
        "year":                       df["year"],
        "month":                      df["month"],
        "scenario":                   df["scenario"],
        "amount":                     df["amount"],
        "createdAt":                  datetime.now(timezone.utc),
        "updatedAt":                  datetime.now(timezone.utc),
    })

    # Load: delete existing rows for this org+CoA, then bulk insert
    with pg_engine.begin() as conn:
        conn.execute(sa.text("""
            DELETE FROM "ExternalBudget"
            WHERE "organizationId" = :orgId
              AND "externalChartOfAccountsId" = :coaId
        """), {"orgId": organization_id, "coaId": external_coa_id})

    load_df.to_sql(
        name="ExternalBudget",
        con=pg_engine,
        if_exists="append",
        index=False,
        method=_pg_copy_insert,
    )

    heartbeat_cb()

    meta["rows_loaded"] = int(len(load_df))
    meta["note"] = "Budget entries imported (full load)."
    return meta


# -------------------------
# Dispatch
# -------------------------
def run_etl_for_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("id", "DEV_JOB")
    meta = parse_meta(job)

    job_type = (job.get("type") or meta.get("type") or "unknown").upper()

    meta["job_type"] = job_type
    meta["started_utc"] = utc_now_iso()

    last_heartbeat = time.time()

    def maybe_heartbeat():
        nonlocal last_heartbeat
        if EXEC_MODE != "production":
            return
        if HEARTBEAT_SECONDS <= 0:
            return
        if time.time() - last_heartbeat >= HEARTBEAT_SECONDS:
            heartbeat(job_id)
            last_heartbeat = time.time()
            logger.debug(f"[{job_id}] Heartbeat")

    logger.info(f"[{job_id}] Running {job_type} (mode={EXEC_MODE})")

    if job_type == "IMPORT_COA":
        meta = etl_import_coa(job, meta, maybe_heartbeat)
    elif job_type == "IMPORT_GL_ENTRIES":
        meta = etl_import_gl_entries(job, meta, maybe_heartbeat)
    elif job_type == "IMPORT_BUDGET":
        meta = etl_import_budget(job, meta, maybe_heartbeat)
    else:
        raise RuntimeError(f"Unsupported job type: {job_type}")

    meta["finished_utc"] = utc_now_iso()
    return meta


# -------------------------
# Main loop
# -------------------------
def main_loop():
    logger.info(f"ETL worker starting. EXEC_MODE={EXEC_MODE}")

    # -------- DEVELOPMENT MODE --------


    
    if EXEC_MODE == "development":

        # AOR DOMINICANA

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "Yg5avCX14YIe8TRTu8RCMrYOWCaCv3Co",
        #         "externalCoaId": "nav.aor.coa",
        #         "externalCoaName": "AOR CoA",
        #         "navCompanyName": "AOR DOMINICANA",
        #         "sourceSystem": "OTHER"
        #     },
        # }        

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "Yg5avCX14YIe8TRTu8RCMrYOWCaCv3Co",
        #         "externalCoaId": "nav.aor.coa",
        #         "externalCoaName": "AOR CoA",
        #         "navCompanyName": "AOR DOMINICANA",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "Yg5avCX14YIe8TRTu8RCMrYOWCaCv3Co",
        #         "externalCoaId": "nav.aor.coa",
        #         "externalCoaName": "AOR CoA",
        #         "navCompanyName": "AOR DOMINICANA",
        #         "sourceSystem": "OTHER"
        #     },
        # }        

        # MEDIAVEST DOMINICANA

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "NTN3NkieI5qAMJRT7sYrawp8ev5q1gS2",
        #         "externalCoaId": "nav.mediavest.coa",
        #         "externalCoaName": "Mediavest CoA",
        #         "navCompanyName": "MEDIAVEST DOMINICANA SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "NTN3NkieI5qAMJRT7sYrawp8ev5q1gS2",
        #         "externalCoaId": "nav.mediavest.coa",
        #         "externalCoaName": "Mediavest CoA",
        #         "navCompanyName": "MEDIAVEST DOMINICANA SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "NTN3NkieI5qAMJRT7sYrawp8ev5q1gS2",
        #         "externalCoaId": "nav.mediavest.coa",
        #         "externalCoaName": "Mediavest CoA",
        #         "navCompanyName": "MEDIAVEST DOMINICANA SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }


        # DNC

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "AEzyIwkOkhnGpMiW1qdbJeUK53y6eadl",
        #         "externalCoaId": "nav.dnc.coa",
        #         "externalCoaName": "DNC CoA",
        #         "navCompanyName": "DNC SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "AEzyIwkOkhnGpMiW1qdbJeUK53y6eadl",
        #         "externalCoaId": "nav.dnc.coa",
        #         "externalCoaName": "DNC CoA",
        #         "navCompanyName": "DNC SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }        

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "AEzyIwkOkhnGpMiW1qdbJeUK53y6eadl",
        #         "externalCoaId": "nav.dnc.coa",
        #         "externalCoaName": "DNC CoA",
        #         "navCompanyName": "DNC SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }        


        # PUBLICIS

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "GWBUVFsX3OWH8BES4mExlMcAxebCwcw7",
        #         "externalCoaId": "nav.publicis.coa",
        #         "externalCoaName": "Publicis CoA",
        #         "navCompanyName": "DAMARIS DEFILLO PUBLICIDAD",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "GWBUVFsX3OWH8BES4mExlMcAxebCwcw7",
        #         "externalCoaId": "nav.publicis.coa",
        #         "externalCoaName": "Publicis CoA",
        #         "navCompanyName": "DAMARIS DEFILLO PUBLICIDAD",
        #         "sourceSystem": "OTHER"
        #     },
        # }   

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "GWBUVFsX3OWH8BES4mExlMcAxebCwcw7",
        #         "externalCoaId": "nav.publicis.coa",
        #         "externalCoaName": "Publicis CoA",
        #         "navCompanyName": "DAMARIS DEFILLO PUBLICIDAD",
        #         "sourceSystem": "OTHER"
        #     },
        # }   

        # DEFILLO GROUP

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "HoxvZCZdFCjtJGIowQXZGGSPjHcoOcbM",
        #         "externalCoaId": "nav.defillogroup.coa",
        #         "externalCoaName": "Defillo Group CoA",
        #         "navCompanyName": "DEFILLO GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "HoxvZCZdFCjtJGIowQXZGGSPjHcoOcbM",
        #         "externalCoaId": "nav.defillogroup.coa",
        #         "externalCoaName": "Defillo Group CoA",
        #         "navCompanyName": "DEFILLO GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # } 

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "HoxvZCZdFCjtJGIowQXZGGSPjHcoOcbM",
        #         "externalCoaId": "nav.defillogroup.coa",
        #         "externalCoaName": "Defillo Group CoA",
        #         "navCompanyName": "DEFILLO GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # } 

        # ALQUILERES CORPORATIVOS

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "OkNRNMJd3ORgi0llzXs7RWLXwyZYp4WW",
        #         "externalCoaId": "nav.alquilerescorporativos.coa",
        #         "externalCoaName": "Alquileres Corporativos CoA",
        #         "navCompanyName": "ALQUILERES CORPORATIVOS",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "OkNRNMJd3ORgi0llzXs7RWLXwyZYp4WW",
        #         "externalCoaId": "nav.alquilerescorporativos.coa",
        #         "externalCoaName": "Alquileres Corporativos CoA",
        #         "navCompanyName": "ALQUILERES CORPORATIVOS",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "OkNRNMJd3ORgi0llzXs7RWLXwyZYp4WW",
        #         "externalCoaId": "nav.alquilerescorporativos.coa",
        #         "externalCoaName": "Alquileres Corporativos CoA",
        #         "navCompanyName": "ALQUILERES CORPORATIVOS",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # ADMINISTRACION DE INVERSIONES

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "SaZ5pvmXy4c3Zh2ZNmQbgBlPOE4zr7B2",
        #         "externalCoaId": "nav.administraciondeinversiones.coa",
        #         "externalCoaName": "Administracion de Inversiones CoA",
        #         "navCompanyName": "ADMINISTRACION DE INVERSIONES",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "SaZ5pvmXy4c3Zh2ZNmQbgBlPOE4zr7B2",
        #         "externalCoaId": "nav.administraciondeinversiones.coa",
        #         "externalCoaName": "Administracion de Inversiones CoA",
        #         "navCompanyName": "ADMINISTRACION DE INVERSIONES",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "SaZ5pvmXy4c3Zh2ZNmQbgBlPOE4zr7B2",
        #         "externalCoaId": "nav.administraciondeinversiones.coa",
        #         "externalCoaName": "Administracion de Inversiones CoA",
        #         "navCompanyName": "ADMINISTRACION DE INVERSIONES",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # SHARE CENTER

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "SbnysQxtcyGCUgCzx0nzPU7fpJ4trjQN",
        #         "externalCoaId": "nav.sharecenter.coa",
        #         "externalCoaName": "Share Center CoA",
        #         "navCompanyName": "SHARE CENTER",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "SbnysQxtcyGCUgCzx0nzPU7fpJ4trjQN",
        #         "externalCoaId": "nav.sharecenter.coa",
        #         "externalCoaName": "Share Center CoA",
        #         "navCompanyName": "SHARE CENTER",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "SbnysQxtcyGCUgCzx0nzPU7fpJ4trjQN",
        #         "externalCoaId": "nav.sharecenter.coa",
        #         "externalCoaName": "Share Center CoA",
        #         "navCompanyName": "SHARE CENTER",
        #         "sourceSystem": "OTHER"
        #     },
        # }  


        # EDIM DIGITAL

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "XQM4JJIGzHNgwVvxK6yWn0AnmeOurpZf",
        #         "externalCoaId": "nav.edimdigital.coa",
        #         "externalCoaName": "Edim Digital CoA",
        #         "navCompanyName": "EDIM DIGITAL GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "XQM4JJIGzHNgwVvxK6yWn0AnmeOurpZf",
        #         "externalCoaId": "nav.edimdigital.coa",
        #         "externalCoaName": "Edim Digital CoA",
        #         "navCompanyName": "EDIM DIGITAL GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # }         

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "XQM4JJIGzHNgwVvxK6yWn0AnmeOurpZf",
        #         "externalCoaId": "nav.edimdigital.coa",
        #         "externalCoaName": "Edim Digital CoA",
        #         "navCompanyName": "EDIM DIGITAL GROUP",
        #         "sourceSystem": "OTHER"
        #     },
        # }         

        # IMSA

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "Xj8KRZpZs8mUzu9SChwsyElnz62AFN7o",
        #         "externalCoaId": "nav.imsa.coa",
        #         "externalCoaName": "IMSA CoA",
        #         "navCompanyName": "IMSA",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "Xj8KRZpZs8mUzu9SChwsyElnz62AFN7o",
        #         "externalCoaId": "nav.imsa.coa",
        #         "externalCoaName": "IMSA CoA",
        #         "navCompanyName": "IMSA",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "Xj8KRZpZs8mUzu9SChwsyElnz62AFN7o",
        #         "externalCoaId": "nav.imsa.coa",
        #         "externalCoaName": "IMSA CoA",
        #         "navCompanyName": "IMSA",
        #         "sourceSystem": "OTHER"
        #     },
        # }  

        # CENTRAL DE MEDIOS

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "p2E3oMlLSTk8ptGajYQCgvOrrnOBGkiU",
        #         "externalCoaId": "nav.cdm.coa",
        #         "externalCoaName": "Central de Medios CoA",
        #         "navCompanyName": "CENTRAL DE MEDIOS, SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "p2E3oMlLSTk8ptGajYQCgvOrrnOBGkiU",
        #         "externalCoaId": "nav.cdm.coa",
        #         "externalCoaName": "Central de Medios CoA",
        #         "navCompanyName": "CENTRAL DE MEDIOS, SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }           

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "p2E3oMlLSTk8ptGajYQCgvOrrnOBGkiU",
        #         "externalCoaId": "nav.cdm.coa",
        #         "externalCoaName": "Central de Medios CoA",
        #         "navCompanyName": "CENTRAL DE MEDIOS, SRL",
        #         "sourceSystem": "OTHER"
        #     },
        # }           

        # ACCUMBENS

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_COA",
        #     "meta": {
        #         "type": "IMPORT_COA",
        #         "organizationId": "satijlk99W0JpO6BD85ZrZ0qdYLhGfpT",
        #         "externalCoaId": "nav.accumbens.coa",
        #         "externalCoaName": "Accumbens CoA",
        #         "navCompanyName": "ACCUMBENS SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_GL_ENTRIES",
        #     "meta": {
        #         "type": "IMPORT_GL_ENTRIES",
        #         "organizationId": "satijlk99W0JpO6BD85ZrZ0qdYLhGfpT",
        #         "externalCoaId": "nav.accumbens.coa",
        #         "externalCoaName": "Accumbens CoA",
        #         "navCompanyName": "ACCUMBENS SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        # fake_job = {
        #     "id": "DEV_JOB",
        #     "type": "IMPORT_BUDGET",
        #     "meta": {
        #         "type": "IMPORT_BUDGET",
        #         "organizationId": "satijlk99W0JpO6BD85ZrZ0qdYLhGfpT",
        #         "externalCoaId": "nav.accumbens.coa",
        #         "externalCoaName": "Accumbens CoA",
        #         "navCompanyName": "ACCUMBENS SAS",
        #         "sourceSystem": "OTHER"
        #     },
        # }

        t0 = time.time()
        try:
            meta = run_etl_for_job(fake_job)
            meta["duration_seconds"] = round(time.time() - t0, 3)
            logger.info("Development ETL completed successfully")
            logger.info("ETL META:")
            logger.info(json_safe(meta))
        except Exception as e:
            logger.exception(f"Development ETL failed: {e}")

        logger.info("Development mode finished. Exiting.")
        return

    # -------- PRODUCTION MODE --------
    processed = 0
    while not _shutdown:
        job = None
        try:
            job = get_pending_job()
        except Exception as e:
            logger.exception(f"Failed to claim job: {e}")
            time.sleep(POLL_SECONDS)
            continue

        if not job:
            time.sleep(POLL_SECONDS)
            continue

        job_id = job["id"]
        t0 = time.time()

        try:
            meta = run_etl_for_job(job)
            meta["duration_seconds"] = round(time.time() - t0, 3)
            mark_success(job_id, meta)
            logger.info(f"[{job_id}] Success in {meta['duration_seconds']}s")
        except Exception as e:
            meta = parse_meta(job)
            meta["duration_seconds"] = round(time.time() - t0, 3)
            meta["failed_utc"] = utc_now_iso()
            err = f"{type(e).__name__}: {e}"

            try:
                mark_failed(job_id, err, meta)
            except Exception:
                logger.exception(f"[{job_id}] Failed but could not update EtlJob")

            logger.exception(f"[{job_id}] Failed: {err}")

        processed += 1
        if MAX_JOBS > 0 and processed >= MAX_JOBS:
            logger.info(f"Processed MAX_JOBS={MAX_JOBS}. Exiting.")
            break

    logger.info("ETL worker exiting")


if __name__ == "__main__":
    main_loop()
