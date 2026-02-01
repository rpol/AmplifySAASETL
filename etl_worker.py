# etl_worker.py
"""
ETL worker for NAV 2017 -> Neon(Postgres) using EtlJob queue (production) or dev-run (development).

Implements:
- EXEC_MODE=production|development
- Production: claim EtlJob rows and update status
- Development: run a local synthetic job, no EtlJob reads/writes
- Job dispatch: currently supports IMPORT_COA
- IMPORT_COA loads:
    ExternalChartOfAccounts (header)
    ExternalAccount (tree attributes: parentExternalId, orderIndex, isPosting, accountType)
"""

import os
import time
import json
import signal
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
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

PG_DATABASE_URL = os.getenv("PG_DATABASE_URL")
SRC_DATABASE_URL = os.getenv("SRC_DATABASE_URL")

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
    # Ensure brackets; company_name may contain spaces
    return f"[{company_name}${base_table}]"

def try_select_columns(engine: Engine, table_full: str, preferred_cols: List[str]) -> List[str]:
    """
    Probe which columns exist. Returns the subset that exists.
    """
    # Use INFORMATION_SCHEMA.COLUMNS (works for SQL Server)
    # table_full includes brackets; strip them for query
    tf = table_full.strip("[]")
    # tf is like 'HVG$G_L Account'
    # In SQL Server, schema is usually dbo. We'll query by table name only.
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

    # -----------------------------
    # 1) Account Category (NAV)
    # -----------------------------
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

    # -----------------------------------------
    # 2) Account number prefix fallback
    # -----------------------------------------
    code = str(row.get("code", "") or "").strip()

    # Extract first digit if numeric
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
        return "COGS"          # or EXPENSE depending on your accounting model
    if first_digit == "6":
        return "EXPENSE"
    if first_digit == "7":
        return "OTHER_INCOME"
    if first_digit == "8":
        return "OTHER_EXPENSE"

    # -----------------------------------------
    # 3) Final safe default
    # -----------------------------------------
    return "EXPENSE"


def nav_is_posting(row: pd.Series) -> bool:
    """
    NAV has "Account Type" with values like Posting/Heading/Total/Begin-Total/End-Total.
    If not available, assume posting.
    """
    acct_type = str(row.get("Account Type", "") or "").strip().upper()
    if acct_type:
        return acct_type == "POSTING"
    return True


def compute_parent_by_indent(df: pd.DataFrame, indent_col: str) -> List[Optional[str]]:
    """
    Given sorted accounts and an indent level per row, compute parentExternalId (as ExternalAccount.id string).
    Uses a stack of last-seen node per indentation.
    """
    parents: List[Optional[str]] = []
    stack: List[Tuple[int, str]] = []  # (indent, external_account_id)

    for _, r in df.iterrows():
        indent = int(r[indent_col]) if pd.notna(r[indent_col]) else 0
        cur_id = r["id"]

        # Pop to find nearest smaller indent as parent
        while stack and stack[-1][0] >= indent:
            stack.pop()

        parent_id = stack[-1][1] if stack else None
        parents.append(parent_id)

        # Push current as latest for this indent
        stack.append((indent, cur_id))

    return parents


# -------------------------
# IMPORT_COA
# -------------------------
def etl_import_coa(job: Dict[str, Any], meta: Dict[str, Any], heartbeat_cb) -> Dict[str, Any]:
    """
    Imports a NAV chart of accounts into:
      - ExternalChartOfAccounts
      - ExternalAccount

    Replace strategy: delete all ExternalAccount for the externalCoaId, then insert fresh snapshot.
    """
    external_coa_id = meta.get("externalCoaId")
    organization_id = meta.get("organizationId")
    nav_company = meta.get("navCompanyName")
    external_coa_name = meta.get("externalCoaName") or f"NAV CoA ({nav_company})"
    source_system = meta.get("sourceSystem") or "NAV2017"

    if not external_coa_id or not organization_id or not nav_company:
        raise RuntimeError("IMPORT_COA requires meta: organizationId, externalCoaId, navCompanyName")

    table = nav_table(nav_company, "G_L Account")

    # Preferred columns. We'll probe existence and build a query dynamically.
    preferred = [
        "No_",
        "Name",
        "Account Type",
        "IndentationNA",  # Not configured properly in some NAV versions, changed deliberately to avoid using this method
        "Income_Balance",
        "Account Category",
        "Blocked",
    ]
    existing_cols = try_select_columns(src_engine, table, preferred)

    # We need at least No_ and Name
    if "No_" not in existing_cols or "Name" not in existing_cols:
        raise RuntimeError(f"Required columns not found in {table}. Found: {existing_cols}")

    # Build SELECT
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

    # Normalize base fields
    df.rename(columns={"No_": "code", "Name": "name"}, inplace=True)
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()

    # Filter blocked? Optional. MVP: keep all.
    if "Blocked" in df.columns:
        df = df[df["Blocked"] == 0]

    # Determine indentation:
    # Prefer NAV "Indentation" column if exists; else fallback to len(code) like your SP.
    if "Indentation" in df.columns:
        df["indent"] = pd.to_numeric(df["Indentation"], errors="coerce").fillna(0).astype(int)
    else:
        df["indent"] = df["code"].str.len().fillna(0).astype(int)

    # Sort order: NAV often expects by "No_" (code) or by Indentation display order.
    # We'll do stable: code ascending.
    df.sort_values(by=["code"], inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)

    # Build ExternalAccount.id as stable string
    # NOTE: this must match your Prisma ExternalAccount.id definition (String @id).
    df["id"] = df["code"].apply(lambda c: f"{external_coa_id}:{c}")

    # accountType mapping and posting/header
    df["accountType"] = df.apply(map_nav_account_type, axis=1)
    df["isPosting"] = df.apply(nav_is_posting, axis=1)

    # orderIndex: spaced increments to allow inserts without full reorder later
    df["orderIndex"] = (df.index + 1) * 10

    # parentExternalId from indent stack
    df["parentExternalId"] = compute_parent_by_indent(df, "indent")

    # Prepare load frame for ExternalAccount
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

    # Load into Postgres
    with pg_engine.begin() as conn:
        # Upsert ExternalChartOfAccounts
        conn.execute(sa.text("""
            INSERT INTO "ExternalChartOfAccounts" ("id","organizationId","name","sourceSystem","createdAt","updatedAt")
            VALUES (:id,:org,:name,:src, now(), now())
            ON CONFLICT ("id") DO UPDATE
              SET "organizationId" = EXCLUDED."organizationId",
                  "name" = EXCLUDED."name",
                  "sourceSystem" = EXCLUDED."sourceSystem",
                  "updatedAt" = now();
        """), {"id": external_coa_id, "org": organization_id, "name": external_coa_name, "src": source_system})

        # Replace strategy for accounts snapshot
        conn.execute(sa.text("""DELETE FROM "ExternalAccount" WHERE "externalCoaId" = :id"""), {"id": external_coa_id})

        # Bulk insert with quoted table/columns (safe with Prisma mixed-case names)
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
# Dispatch
# -------------------------
def run_etl_for_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("id", "DEV_JOB")
    meta = parse_meta(job)

    # Job type can come from EtlJob.type or meta.type
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
        # Run once with a synthetic job payload (no EtlJob reads/writes)
        fake_job = {
            "id": "DEV_JOB",
            "type": "IMPORT_COA",
            "meta": {
            "type": "IMPORT_COA",
            "organizationId": "Yg5avCX14YIe8TRTu8RCMrYOWCaCv3Co",
            "externalCoaId": "nav.panorama.coa", 
            "externalCoaName": "PanoramaJets CoA",
            "navCompanyName": "HELIDOSAVG",
            "sourceSystem": "NAV2017"
            },
        }

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
