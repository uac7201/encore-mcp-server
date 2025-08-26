# airflow_mcp_server.py
from __future__ import annotations

import os
import re
import logging
from typing import Any, Optional, Dict, Union
import requests
import anyio
from mcp.server.fastmcp import FastMCP
from datetime import datetime, timezone as dt_tz
from validator import validate_conf, POSTGRES_JOB_SCHEMA

# ---------- logging (MCP servers must not print to stdout) ----------
# Logging goes to stderr, safe for MCP stdio.
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("airflow-mcp")

# ---------- Airflow config via env ----------
def _normalize_base_url(raw: str) -> str:
    """Ensure the base URL has a scheme and no trailing slash."""
    if not raw:
        raise RuntimeError("AIRFLOW_BASE_URL is required")
    url = raw.strip().rstrip("/")
    if not re.match(r"^https?://", url, flags=re.I):
        url = "http://" + url
    return url

AF_URL  = _normalize_base_url(os.environ["AIRFLOW_BASE_URL"])  
AF_USER = os.getenv("AIRFLOW_USERNAME")                          
AF_PASS = os.getenv("AIRFLOW_PASSWORD")

# ---------- HTTP defaults ----------
JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
REQ_TIMEOUT_DEFAULT = 30

def _error_detail(r: requests.Response) -> str:
    try:
        return r.json()  
    except Exception:
        return r.text

# ---------- Auth helper (Airflow 3 public API) ----------
def _jwt() -> str:
    """Obtain a short-lived JWT via /auth/token."""
    r = requests.post(
        f"{AF_URL}/auth/token",
        headers=JSON_HEADERS,
        json={"username": AF_USER, "password": AF_PASS},
        timeout=REQ_TIMEOUT_DEFAULT,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {_error_detail(r)}", response=r)
    body = r.json()
    token = body.get("access_token")
    if not token:
        raise RuntimeError(f"Auth response missing access_token: {body}")
    return token

def _iso_utc_now() -> str:
    # e.g. "2025-08-26T20:31:00Z"
    return datetime.now(dt_tz.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def airflow_trigger(
    dag_id: str,
    conf: Dict[str, Any],
    dag_run_id: Optional[str] = None,
    logical_date: Optional[Union[str, None]] = None,   # required-by-schema, nullable
) -> Dict[str, str]:
    """
    Create a DAG run. logical_date is required-by-schema on Airflow 3 /api/v2.
    Pass None to create a "no data interval" run, or pass an RFC3339 string.
    """
    token = _jwt()
    payload: Dict[str, Any] = {
        "conf": conf,
        "logical_date": logical_date,   # include the key even when None -> JSON null
    }
    if dag_run_id:
        payload["dag_run_id"] = dag_run_id

    r = requests.post(
        f"{AF_URL}/api/v2/dags/{dag_id}/dagRuns",
        headers={**JSON_HEADERS, "Authorization": f"Bearer {token}"},
        json=payload,
        timeout=REQ_TIMEOUT_DEFAULT,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {_error_detail(r)}", response=r)
    body = r.json()
    return {"dag_run_id": body.get("dag_run_id")}


def airflow_status(dag_id: str, dag_run_id: str) -> Dict[str, str]:
    """Get status for a DAG run."""
    token = _jwt()
    r = requests.get(
        f"{AF_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
        headers={**JSON_HEADERS, "Authorization": f"Bearer {token}"},
        timeout=REQ_TIMEOUT_DEFAULT,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {_error_detail(r)}", response=r)
    b = r.json()
    ui = f"{AF_URL}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    return {"state": b.get("state", "unknown"), "external_url": ui}

def airflow_get_dag(dag_id: str) -> Dict[str, Any]:
    token = _jwt()
    r = requests.get(
        f"{AF_URL}/api/v2/dags/{dag_id}",
        headers={**JSON_HEADERS, "Authorization": f"Bearer {token}"},
        timeout=REQ_TIMEOUT_DEFAULT,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {_error_detail(r)}", response=r)
    return r.json()

def airflow_unpause(dag_id: str) -> Dict[str, Any]:
    # PATCH with update_mask works on /api/v2 as well. :contentReference[oaicite:2]{index=2}
    token = _jwt()
    r = requests.patch(
        f"{AF_URL}/api/v2/dags/{dag_id}?update_mask=is_paused",
        headers={**JSON_HEADERS, "Authorization": f"Bearer {token}"},
        json={"is_paused": False},
        timeout=REQ_TIMEOUT_DEFAULT,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {_error_detail(r)}", response=r)
    return r.json()

# ---------- MCP server ----------
mcp = FastMCP("encore-airflow")

SPARK_DAG_ID = "simple_spark_job"
POSTGRES_DAG_ID = "extract_data_from_postgres_job"

@mcp.tool(name="deploy.spark", description="Deploy a simple Spark application that calculates PI.")
async def deploy_simple_spark(conf: Dict[str, Any], dag_run_id: Optional[str] = None) -> Dict[str, str]:
    return await anyio.to_thread.run_sync(airflow_trigger, SPARK_DAG_ID, conf, dag_run_id)

@mcp.tool(name="deploy.postgres-to-polaris", description="Extract data from Postgres and write to Polaris using Spark.")
async def deploy_postgres_data_extractor(conf: Dict[str, Any], dag_run_id: Optional[str] = None) -> Dict[str, str]:
    validate_conf(conf, POSTGRES_JOB_SCHEMA)
    return await anyio.to_thread.run_sync(airflow_trigger, POSTGRES_DAG_ID, conf, dag_run_id)

@mcp.tool(name="airflow.status")
async def mcp_airflow_status(dag_id: str, dag_run_id: str) -> Dict[str, str]:
    return await anyio.to_thread.run_sync(airflow_status, dag_id, dag_run_id)

if __name__ == "__main__":
    airflow_trigger(
    "extract_data_from_postgres_job",
    {
        "JDBC_URL": "jdbc:postgresql://postgres.default.svc:5432/shop",
        "JDBC_USERNAME": "ingest_user",
        "JDBC_PASSWORD": "supersecret",
        "JDBC_DRIVER": "org.postgresql.Driver",
        "JDBC_TABLE": "public.orders",
        "JDBC_QUERY": "SELECT * FROM public.orders",
        "APP_NAME": "postgres-extractor-job"
    }
    )

    #mcp.run(transport="stdio")
