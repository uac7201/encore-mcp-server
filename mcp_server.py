# airflow_mcp_server.py
from __future__ import annotations

import os
import re
import logging
from typing import Any, Optional, Dict, Union
import traceback

import anyio
import requests

from fastmcp import FastMCP

from datetime import datetime, timezone as dt_tz

# If you have a local schema/validator, keep these imports.
# Otherwise you can comment them out.
from validator import validate_conf, POSTGRES_JOB_SCHEMA  # type: ignore

# ---------- logging (MCP servers must not print to stdout) ----------
# Logs go to stderr; Claude reads stdio JSON-RPC from stdout.
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

if not AF_USER or not AF_PASS:
    raise RuntimeError("AIRFLOW_USERNAME and AIRFLOW_PASSWORD are required env vars")

# ---------- HTTP defaults ----------
JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
REQ_TIMEOUT_DEFAULT = 30

def _error_detail(r: requests.Response) -> str:
    try:
        return r.json()  # type: ignore[return-value]
    except Exception:
        return r.text

def _format_http_error(e: requests.HTTPError) -> Dict[str, Any]:
    resp = e.response
    return {
        "error": "HTTPError",
        "status": getattr(resp, "status_code", None),
        "reason": getattr(resp, "reason", None),
        "url": getattr(resp, "url", None),
        "body": (_error_detail(resp) if resp is not None else str(e)),
    }

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

# ---------- Airflow helpers ----------
def airflow_trigger(
    dag_id: str,
    conf: Dict[str, Any],
    dag_run_id: Optional[str] = None,
    logical_date: Optional[Union[str, None]] = None,
) -> Dict[str, str]:
    """
    Create a DAG run via Airflow 3 /api/v2.
    Airflow 3 requires logical_date; if not provided, we generate a now-UTC value.
    """
    token = _jwt()
    payload: Dict[str, Any] = {
        "conf": conf,
        "logical_date": logical_date if isinstance(logical_date, str) else _iso_utc_now(),
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
    """Unpause the DAG."""
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
# Important: use only letters/digits in the server name for Claude Desktop.
mcp = FastMCP(name="encore_airflow")

SPARK_DAG_ID = "simple_spark_job"
POSTGRES_DAG_ID = "extract_data_from_postgres_job"

# Small health check to debug connectivity/auth quickly
@mcp.tool(name="airflow_ping", description="Check connectivity/auth to Airflow and count DAGs.")
async def airflow_ping() -> Dict[str, Any]:
    try:
        token = _jwt()
        r = requests.get(
            f"{AF_URL}/api/v2/dags",
            headers={**JSON_HEADERS, "Authorization": f"Bearer {token}"},
            timeout=REQ_TIMEOUT_DEFAULT,
        )
        ok = r.ok
        data = {}
        try:
            data = r.json()
        except Exception:
            data = {"text": r.text[:2000]}
        return {"ok": ok, "status": r.status_code, "dags_count": len(data.get("dags", []))}
    except Exception as e:
        log.exception("airflow_ping failed")
        return {"error": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}

@mcp.tool(name="deploy_spark", description="Deploy Spark Pi.")
async def deploy_simple_spark(conf: Dict[str, Any], dag_run_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        out = await anyio.to_thread.run_sync(airflow_trigger, SPARK_DAG_ID, conf, dag_run_id)
        return out  # {"dag_run_id": "..."}
    except requests.HTTPError as e:
        log.exception("deploy_spark failed")
        return _format_http_error(e)
    except Exception as e:
        log.exception("deploy_spark failed (generic)")
        return {"error": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}

@mcp.tool(name="deploy_postgres_to_polaris", description="Extract data from Postgres and write to Polaris using Spark.")
async def deploy_postgres_data_extractor(conf: Dict[str, Any], dag_run_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        # Validate input (optional; comment out if you don't have a validator)
        validate_conf(conf, POSTGRES_JOB_SCHEMA)
        out = await anyio.to_thread.run_sync(airflow_trigger, POSTGRES_DAG_ID, conf, dag_run_id)
        return out
    except requests.HTTPError as e:
        log.exception("deploy_postgres_to_polaris failed")
        return _format_http_error(e)
    except Exception as e:
        log.exception("deploy_postgres_to_polaris failed (generic)")
        return {"error": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}

@mcp.tool(name="airflow_status", description="Get status for a specific DAG run.")
async def mcp_airflow_status(dag_id: str, dag_run_id: str) -> Dict[str, Any]:
    try:
        return await anyio.to_thread.run_sync(airflow_status, dag_id, dag_run_id)
    except requests.HTTPError as e:
        log.exception("airflow_status failed")
        return _format_http_error(e)
    except Exception as e:
        log.exception("airflow_status failed (generic)")
        return {"error": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}

@mcp.tool(name="airflow_unpause", description="Unpause a DAG by id.")
async def mcp_airflow_unpause(dag_id: str) -> Dict[str, Any]:
    try:
        return await anyio.to_thread.run_sync(airflow_unpause, dag_id)
    except requests.HTTPError as e:
        log.exception("airflow_unpause failed")
        return _format_http_error(e)
    except Exception as e:
        log.exception("airflow_unpause failed (generic)")
        return {"error": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}

@mcp.tool(name="airflow_maik", description="Deploy a Maik in Airflow")
async def airflow_maik(dag_id: str) -> Dict[str, Any]:
    print("Deploying a Maik in Airflow!")


if __name__ == "__main__":
    mcp.run()