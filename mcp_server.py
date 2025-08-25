# airflow_mcp_server.py
from __future__ import annotations

import os
import logging
from typing import Any, Optional, Dict

import requests
import anyio
from mcp.server.fastmcp import FastMCP

# ---------- logging (MCP servers must not print to stdout) ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("airflow-mcp")

# ---------- Airflow config via env ----------
AF_URL  = os.environ["AIRFLOW_BASE_URL"]         # e.g. https://airflow.example.com
AF_USER = os.getenv("AIRFLOW_USERNAME")          # for Simple/FAB auth manager
AF_PASS = os.getenv("AIRFLOW_PASSWORD")

# ---------- Your original helpers (sync, using requests) ----------
def _jwt() -> str:
    """
    Airflow 3 public API auth (auth-manager exposes /auth/token)
    Returns a short-lived JWT access token.
    """
    r = requests.post(
        f"{AF_URL}/auth/token",
        json={"username": AF_USER, "password": AF_PASS},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def airflow_trigger(dag_id: str, conf: Dict[str, Any], dag_run_id: Optional[str] = None) -> Dict[str, str]:
    token = _jwt()
    payload: Dict[str, Any] = {"conf": conf}
    if dag_run_id:
        payload["dag_run_id"] = dag_run_id
    r = requests.post(
        f"{AF_URL}/api/v2/dags/{dag_id}/dagRuns",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    return {"dag_run_id": body.get("dag_run_id", dag_run_id or body.get("dag_run_id"))}

def airflow_status(dag_id: str, dag_run_id: str) -> Dict[str, str]:
    token = _jwt()
    r = requests.get(
        f"{AF_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    r.raise_for_status()
    b = r.json()
    ui = f"{AF_URL}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    return {"state": b.get("state", "unknown"), "external_url": ui}

# ---------- MCP server ----------
mcp = FastMCP("airflow")

@mcp.tool(name="airflow.trigger")
async def mcp_airflow_trigger(
    dag_id: str,
    conf: Dict[str, Any],
    dag_run_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    Trigger an Airflow DAG run via /api/v2/dags/{dag_id}/dagRuns.

    Args:
      dag_id: Airflow DAG ID (e.g., "ingest_runner").
      conf:  Arbitrary JSON passed to the DAG run (your IngestSpec, etc.).
      dag_run_id: Optional custom run id for idempotency.

    Returns:
      {"dag_run_id": "<the run id Airflow created/accepted>"}.
    """
    # Run the sync helper in a worker thread to avoid blocking the MCP event loop
    return await anyio.to_thread.run_sync(airflow_trigger, dag_id, conf, dag_run_id)

@mcp.tool(name="airflow.status")
async def mcp_airflow_status(
    dag_id: str,
    dag_run_id: str,
) -> Dict[str, str]:
    """
    Get the status for a specific DAG run.

    Args:
      dag_id: Airflow DAG ID.
      dag_run_id: DAG run identifier returned by airflow.trigger.

    Returns:
      {"state": "queued|running|success|failed|...", "external_url": "<Airflow UI link>"}.
    """
    return await anyio.to_thread.run_sync(airflow_status, dag_id, dag_run_id)

if __name__ == "__main__":
    # Start the MCP server over stdio (recommended for desktop clients)
    # See official quickstart for details on running & registering servers. 
    # https://modelcontextprotocol.io/quickstart/server
    mcp.run(transport="stdio")
