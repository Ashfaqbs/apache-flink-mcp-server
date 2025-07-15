import logging
import sys
import os
import argparse
import httpx
from fastmcp import FastMCP


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("flink-mcp-server")


mcp = FastMCP("Apache Flink MCP Server")

parser = argparse.ArgumentParser(description="Flink MCP Server")
parser.add_argument(
    "--url",
    dest="flink_url",
    default=os.getenv("FLINK_API_BASE_URL", "http://localhost:8081"),
    help="Base URL of Flink REST API"
)


args, _ = parser.parse_known_args()
FLINK_URL = args.flink_url

logger.info(f"Connecting to Flink REST at: {FLINK_URL}")



@mcp.tool()
async def get_cluster_info() -> str:
    """Fetch an overview of the Flink cluster: jobs, slots, taskmanagers."""
    url = f"{FLINK_URL}/overview"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()

        return (
            f"Flink Cluster Info:\n"
            f"- TaskManagers: {data.get('taskmanagers')}\n"
            f"- Slots Total: {data.get('slots-total')}\n"
            f"- Slots Available: {data.get('slots-available')}\n"
            f"- Jobs Running: {data.get('jobs-running')}\n"
            f"- Jobs Finished: {data.get('jobs-finished')}\n"
            f"- Jobs Cancelled: {data.get('jobs-cancelled')}\n"
            f"- Jobs Failed: {data.get('jobs-failed')}"
        )
    except Exception as e:
        logger.error(f"Failed to fetch cluster info: {e}")
        return f"Error fetching cluster info: {str(e)}"
    
    



@mcp.tool()
async def list_jobs() -> str:
    """List all current and recent Flink jobs with their status."""
    url = f"{FLINK_URL}/jobs/overview"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            jobs = response.json().get("jobs", [])

        if not jobs:
            return "No jobs found."

        result = ["Flink Jobs Overview:"]
        for job in jobs:
            result.append(
                f"- ID: {job.get('jid')} | Name: {job.get('name')} | State: {job.get('state')}"
            )
        return "\n".join(result)
    except Exception as e:
        logger.error(f"Failed to fetch job list: {e}")
        return f"Error fetching jobs: {str(e)}"


@mcp.tool()
async def get_job_details(job_id: str) -> str:
    """Get details of a specific Flink job by job ID."""
    url = f"{FLINK_URL}/jobs/{job_id}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            job = response.json()

        return (
            f"Job ID: {job.get('jid')}\n"
            f"Name: {job.get('name')}\n"
            f"State: {job.get('state')}\n"
            f"Start Time: {job.get('start-time')}\n"
            f"Duration (ms): {job.get('duration')}\n"
            f"Max Parallelism: {job.get('maxParallelism')}"
        )
    except Exception as e:
        logger.error(f"Failed to get job details: {e}")
        return f"Error getting job details: {str(e)}"