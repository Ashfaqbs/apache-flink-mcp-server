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
    

@mcp.tool()
async def list_taskmanagers() -> str:
    """List all registered TaskManagers in the Flink cluster."""
    url = f"{FLINK_URL}/taskmanagers"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            tms = response.json().get("taskmanagers", [])

        if not tms:
            return "No TaskManagers registered."

        result = ["TaskManagers:"]
        for tm in tms:
            result.append(
                f"- ID: {tm.get('id')}\n"
                f"  Slots: {tm.get('slotsNumber')} | Free: {tm.get('freeSlots')}\n"
                f"  CPU Cores: {tm.get('hardware').get('cpuCores') if tm.get('hardware') else 'n/a'}"
            )
        return "\n".join(result)
    except Exception as e:
        logger.error(f"Failed to list TaskManagers: {e}")
        return f"Error listing TaskManagers: {str(e)}"
    

@mcp.tool()
async def get_job_exceptions(job_id: str) -> str:
    """Fetch exceptions that occurred in the specified job."""
    url = f"{FLINK_URL}/jobs/{job_id}/exceptions"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            exceptions = response.json().get("allExceptions", [])

        if not exceptions:
            return "No exceptions found for this job."

        result = ["Job Exceptions:"]
        for e in exceptions:
            result.append(f"- {e.get('exception')} at {e.get('timestamp')}")
        return "\n".join(result)
    except Exception as e:
        logger.error(f"Failed to get job exceptions: {e}")
        return f"Error: {str(e)}"
    
    
@mcp.tool()
async def list_jar_files() -> str:
    """List all uploaded JARs in the Flink cluster."""
    url = f"{FLINK_URL}/jars"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            jars = response.json().get("files", [])

        if not jars:
            return "No JARs uploaded."

        return "\n".join([f"{jar['id']} - {jar['name']}" for jar in jars])
    except Exception as e:
        logger.error(f"Failed to list jars: {e}")
        return f"Error: {str(e)}"
    
    

@mcp.tool()
async def get_job_metrics(job_id: str) -> str:
    """Fetch selected useful metrics for a running Flink job."""
    base_url = f"{FLINK_URL}/jobs/{job_id}/metrics"
    try:
        async with httpx.AsyncClient() as client:
           
            r = await client.get(base_url)
            available_metrics = r.json()
            metric_ids = [m["id"] for m in available_metrics]

            
            common = [
                "uptime",
                "runningTime",
                "numberOfCompletedCheckpoints",
                "numberOfFailedCheckpoints",
                "numRestarts",
                "lastCheckpointDuration"
            ]
            selected = [m for m in common if m in metric_ids]
            if not selected:
                return "No common metrics available for this job."

            
            get_param = ",".join(selected)
            r = await client.get(f"{base_url}?get={get_param}")
            values = r.json()

        
        return "\n".join([
            f"{m['id']}: {m.get('value', '<no value>')}"
            for m in values
        ])
    except Exception as e:
        logger.error(f"Failed to get job metrics: {e}")
        return f"Error: {str(e)}"
