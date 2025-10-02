import logging
from typing import Optional, List, Dict, Union
import json

import sys
import os
import argparse
from fastmcp import FastMCP
import httpx
from typing import Dict, List, Any

# mail imports
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("flink-mcp-server")


mcp = FastMCP("Apache Flink MCP Server")

# Global state for Flink connection
FLINK_CONNECTION = {
    "url": None,
    "initialized": False
}


def check_initialized():
    """Helper to verify Flink connection is initialized."""
    if not FLINK_CONNECTION["initialized"]:
        return False, "Flink connection not initialized. Please call initialize_flink_connection first."
    return True, None

## ** Working 
@mcp.tool() 
async def initialize_flink_connection(flink_url: str) -> str:
    """
    Initialize connection to Apache Flink REST API.
    Must be called before using any other Flink tools.
    
    Args:
        flink_url: Base URL of Flink REST API (e.g., http://localhost:8081)
    """
    # Remove trailing slash if present
    flink_url = flink_url.rstrip('/')
    
    # Test the connection
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{flink_url}/overview", timeout=5.0)
            response.raise_for_status()
            
        FLINK_CONNECTION["url"] = flink_url
        FLINK_CONNECTION["initialized"] = True
        logger.info(f"Successfully connected to Flink at: {flink_url}")
        return f"✓ Successfully connected to Flink cluster at {flink_url}"
    except Exception as e:
        logger.error(f"Failed to connect to Flink: {e}")
        return f"✗ Failed to connect to Flink at {flink_url}: {str(e)}"

## ** Working 
@mcp.tool()
async def get_connection_status() -> str:
    """Check if Flink connection is initialized and get current URL."""
    if FLINK_CONNECTION["initialized"]:
        return f"✓ Connected to: {FLINK_CONNECTION['url']}"
    else:
        return "✗ Not connected. Use initialize_flink_connection to connect."

## ** Working
@mcp.tool()
async def get_cluster_info() -> str:
    """Fetch an overview of the Flink cluster: jobs, slots, taskmanagers."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/overview"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
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

## ** Working
@mcp.tool()
async def list_jobs() -> str:
    """List all current and recent Flink jobs with their status."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/jobs/overview"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
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

## ** Working
@mcp.tool()
async def get_job_details(job_id: str) -> str:
    """Get comprehensive details of a specific Flink job by job ID including configuration, 
    vertices, metrics, and execution plan."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    # Fetch both job details and config in parallel
    details_url = f"{FLINK_CONNECTION['url']}/jobs/{job_id}"
    config_url = f"{FLINK_CONNECTION['url']}/jobs/{job_id}/config"
    
    job_data = None
    config_data = None
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Fetch job details
            try:
                details_response = await client.get(details_url)
                details_response.raise_for_status()
                job_data = details_response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"❌ Job not found: {job_id}\nThis job may have been removed from the cluster history."
                logger.error(f"HTTP error fetching job details: {e}")
                return f"❌ Error fetching job details (HTTP {e.response.status_code}): {str(e)}"
            except Exception as e:
                logger.error(f"Failed to fetch job details: {e}")
                return f"❌ Error fetching job details: {str(e)}"
            
            # Fetch job config
            try:
                config_response = await client.get(config_url)
                config_response.raise_for_status()
                config_data = config_response.json()
            except Exception as e:
                logger.warning(f"Failed to fetch job config (non-critical): {e}")
                # Config fetch failure is non-critical, continue with job data
                
    except Exception as e:
        logger.error(f"Failed to create HTTP client: {e}")
        return f"❌ Network error: {str(e)}"
    
    if not job_data:
        return f"❌ No data retrieved for job {job_id}"
    
    # Build comprehensive output
    output_lines = []
    
    # === BASIC INFO ===
    output_lines.append("=" * 60)
    output_lines.append("JOB OVERVIEW")
    output_lines.append("=" * 60)
    output_lines.append(f"Job ID: {job_data.get('jid', 'N/A')}")
    output_lines.append(f"Name: {job_data.get('name', 'N/A')}")
    output_lines.append(f"State: {job_data.get('state', 'UNKNOWN')}")
    output_lines.append(f"Type: {job_data.get('plan', {}).get('type', 'N/A')}")
    
    # Timing information
    start_time = job_data.get('start-time', 0)
    end_time = job_data.get('end-time', -1)
    duration = job_data.get('duration', 0)
    
    output_lines.append(f"\nStart Time: {start_time} ({_format_timestamp(start_time)})")
    if end_time > 0:
        output_lines.append(f"End Time: {end_time} ({_format_timestamp(end_time)})")
    output_lines.append(f"Duration: {_format_duration(duration)}")
    
    # Timestamps
    timestamps = job_data.get('timestamps', {})
    if timestamps:
        output_lines.append("\nState Transitions:")
        for state, ts in timestamps.items():
            if ts > 0:
                output_lines.append(f"  - {state}: {_format_timestamp(ts)}")
    
    # === CONFIGURATION ===
    output_lines.append("\n" + "=" * 60)
    output_lines.append("CONFIGURATION")
    output_lines.append("=" * 60)
    
    if config_data:
        exec_config = config_data.get('execution-config', {})
        output_lines.append(f"Execution Mode: {exec_config.get('execution-mode', 'N/A')}")
        output_lines.append(f"Restart Strategy: {exec_config.get('restart-strategy', 'N/A')}")
        output_lines.append(f"Job Parallelism: {exec_config.get('job-parallelism', 'N/A')}")
        output_lines.append(f"Object Reuse: {exec_config.get('object-reuse-mode', False)}")
        
        user_config = exec_config.get('user-config', {})
        if user_config:
            output_lines.append("\nUser Configuration:")
            for key, value in user_config.items():
                output_lines.append(f"  - {key}: {value}")
    else:
        output_lines.append("⚠️  Configuration data unavailable")
    
    output_lines.append(f"\nMax Parallelism: {job_data.get('maxParallelism', 'Not set')}")
    output_lines.append(f"Stoppable: {job_data.get('isStoppable', False)}")
    
    # === VERTICES (Operators) ===
    vertices = job_data.get('vertices', [])
    if vertices:
        output_lines.append("\n" + "=" * 60)
        output_lines.append(f"VERTICES ({len(vertices)} operators)")
        output_lines.append("=" * 60)
        
        for idx, vertex in enumerate(vertices, 1):
            output_lines.append(f"\n[{idx}] {vertex.get('name', 'Unknown')}")
            output_lines.append(f"    ID: {vertex.get('id', 'N/A')}")
            output_lines.append(f"    Status: {vertex.get('status', 'N/A')}")
            output_lines.append(f"    Parallelism: {vertex.get('parallelism', 'N/A')} (max: {vertex.get('maxParallelism', 'N/A')})")
            output_lines.append(f"    Duration: {_format_duration(vertex.get('duration', 0))}")
            
            # Task status breakdown
            tasks = vertex.get('tasks', {})
            if tasks:
                task_summary = ", ".join([f"{state}: {count}" for state, count in tasks.items() if count > 0])
                if task_summary:
                    output_lines.append(f"    Tasks: {task_summary}")
            
            # Metrics
            metrics = vertex.get('metrics', {})
            if metrics:
                output_lines.append("    Metrics:")
                
                # I/O metrics
                read_records = metrics.get('read-records', 0)
                write_records = metrics.get('write-records', 0)
                read_bytes = metrics.get('read-bytes', 0)
                write_bytes = metrics.get('write-bytes', 0)
                
                if read_records > 0 or read_bytes > 0:
                    output_lines.append(f"      Read: {read_records:,} records, {_format_bytes(read_bytes)}")
                if write_records > 0 or write_bytes > 0:
                    output_lines.append(f"      Write: {write_records:,} records, {_format_bytes(write_bytes)}")
                
                # Performance metrics
                backpressured = metrics.get('accumulated-backpressured-time', 0)
                idle_time = metrics.get('accumulated-idle-time', 0)
                busy_time = metrics.get('accumulated-busy-time', 'NaN')
                
                if backpressured > 0:
                    output_lines.append(f"      ⚠️  Backpressured Time: {_format_duration(backpressured)}")
                if idle_time > 0:
                    output_lines.append(f"      Idle Time: {_format_duration(idle_time)}")
                if busy_time != 'NaN' and str(busy_time) != 'NaN':
                    output_lines.append(f"      Busy Time: {_format_duration(busy_time)}")
                
                # Throughput calculation
                vertex_duration = vertex.get('duration', 0)
                if vertex_duration > 0 and write_records > 0:
                    throughput = (write_records / vertex_duration) * 1000  # records per second
                    output_lines.append(f"      Throughput: {throughput:,.2f} records/sec")
    
    # === STATUS SUMMARY ===
    status_counts = job_data.get('status-counts', {})
    if status_counts and any(count > 0 for count in status_counts.values()):
        output_lines.append("\n" + "=" * 60)
        output_lines.append("OVERALL TASK STATUS")
        output_lines.append("=" * 60)
        for status, count in status_counts.items():
            if count > 0:
                output_lines.append(f"{status}: {count}")
    
    # === EXECUTION PLAN ===
    plan = job_data.get('plan', {})
    if plan and plan.get('nodes'):
        output_lines.append("\n" + "=" * 60)
        output_lines.append("EXECUTION PLAN")
        output_lines.append("=" * 60)
        
        nodes = plan.get('nodes', [])
        output_lines.append(f"Total Nodes: {len(nodes)}\n")
        
        for idx, node in enumerate(nodes, 1):
            output_lines.append(f"[Node {idx}] ID: {node.get('id', 'N/A')}")
            output_lines.append(f"  Parallelism: {node.get('parallelism', 'N/A')}")
            
            # Clean up description (remove HTML tags)
            description = node.get('description', '')
            if description:
                clean_desc = description.replace('<br/>', '\n  ').replace('<', '').replace('>', '')
                output_lines.append(f"  Description:\n  {clean_desc}")
            
            # Inputs
            inputs = node.get('inputs', [])
            if inputs:
                output_lines.append("  Inputs:")
                for inp in inputs:
                    output_lines.append(f"    - From Node: {inp.get('id', 'N/A')}")
                    output_lines.append(f"      Strategy: {inp.get('ship_strategy', 'N/A')}")
                    output_lines.append(f"      Exchange: {inp.get('exchange', 'N/A')}")
            output_lines.append("")
    
    # === PERFORMANCE INSIGHTS ===
    output_lines.append("=" * 60)
    output_lines.append("PERFORMANCE INSIGHTS")
    output_lines.append("=" * 60)
    
    insights = []
    
    # Check for backpressure
    for vertex in vertices:
        metrics = vertex.get('metrics', {})
        backpressure = metrics.get('accumulated-backpressured-time', 0)
        if backpressure > 0:
            vertex_name = vertex.get('name', 'Unknown')
            insights.append(f"⚠️  BACKPRESSURE detected in '{vertex_name}': {_format_duration(backpressure)}")
    
    # Check for idle operators
    for vertex in vertices:
        metrics = vertex.get('metrics', {})
        idle = metrics.get('accumulated-idle-time', 0)
        duration = vertex.get('duration', 1)
        if duration > 0 and idle > 0:
            idle_percent = (idle / duration) * 100
            if idle_percent > 50:
                vertex_name = vertex.get('name', 'Unknown')
                insights.append(f"ℹ️  High idle time in '{vertex_name}': {idle_percent:.1f}%")
    
    # Check for failed tasks
    for vertex in vertices:
        tasks = vertex.get('tasks', {})
        failed = tasks.get('FAILED', 0)
        if failed > 0:
            vertex_name = vertex.get('name', 'Unknown')
            insights.append(f"❌ Failed tasks in '{vertex_name}': {failed}")
    
    # Check parallelism vs available slots
    total_parallelism = sum(v.get('parallelism', 0) for v in vertices)
    if total_parallelism > 0:
        insights.append(f"📊 Total parallelism across all operators: {total_parallelism}")
    
    if insights:
        for insight in insights:
            output_lines.append(insight)
    else:
        output_lines.append("✅ No performance issues detected")
    
    output_lines.append("=" * 60)
    
    return "\n".join(output_lines)


# Helper functions
def _format_timestamp(ts: int) -> str:
    """Format Unix timestamp in milliseconds to readable string."""
    if ts <= 0:
        return "N/A"
    try:
        from datetime import datetime
        dt = datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return f"{ts}ms"


def _format_duration(duration_ms) -> str:
    """Format duration in milliseconds to readable string."""
    try:
        duration_ms = float(duration_ms)
        if duration_ms <= 0:
            return "N/A"
        
        seconds = duration_ms / 1000
        
        if seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f}m ({seconds:.0f}s)"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.2f}h ({seconds:.0f}s)"
        else:
            days = seconds / 86400
            return f"{days:.2f}d ({seconds:.0f}s)"
    except (ValueError, TypeError):
        return "N/A"


def _format_bytes(bytes_val) -> str:
    """Format bytes to human-readable string."""
    try:
        bytes_val = float(bytes_val)
        if bytes_val <= 0:
            return "0 B"
        
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        
        while bytes_val >= 1024 and unit_index < len(units) - 1:
            bytes_val /= 1024
            unit_index += 1
        
        return f"{bytes_val:.2f} {units[unit_index]}"
    except (ValueError, TypeError):
        return "N/A"


def _to_num(v):
    try:
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, str) and v.strip() != "":
            # prefer int if possible
            f = float(v)
            i = int(f)
            return i if i == f else f
    except:
        pass
    return None

def _index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {it.get("id"): it for it in items if isinstance(it, dict) and "id" in it}

def _chunk(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf
        
        
        
        
## ** Working
@mcp.tool()
async def list_taskmanagers() -> str:
    """List all registered TaskManagers in the Flink cluster with detailed resource information."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/taskmanagers"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            tms = response.json().get("taskmanagers", [])
        
        if not tms:
            return "⚠️  No TaskManagers registered in the cluster."
        
        result = []
        result.append("=" * 70)
        result.append(f"TASKMANAGERS OVERVIEW ({len(tms)} TaskManager(s))")
        result.append("=" * 70)
        
        # Cluster-wide summary
        total_slots = sum(tm.get('slotsNumber', 0) for tm in tms)
        total_free_slots = sum(tm.get('freeSlots', 0) for tm in tms)
        total_used_slots = total_slots - total_free_slots
        
        result.append(f"\nCluster Capacity:")
        result.append(f"  Total Slots: {total_slots}")
        result.append(f"  Used Slots: {total_used_slots} ({(total_used_slots/total_slots*100) if total_slots > 0 else 0:.1f}%)")
        result.append(f"  Free Slots: {total_free_slots} ({(total_free_slots/total_slots*100) if total_slots > 0 else 0:.1f}%)")
        
        # Detail each TaskManager
        for idx, tm in enumerate(tms, 1):
            result.append("\n" + "=" * 70)
            result.append(f"TaskManager #{idx}")
            result.append("=" * 70)
            
            # Basic Info
            result.append(f"\n📋 BASIC INFO")
            result.append(f"  ID: {tm.get('id', 'N/A')}")
            result.append(f"  Path: {tm.get('path', 'N/A')}")
            result.append(f"  Data Port: {tm.get('dataPort', 'N/A')}")
            result.append(f"  JMX Port: {tm.get('jmxPort', 'Disabled' if tm.get('jmxPort', -1) == -1 else tm.get('jmxPort'))}")
            
            # Heartbeat
            heartbeat = tm.get('timeSinceLastHeartbeat', 0)
            if heartbeat > 0:
                from datetime import datetime
                last_heartbeat = datetime.fromtimestamp(heartbeat / 1000.0)
                result.append(f"  Last Heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Slot Info
            slots_total = tm.get('slotsNumber', 0)
            slots_free = tm.get('freeSlots', 0)
            slots_used = slots_total - slots_free
            
            result.append(f"\n🎰 SLOT ALLOCATION")
            result.append(f"  Total Slots: {slots_total}")
            result.append(f"  Used Slots: {slots_used} ({(slots_used/slots_total*100) if slots_total > 0 else 0:.1f}%)")
            result.append(f"  Free Slots: {slots_free} ({(slots_free/slots_total*100) if slots_total > 0 else 0:.1f}%)")
            
            if slots_free == 0 and slots_total > 0:
                result.append(f"  ⚠️  WARNING: No free slots available!")
            
            # Hardware Resources
            hardware = tm.get('hardware', {})
            if hardware:
                result.append(f"\n💻 HARDWARE")
                result.append(f"  CPU Cores: {hardware.get('cpuCores', 'N/A')}")
                
                phys_mem = hardware.get('physicalMemory', 0)
                free_mem = hardware.get('freeMemory', 0)
                managed_mem = hardware.get('managedMemory', 0)
                
                result.append(f"  Physical Memory: {_format_bytes(phys_mem)}")
                result.append(f"  Free Memory: {_format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
                result.append(f"  Managed Memory: {_format_bytes(managed_mem)}")
                
                if phys_mem > 0 and free_mem > 0:
                    used_mem = phys_mem - free_mem
                    result.append(f"  Used Memory: {_format_bytes(used_mem)} ({(used_mem/phys_mem*100):.1f}%)")
            
            # Total Resources (configured)
            total_res = tm.get('totalResource', {})
            if total_res:
                result.append(f"\n📊 CONFIGURED RESOURCES")
                result.append(f"  CPU Cores (slots): {total_res.get('cpuCores', 'N/A')}")
                result.append(f"  Task Heap Memory: {_format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {_format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {_format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {_format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")
                
                extended = total_res.get('extendedResources', {})
                if extended:
                    result.append(f"  Extended Resources: {extended}")
            
            # Free Resources (available)
            free_res = tm.get('freeResource', {})
            if free_res:
                result.append(f"\n✅ AVAILABLE RESOURCES")
                result.append(f"  CPU Cores: {free_res.get('cpuCores', 0)}")
                result.append(f"  Task Heap Memory: {_format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Task Off-Heap Memory: {_format_bytes(free_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"  Managed Memory: {_format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"  Network Memory: {_format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")
            
            # Memory Configuration Details
            mem_config = tm.get('memoryConfiguration', {})
            if mem_config:
                result.append(f"\n🧠 MEMORY CONFIGURATION")
                result.append(f"  Framework Heap: {_format_bytes(mem_config.get('frameworkHeap', 0))}")
                result.append(f"  Framework Off-Heap: {_format_bytes(mem_config.get('frameworkOffHeap', 0))}")
                result.append(f"  Task Heap: {_format_bytes(mem_config.get('taskHeap', 0))}")
                result.append(f"  Task Off-Heap: {_format_bytes(mem_config.get('taskOffHeap', 0))}")
                result.append(f"  Network Memory: {_format_bytes(mem_config.get('networkMemory', 0))}")
                result.append(f"  Managed Memory: {_format_bytes(mem_config.get('managedMemory', 0))}")
                result.append(f"  JVM Metaspace: {_format_bytes(mem_config.get('jvmMetaspace', 0))}")
                result.append(f"  JVM Overhead: {_format_bytes(mem_config.get('jvmOverhead', 0))}")
                result.append(f"  Total Flink Memory: {_format_bytes(mem_config.get('totalFlinkMemory', 0))}")
                result.append(f"  Total Process Memory: {_format_bytes(mem_config.get('totalProcessMemory', 0))}")
            
            # Resource Utilization Analysis
            result.append(f"\n📈 UTILIZATION ANALYSIS")
            
            # Slot utilization
            if slots_total > 0:
                slot_util = (slots_used / slots_total) * 100
                if slot_util >= 90:
                    result.append(f"  ⚠️  HIGH slot utilization: {slot_util:.1f}%")
                elif slot_util >= 70:
                    result.append(f"  ⚡ MODERATE slot utilization: {slot_util:.1f}%")
                else:
                    result.append(f"  ✅ LOW slot utilization: {slot_util:.1f}%")
            
            # Memory utilization
            if hardware:
                phys_mem = hardware.get('physicalMemory', 0)
                free_mem = hardware.get('freeMemory', 0)
                if phys_mem > 0:
                    mem_util = ((phys_mem - free_mem) / phys_mem) * 100
                    if mem_util >= 90:
                        result.append(f"  ⚠️  HIGH memory utilization: {mem_util:.1f}%")
                    elif mem_util >= 70:
                        result.append(f"  ⚡ MODERATE memory utilization: {mem_util:.1f}%")
                    else:
                        result.append(f"  ✅ LOW memory utilization: {mem_util:.1f}%")
            
            # CPU vs Slots mismatch warning
            hw_cpus = hardware.get('cpuCores', 0) if hardware else 0
            if hw_cpus > 0 and slots_total > 0:
                if slots_total < hw_cpus:
                    result.append(f"  💡 TIP: You have {hw_cpus} CPU cores but only {slots_total} slot(s). Consider increasing taskmanager.numberOfTaskSlots")
                elif slots_total > hw_cpus:
                    result.append(f"  ⚠️  WARNING: {slots_total} slots configured but only {hw_cpus} CPU cores available. Potential oversubscription!")
        
        result.append("\n" + "=" * 70)
        
        # Overall recommendations
        result.append("\n💡 RECOMMENDATIONS")
        if total_free_slots == 0:
            result.append("  ⚠️  No free slots available. Cannot schedule new jobs.")
            result.append("  → Consider adding more TaskManagers or increasing slots per TaskManager")
        elif total_free_slots < total_slots * 0.2:
            result.append("  ⚡ Low slot availability. Cluster is near capacity.")
        else:
            result.append("  ✅ Sufficient slot capacity available")
        
        result.append("=" * 70)
        
        return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error listing TaskManagers: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout listing TaskManagers")
        return "❌ Request timeout while listing TaskManagers"
    except Exception as e:
        logger.error(f"Failed to list TaskManagers: {e}")
        return f"❌ Error listing TaskManagers: {str(e)}"


## ** Working
@mcp.tool()
async def get_job_exceptions(job_id: str) -> str:
    """Fetch exceptions that occurred in the specified job."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/jobs/{job_id}/exceptions"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        # Check if there are any exceptions
        all_exceptions = data.get("all-exceptions", [])
        root_exception = data.get("root-exception", "")
        exception_history = data.get("exceptionHistory", {}).get("entries", [])
        
        if not all_exceptions and not root_exception:
            return "No exceptions found for this job."

        result = ["=" * 80, "JOB EXCEPTIONS REPORT", "=" * 80, ""]
        
        # Root exception (main failure reason)
        if root_exception:
            result.append("ROOT CAUSE:")
            result.append("-" * 80)
            result.append(root_exception)
            result.append("")
        
        # All exceptions with details
        if all_exceptions:
            result.append(f"EXCEPTION DETAILS ({len(all_exceptions)} exception(s)):")
            result.append("-" * 80)
            for idx, exc in enumerate(all_exceptions, 1):
                result.append(f"\n[Exception #{idx}]")
                result.append(f"Task: {exc.get('task', 'N/A')}")
                result.append(f"TaskManager ID: {exc.get('taskManagerId', 'N/A')}")
                result.append(f"Location: {exc.get('location', 'N/A')}")
                result.append(f"Endpoint: {exc.get('endpoint', 'N/A')}")
                result.append(f"Timestamp: {exc.get('timestamp', 'N/A')}")
                result.append(f"\nStack Trace:")
                result.append(exc.get('exception', 'No stack trace available'))
                result.append("-" * 80)
        
        # Exception history (if available)
        if exception_history:
            result.append(f"\nEXCEPTION HISTORY ({len(exception_history)} entry/entries):")
            result.append("-" * 80)
            for idx, entry in enumerate(exception_history, 1):
                result.append(f"\n[History Entry #{idx}]")
                result.append(f"Exception Type: {entry.get('exceptionName', 'N/A')}")
                result.append(f"Timestamp: {entry.get('timestamp', 'N/A')}")
                
                # Concurrent exceptions
                concurrent = entry.get('concurrentExceptions', [])
                if concurrent:
                    result.append(f"\nConcurrent Exceptions ({len(concurrent)}):")
                    for cidx, cexc in enumerate(concurrent, 1):
                        result.append(f"  [{cidx}] {cexc.get('exceptionName', 'N/A')}")
                        result.append(f"      Task: {cexc.get('taskName', 'N/A')}")
                        result.append(f"      Location: {cexc.get('location', 'N/A')}")
                        result.append(f"      TaskManager: {cexc.get('taskManagerId', 'N/A')}")
                
                result.append("-" * 80)
        
        # Summary
        result.append("\nSUMMARY:")
        result.append(f"- Total Exceptions: {len(all_exceptions)}")
        result.append(f"- History Entries: {len(exception_history)}")
        result.append(f"- Truncated: {data.get('truncated', False)}")
        result.append("=" * 80)
        
        return "\n".join(result)
        
    except Exception as e:
        logger.error(f"Failed to get job exceptions: {e}")
        return f"Error: {str(e)}"

## ** Working
@mcp.tool()
async def list_jar_files() -> str:
    """List all uploaded JARs in the Flink cluster."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/jars"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            jars = response.json().get("files", [])

        if not jars:
            return "No JARs uploaded."

        return "\n".join([f"{jar['id']} - {jar['name']}" for jar in jars])
    except Exception as e:
        logger.error(f"Failed to list jars: {e}")
        return f"Error: {str(e)}"


## ** Working
@mcp.tool()
async def get_job_metrics(job_id: str) -> str:
    """Fetch selected useful metrics for a running Flink job; produce a diagnostic summary."""
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg

    base_url = f"{FLINK_CONNECTION['url'].rstrip('/')}/jobs/{job_id}/metrics"

    # Curated set for analysis & tuning (kept small for clarity; extend as needed)
    common = [
        # lifecycle/time
        "uptime",
        "runningTime",
        "downtime",
        "initializingTime",
        "deployingTime",
        "restartingTime",
        "failingTime",
        "cancellingTime",

        # restarts & stability
        "numRestarts",
        "fullRestarts",

        # checkpoints overview
        "totalNumberOfCheckpoints",
        "numberOfCompletedCheckpoints",
        "numberOfFailedCheckpoints",
        "numberOfInProgressCheckpoints",

        # last checkpoint details
        "lastCompletedCheckpointId",
        "lastCheckpointDuration",
        "lastCheckpointSize",            # logical size (may be null)
        "lastCheckpointFullSize",        # physical size including overhead
        "lastCheckpointPersistedData",
        "lastCheckpointProcessedData",
        "lastCheckpointRestoreTimestamp",
        "lastCheckpointExternalPath",

        # creation time
        "createdTime",
    ]

    # Fetch with batching; fallback to per-metric if server rejects long GETs
    async def fetch_values(client: httpx.AsyncClient, selected: List[str]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        try:
            for batch in _chunk(selected, 50):
                resp = await client.get(base_url, params={"get": ",".join(batch)})
                resp.raise_for_status()
                results.extend(resp.json())
            return results
        except httpx.HTTPStatusError:
            results.clear()
            for m in selected:
                resp = await client.get(base_url, params={"get": m})
                resp.raise_for_status()
                results.extend(resp.json())
            return results

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            # 1) discover available metric ids
            r = await client.get(base_url)
            r.raise_for_status()
            metric_ids = [m["id"] for m in r.json() if "id" in m]

            selected = [m for m in common if m in metric_ids]
            if not selected:
                return "No common metrics available for this job."

            # 2) fetch values
            values = await fetch_values(client, selected)
            by_id = _index_by_id(values)

        # 3) pull typed values
        uptime_ms                     = _to_num(by_id.get("uptime", {}).get("value"))
        running_ms                    = _to_num(by_id.get("runningTime", {}).get("value"))
        downtime_ms                   = _to_num(by_id.get("downtime", {}).get("value"))
        initializing_ms               = _to_num(by_id.get("initializingTime", {}).get("value"))
        deploying_ms                  = _to_num(by_id.get("deployingTime", {}).get("value"))
        restarting_ms                 = _to_num(by_id.get("restartingTime", {}).get("value"))
        failing_ms                    = _to_num(by_id.get("failingTime", {}).get("value"))
        cancelling_ms                 = _to_num(by_id.get("cancellingTime", {}).get("value"))

        num_restarts                  = _to_num(by_id.get("numRestarts", {}).get("value")) or 0
        full_restarts                 = _to_num(by_id.get("fullRestarts", {}).get("value")) or 0

        total_ckpt                    = _to_num(by_id.get("totalNumberOfCheckpoints", {}).get("value")) or 0
        completed_ckpt                = _to_num(by_id.get("numberOfCompletedCheckpoints", {}).get("value")) or 0
        failed_ckpt                   = _to_num(by_id.get("numberOfFailedCheckpoints", {}).get("value")) or 0
        inprog_ckpt                   = _to_num(by_id.get("numberOfInProgressCheckpoints", {}).get("value")) or 0

        last_ckpt_id                  = by_id.get("lastCompletedCheckpointId", {}).get("value")
        last_ckpt_duration_ms         = _to_num(by_id.get("lastCheckpointDuration", {}).get("value"))
        last_ckpt_size_bytes          = _to_num(by_id.get("lastCheckpointSize", {}).get("value"))
        last_ckpt_full_size_bytes     = _to_num(by_id.get("lastCheckpointFullSize", {}).get("value"))
        last_ckpt_persisted_bytes     = _to_num(by_id.get("lastCheckpointPersistedData", {}).get("value"))
        last_ckpt_processed_bytes     = _to_num(by_id.get("lastCheckpointProcessedData", {}).get("value"))
        last_ckpt_restore_ts          = _to_num(by_id.get("lastCheckpointRestoreTimestamp", {}).get("value"))
        last_ckpt_external_path       = by_id.get("lastCheckpointExternalPath", {}).get("value")

        created_time_ms               = _to_num(by_id.get("createdTime", {}).get("value"))

        # 4) derived indicators
        success_ratio = (completed_ckpt / total_ckpt) if total_ckpt > 0 else None
        failure_ratio = (failed_ckpt / total_ckpt) if total_ckpt > 0 else None

        # 5) build diagnostic report
        lines: List[str] = []
        lines.append("=== Job Runtime ===")
        lines.append(f"Uptime:        {_format_duration(uptime_ms) if uptime_ms is not None else 'N/A'}")
        lines.append(f"Running:       {_format_duration(running_ms) if running_ms is not None else 'N/A'}")
        lines.append(f"Downtime:      {_format_duration(downtime_ms) if downtime_ms is not None else 'N/A'}")
        lines.append(f"Init:          {_format_duration(initializing_ms) if initializing_ms is not None else 'N/A'}")
        lines.append(f"Deploying:     {_format_duration(deploying_ms) if deploying_ms is not None else 'N/A'}")
        lines.append(f"Restarting:    {_format_duration(restarting_ms) if restarting_ms is not None else 'N/A'}")
        lines.append(f"Failing:       {_format_duration(failing_ms) if failing_ms is not None else 'N/A'}")
        lines.append(f"Cancelling:    {_format_duration(cancelling_ms) if cancelling_ms is not None else 'N/A'}")
        lines.append(f"Created:       {_format_timestamp(created_time_ms) if created_time_ms is not None else 'N/A'}")
        lines.append("")

        lines.append("=== Stability ===")
        lines.append(f"Restarts:      {int(num_restarts)} (full: {int(full_restarts)})")
        lines.append("")

        lines.append("=== Checkpoints ===")
        lines.append(f"Total:         {int(total_ckpt)}  | Completed: {int(completed_ckpt)}  | Failed: {int(failed_ckpt)}  | In-Progress: {int(inprog_ckpt)}")
        lines.append(f"Success Ratio: {f'{success_ratio:.2%}' if success_ratio is not None else 'N/A'}"
                     f"  | Failure Ratio: {f'{failure_ratio:.2%}' if failure_ratio is not None else 'N/A'}")
        lines.append(f"Last Completed ID: {last_ckpt_id if last_ckpt_id not in (None, '') else 'N/A'}")
        lines.append(f"Last Duration:     {_format_duration(last_ckpt_duration_ms) if last_ckpt_duration_ms is not None else 'N/A'}")
        lines.append(f"Last Size (logical): {_format_bytes(last_ckpt_size_bytes) if last_ckpt_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Size (full):    {_format_bytes(last_ckpt_full_size_bytes) if last_ckpt_full_size_bytes is not None else 'N/A'}")
        lines.append(f"Last Persisted:      {_format_bytes(last_ckpt_persisted_bytes) if last_ckpt_persisted_bytes is not None else 'N/A'}")
        lines.append(f"Last Processed:      {_format_bytes(last_ckpt_processed_bytes) if last_ckpt_processed_bytes is not None else 'N/A'}")
        lines.append(f"Last Restored At:    {_format_timestamp(last_ckpt_restore_ts) if last_ckpt_restore_ts is not None else 'N/A'}")
        lines.append(f"External Path:       {last_ckpt_external_path if last_ckpt_external_path else 'N/A'}")
        lines.append("")

        # 6) hints (light heuristics)
        hints: List[str] = []
        # High uptime but no checkpoints
        if (uptime_ms or 0) > 10 * 60 * 1000 and completed_ckpt == 0:
            hints.append("High uptime with 0 completed checkpoints → verify checkpointing is enabled and backend/storage is reachable.")
        # Failures dominating
        if total_ckpt >= 5 and failure_ratio is not None and failure_ratio > 0.3:
            hints.append("Checkpoint failure ratio > 30% → investigate operator backpressure, I/O sinks, or timeout settings.")
        # Restarts
        if num_restarts >= 3:
            hints.append("Multiple restarts observed → review TaskManager/JobManager logs for exceptions or OOMs.")
        # Long durations
        if last_ckpt_duration_ms and last_ckpt_duration_ms > 60_000:
            hints.append("Last checkpoint duration > 60s → consider tuning state.backend, increasing I/O throughput, or lowering concurrency on heavy operators.")
        # No external path (when expected)
        if (completed_ckpt > 0) and not last_ckpt_external_path:
            hints.append("Completed checkpoints without externalized path → check externalization settings if you expect retained checkpoints.")

        if hints:
            lines.append("=== Hints ===")
            lines.extend(f"- {h}" for h in hints)

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"Failed to get job metrics: {e}")
        return f"Error: {str(e)}"

# New 

@mcp.tool()
async def get_taskmanager_details(taskmanager_id: str) -> str:
    """Get comprehensive details about a specific TaskManager including metrics.
    
    Args:
        taskmanager_id: TaskManager ID (e.g., '172.20.0.3:38373-66c42c')
        
    Returns detailed information about:
    - Basic info (ID, ports, heartbeat)
    - Slot allocation and usage
    - Hardware resources (CPU, memory)
    - Memory configuration breakdown
    - Currently allocated slots and jobs
    - Real-time metrics (heap, non-heap, GC stats)
    - Network buffer usage
    
    This is more detailed than list_taskmanagers which shows all TaskManagers.
    Use this when you need to deep-dive into a specific TaskManager.
    """
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/taskmanagers/{taskmanager_id}"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            tm = response.json()
        
        result = []
        result.append("=" * 80)
        result.append(f"TASKMANAGER DETAILS: {taskmanager_id}")
        result.append("=" * 80)
        
        # Basic Info
        result.append("\n📋 BASIC INFORMATION")
        result.append(f"  ID: {tm.get('id', 'N/A')}")
        result.append(f"  Path: {tm.get('path', 'N/A')}")
        result.append(f"  Data Port: {tm.get('dataPort', 'N/A')}")
        result.append(f"  JMX Port: {tm.get('jmxPort', 'Disabled' if tm.get('jmxPort', -1) == -1 else tm.get('jmxPort'))}")
        
        # Heartbeat
        heartbeat = tm.get('timeSinceLastHeartbeat', 0)
        if heartbeat > 0:
            from datetime import datetime
            last_heartbeat = datetime.fromtimestamp(heartbeat / 1000.0)
            result.append(f"  Last Heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Slot Information
        slots_total = tm.get('slotsNumber', 0)
        slots_free = tm.get('freeSlots', 0)
        slots_used = slots_total - slots_free
        
        result.append("\n🎰 SLOT INFORMATION")
        result.append(f"  Total Slots: {slots_total}")
        result.append(f"  Used Slots: {slots_used} ({(slots_used/slots_total*100) if slots_total > 0 else 0:.1f}%)")
        result.append(f"  Free Slots: {slots_free} ({(slots_free/slots_total*100) if slots_total > 0 else 0:.1f}%)")
        
        # Allocated Slots
        allocated_slots = tm.get('allocatedSlots', [])
        if allocated_slots:
            result.append(f"\n📌 ALLOCATED SLOTS ({len(allocated_slots)})")
            for idx, slot in enumerate(allocated_slots, 1):
                job_id = slot.get('jobId', 'N/A')
                resource = slot.get('resource', {})
                result.append(f"  [{idx}] Job: {job_id}")
                result.append(f"      CPU Cores: {resource.get('cpuCores', 'N/A')}")
                result.append(f"      Task Heap: {_format_bytes(resource.get('taskHeapMemory', 0) * 1024 * 1024)}")
                result.append(f"      Managed Memory: {_format_bytes(resource.get('managedMemory', 0) * 1024 * 1024)}")
                result.append(f"      Network Memory: {_format_bytes(resource.get('networkMemory', 0) * 1024 * 1024)}")
        
        # Hardware
        hardware = tm.get('hardware', {})
        if hardware:
            result.append("\n💻 HARDWARE")
            result.append(f"  CPU Cores: {hardware.get('cpuCores', 'N/A')}")
            phys_mem = hardware.get('physicalMemory', 0)
            free_mem = hardware.get('freeMemory', 0)
            managed_mem = hardware.get('managedMemory', 0)
            
            result.append(f"  Physical Memory: {_format_bytes(phys_mem)}")
            result.append(f"  Free Memory: {_format_bytes(free_mem)} ({(free_mem/phys_mem*100) if phys_mem > 0 else 0:.1f}%)")
            result.append(f"  Managed Memory: {_format_bytes(managed_mem)}")
        
        # Resource Configuration
        total_res = tm.get('totalResource', {})
        free_res = tm.get('freeResource', {})
        
        result.append("\n📊 RESOURCE ALLOCATION")
        result.append(f"  Configured:")
        result.append(f"    CPU Cores: {total_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {_format_bytes(total_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Task Off-Heap: {_format_bytes(total_res.get('taskOffHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {_format_bytes(total_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {_format_bytes(total_res.get('networkMemory', 0) * 1024 * 1024)}")
        
        result.append(f"  Available:")
        result.append(f"    CPU Cores: {free_res.get('cpuCores', 'N/A')}")
        result.append(f"    Task Heap: {_format_bytes(free_res.get('taskHeapMemory', 0) * 1024 * 1024)}")
        result.append(f"    Managed Memory: {_format_bytes(free_res.get('managedMemory', 0) * 1024 * 1024)}")
        result.append(f"    Network Memory: {_format_bytes(free_res.get('networkMemory', 0) * 1024 * 1024)}")
        
        # Memory Configuration
        mem_config = tm.get('memoryConfiguration', {})
        if mem_config:
            result.append("\n🧠 MEMORY CONFIGURATION")
            result.append(f"  Framework Heap: {_format_bytes(mem_config.get('frameworkHeap', 0))}")
            result.append(f"  Framework Off-Heap: {_format_bytes(mem_config.get('frameworkOffHeap', 0))}")
            result.append(f"  Task Heap: {_format_bytes(mem_config.get('taskHeap', 0))}")
            result.append(f"  Task Off-Heap: {_format_bytes(mem_config.get('taskOffHeap', 0))}")
            result.append(f"  Network Memory: {_format_bytes(mem_config.get('networkMemory', 0))}")
            result.append(f"  Managed Memory: {_format_bytes(mem_config.get('managedMemory', 0))}")
            result.append(f"  JVM Metaspace: {_format_bytes(mem_config.get('jvmMetaspace', 0))}")
            result.append(f"  JVM Overhead: {_format_bytes(mem_config.get('jvmOverhead', 0))}")
            result.append(f"  Total Flink Memory: {_format_bytes(mem_config.get('totalFlinkMemory', 0))}")
            result.append(f"  Total Process Memory: {_format_bytes(mem_config.get('totalProcessMemory', 0))}")
        
        # Real-time Metrics
        metrics = tm.get('metrics', {})
        if metrics:
            result.append("\n📈 REAL-TIME METRICS")
            
            # Memory metrics
            heap_used = metrics.get('heapUsed', 0)
            heap_committed = metrics.get('heapCommitted', 0)
            heap_max = metrics.get('heapMax', 0)
            
            result.append(f"  Heap Memory:")
            result.append(f"    Used: {_format_bytes(heap_used)} / {_format_bytes(heap_max)} ({(heap_used/heap_max*100) if heap_max > 0 else 0:.1f}%)")
            result.append(f"    Committed: {_format_bytes(heap_committed)}")
            
            if heap_used / heap_max > 0.9 if heap_max > 0 else False:
                result.append(f"    ⚠️  WARNING: Heap usage above 90%!")
            
            non_heap_used = metrics.get('nonHeapUsed', 0)
            non_heap_max = metrics.get('nonHeapMax', 0)
            
            result.append(f"  Non-Heap Memory:")
            result.append(f"    Used: {_format_bytes(non_heap_used)} / {_format_bytes(non_heap_max)}")
            result.append(f"    Committed: {_format_bytes(metrics.get('nonHeapCommitted', 0))}")
            
            # Direct memory
            direct_used = metrics.get('directUsed', 0)
            direct_max = metrics.get('directMax', 0)
            result.append(f"  Direct Memory:")
            result.append(f"    Used: {_format_bytes(direct_used)} / {_format_bytes(direct_max)} ({(direct_used/direct_max*100) if direct_max > 0 else 0:.1f}%)")
            result.append(f"    Buffer Count: {metrics.get('directCount', 0):,}")
            
            # Network buffers
            mem_segs_avail = metrics.get('memorySegmentsAvailable', 0)
            mem_segs_total = metrics.get('memorySegmentsTotal', 0)
            
            result.append(f"  Network Buffers:")
            result.append(f"    Available: {mem_segs_avail:,} / {mem_segs_total:,}")
            result.append(f"    Netty Shuffle:")
            result.append(f"      Available: {_format_bytes(metrics.get('nettyShuffleMemoryAvailable', 0))}")
            result.append(f"      Used: {_format_bytes(metrics.get('nettyShuffleMemoryUsed', 0))}")
            result.append(f"      Total: {_format_bytes(metrics.get('nettyShuffleMemoryTotal', 0))}")
            
            # Garbage Collection
            gc_stats = metrics.get('garbageCollectors', [])
            if gc_stats:
                result.append(f"  Garbage Collection:")
                for gc in gc_stats:
                    gc_name = gc.get('name', 'Unknown')
                    gc_count = gc.get('count', 0)
                    gc_time = gc.get('time', 0)
                    result.append(f"    {gc_name}: {gc_count} collections, {gc_time}ms total")
        
        result.append("=" * 80)
        
        return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ TaskManager not found: {taskmanager_id}"
        logger.error(f"HTTP error fetching TaskManager details: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching TaskManager details")
        return "❌ Request timeout while fetching TaskManager details"
    except Exception as e:
        logger.error(f"Failed to get TaskManager details: {e}")
        return f"❌ Error getting TaskManager details: {str(e)}"


@mcp.tool()
async def get_taskmanager_metrics(
    taskmanager_id: str,
    metric_names: Optional[str] = None
) -> str:
    """Get specific metrics for a TaskManager.
    
    Args:
        taskmanager_id: TaskManager ID
        metric_names: Optional comma-separated list of metric names to query.
                     If not provided, returns list of available metrics.
                     
    Common metrics to query:
    - Status.JVM.CPU.Load: CPU usage
    - Status.JVM.Memory.Heap.Used/Max: Heap memory
    - Status.JVM.Memory.NonHeap.Used: Non-heap memory
    - Status.JVM.Threads.Count: Thread count
    - Status.Flink.Memory.Managed.Used/Total: Managed memory
    - Status.Network.AvailableMemorySegments: Network buffers
    
    Example:
        metric_names="Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used"
    """
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    base_url = f"{FLINK_CONNECTION['url']}/taskmanagers/{taskmanager_id}/metrics"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            if metric_names:
                # Query specific metrics
                url = f"{base_url}?get={metric_names}"
                response = await client.get(url)
                response.raise_for_status()
                metrics = response.json()
                
                result = []
                result.append("=" * 70)
                result.append(f"TASKMANAGER METRICS: {taskmanager_id}")
                result.append("=" * 70)
                
                for metric in metrics:
                    metric_id = metric.get('id', 'Unknown')
                    metric_value = metric.get('value', 'N/A')
                    
                    # Format based on metric type
                    if 'Memory' in metric_id and 'Ratio' not in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = _format_bytes(metric_value)
                        else:
                            formatted_value = metric_value
                    elif 'CPU.Load' in metric_id or 'Ratio' in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = f"{metric_value * 100:.2f}%" if metric_value <= 1 else f"{metric_value:.2f}%"
                        else:
                            formatted_value = metric_value
                    elif 'Count' in metric_id or 'Segments' in metric_id:
                        if isinstance(metric_value, (int, float)):
                            formatted_value = f"{int(metric_value):,}"
                        else:
                            formatted_value = metric_value
                    else:
                        formatted_value = metric_value
                    
                    result.append(f"  {metric_id}: {formatted_value}")
                
                result.append("=" * 70)
                return "\n".join(result)
            else:
                # List available metrics
                response = await client.get(base_url)
                response.raise_for_status()
                metrics = response.json()
                
                result = []
                result.append("=" * 70)
                result.append(f"AVAILABLE METRICS for {taskmanager_id}")
                result.append("=" * 70)
                result.append(f"\nTotal metrics available: {len(metrics)}\n")
                
                # Group metrics by category
                categories = {}
                for metric in metrics:
                    metric_id = metric.get('id', '')
                    parts = metric_id.split('.')
                    category = '.'.join(parts[:3]) if len(parts) >= 3 else 'Other'
                    
                    if category not in categories:
                        categories[category] = []
                    categories[category].append(metric_id)
                
                for category in sorted(categories.keys()):
                    result.append(f"\n{category}:")
                    for metric_id in sorted(categories[category]):
                        result.append(f"  - {metric_id}")
                
                result.append("\n" + "=" * 70)
                result.append("\n💡 TIP: To query specific metrics, use:")
                result.append('   metric_names="Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used"')
                result.append("=" * 70)
                
                return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ TaskManager not found: {taskmanager_id}"
        logger.error(f"HTTP error fetching TaskManager metrics: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching TaskManager metrics")
        return "❌ Request timeout while fetching TaskManager metrics"
    except Exception as e:
        logger.error(f"Failed to get TaskManager metrics: {e}")
        return f"❌ Error getting TaskManager metrics: {str(e)}"


@mcp.tool()
async def get_vertex_backpressure(job_id: str, vertex_id: str) -> str:
    """Get backpressure information for a specific operator/vertex.
    
    Args:
        job_id: Job ID
        vertex_id: Vertex/operator ID
        
    Returns:
    - Overall backpressure status (ok, low, high)
    - Backpressure level/ratio (0.0 to 1.0)
    - Per-subtask backpressure breakdown
    - Idle and busy ratios
    
    Backpressure indicates when an operator can't keep up with incoming data.
    - OK: No backpressure, operator processing smoothly
    - LOW: Minor backpressure, monitor but not critical
    - HIGH: Severe backpressure, operator is bottleneck - needs optimization!
    
    Use this to identify performance bottlenecks in your job.
    """
    is_init, error_msg = check_initialized()
    if not is_init:
        return error_msg
    
    url = f"{FLINK_CONNECTION['url']}/jobs/{job_id}/vertices/{vertex_id}/backpressure"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            bp_data = response.json()
        
        result = []
        result.append("=" * 70)
        result.append("BACKPRESSURE ANALYSIS")
        result.append("=" * 70)
        result.append(f"Job ID: {job_id}")
        result.append(f"Vertex ID: {vertex_id}")
        
        # Overall status
        status = bp_data.get('status', 'unknown')
        bp_level = bp_data.get('backpressureLevel', 'unknown').upper()
        end_ts = bp_data.get('end-timestamp', 0)
        
        result.append(f"\n📊 OVERALL STATUS")
        result.append(f"  Status: {status}")
        
        # Color-code backpressure level
        if bp_level == 'OK':
            result.append(f"  Backpressure Level: ✅ {bp_level} - No issues")
        elif bp_level == 'LOW':
            result.append(f"  Backpressure Level: ⚡ {bp_level} - Minor pressure")
        elif bp_level == 'HIGH':
            result.append(f"  Backpressure Level: 🔴 {bp_level} - BOTTLENECK DETECTED!")
        else:
            result.append(f"  Backpressure Level: {bp_level}")
        
        if end_ts > 0:
            from datetime import datetime
            measurement_time = datetime.fromtimestamp(end_ts / 1000.0)
            result.append(f"  Measured At: {measurement_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Per-subtask breakdown
        subtasks = bp_data.get('subtasks', [])
        if subtasks:
            result.append(f"\n📋 PER-SUBTASK BREAKDOWN ({len(subtasks)} subtask(s))")
            
            for subtask in subtasks:
                subtask_idx = subtask.get('subtask', 0)
                subtask_bp_level = subtask.get('backpressureLevel', 'unknown').upper()
                ratio = subtask.get('ratio', 0)
                idle_ratio = subtask.get('idleRatio', 0)
                busy_ratio = subtask.get('busyRatio', 'NaN')
                
                result.append(f"\n  Subtask #{subtask_idx}:")
                
                if subtask_bp_level == 'OK':
                    result.append(f"    Status: ✅ {subtask_bp_level}")
                elif subtask_bp_level == 'LOW':
                    result.append(f"    Status: ⚡ {subtask_bp_level}")
                elif subtask_bp_level == 'HIGH':
                    result.append(f"    Status: 🔴 {subtask_bp_level}")
                else:
                    result.append(f"    Status: {subtask_bp_level}")
                
                result.append(f"    Backpressure Ratio: {ratio:.2%}" if isinstance(ratio, (int, float)) else f"    Backpressure Ratio: {ratio}")
                result.append(f"    Idle Ratio: {idle_ratio:.2%}" if isinstance(idle_ratio, (int, float)) else f"    Idle Ratio: {idle_ratio}")
                
                if busy_ratio != 'NaN' and isinstance(busy_ratio, (int, float)):
                    result.append(f"    Busy Ratio: {busy_ratio:.2%}")
                else:
                    result.append(f"    Busy Ratio: {busy_ratio}")
        
        # Analysis & Recommendations
        result.append("\n" + "=" * 70)
        result.append("ANALYSIS & RECOMMENDATIONS")
        result.append("=" * 70)
        
        if bp_level == 'OK':
            result.append("✅ Operator is processing data smoothly with no backpressure.")
            result.append("   No action needed.")
        elif bp_level == 'LOW':
            result.append("⚡ Minor backpressure detected. Monitor this operator.")
            result.append("   Consider:")
            result.append("   - Check if this is temporary or persistent")
            result.append("   - Monitor for trend over time")
        elif bp_level == 'HIGH':
            result.append("🔴 CRITICAL: High backpressure - this operator is a bottleneck!")
            result.append("   Immediate actions:")
            result.append("   1. Increase parallelism for this operator")
            result.append("   2. Optimize operator logic (reduce computation)")
            result.append("   3. Check if downstream systems are slow")
            result.append("   4. Review resource allocation (CPU, memory)")
            result.append("   5. Consider data partitioning/rebalancing")
        
        # Check for data skew
        if subtasks and len(subtasks) > 1:
            ratios = [s.get('ratio', 0) for s in subtasks if isinstance(s.get('ratio', 0), (int, float))]
            if ratios and max(ratios) - min(ratios) > 0.3:
                result.append("\n⚠️  Potential data skew detected!")
                result.append("   Some subtasks have significantly different backpressure.")
                result.append("   Consider rebalancing or repartitioning your data.")
        
        result.append("=" * 70)
        
        return "\n".join(result)
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"❌ Job or vertex not found: {job_id} / {vertex_id}"
        logger.error(f"HTTP error fetching backpressure: {e}")
        return f"❌ HTTP Error ({e.response.status_code}): {str(e)}"
    except httpx.TimeoutException:
        logger.error("Timeout fetching backpressure")
        return "❌ Request timeout while fetching backpressure"
    except Exception as e:
        logger.error(f"Failed to get backpressure: {e}")
        return f"❌ Error getting backpressure: {str(e)}"
    

@mcp.tool 
def send_email_notification(recipient_email: str, content: str, subject: str = "Flink AI Notifications") -> str:
    """
    Send email notification using Gmail SMTP.
    
    Args:
        recipient_email: Email address to send notification to
        content: Email content/body
        subject: Email subject line (optional)
    
    Returns:
        JSON string indicating email send status
    """
    try:
        # Validate inputs first
        if not recipient_email or "@" not in recipient_email:
            return json.dumps({
                "status": "failed",
                "error": "Invalid recipient email address"
            })
        
        if not content.strip():
            return json.dumps({
                "status": "failed",
                "error": "Email content cannot be empty"
            })
        
        # Gmail SMTP settings
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        
        # Email credentials or get from env
        sender_email = os.getenv('GMAIL_USER', 'default')
        sender_password = os.getenv('GMAIL_PASSWORD', 'default')

        
        # Create email message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject
        
        # Simple text email body
        email_body = f"""Flink AI Notification
===================

{content}

---
Sent by Flink AI at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        message.attach(MIMEText(email_body, "plain"))
        
        # Send email via Gmail SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Enable encryption
        server.login(sender_email, sender_password)
        
        # Send the email
        text = message.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        return json.dumps({
            "status": "sent",
            "message": "Email sent successfully",
            "recipient": recipient_email,
            "subject": subject,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except smtplib.SMTPAuthenticationError:
        return json.dumps({
            "status": "failed",
            "error": "Gmail authentication failed. Check email/password or enable 2FA and use App Password"
        })
    except Exception as e:
        return json.dumps({
            "status": "failed",
            "error": f"Failed to send email: {str(e)}"
        })
        

def main():
    try:
        logger.info("Starting Flink MCP server...")
        logger.info("=" * 60)
        logger.info("Available tools:")
        logger.info("-" * 60)
        logger.info("  🔌 initialize_flink_connection: Connect to Flink REST API")
        logger.info("  📊 get_connection_status: Check connection status")
        logger.info("  🏢 get_cluster_info: Overview of the Flink cluster")
        logger.info("  📋 list_jobs: List all Flink jobs with status")
        logger.info("  🔍 get_job_details: Comprehensive job details by ID")
        logger.info("  ⚠️  get_job_exceptions: Fetch job-level exceptions")
        logger.info("  📈 get_job_metrics: Fetch metrics for a job")
        logger.info("  💻 list_taskmanagers: List TaskManagers with resources")
        logger.info("  📦 list_jar_files: List uploaded JAR files")
        logger.info("=" * 60)
        logger.info("Server starting on http://127.0.0.1:9090")
        logger.info("=" * 60)
        
        mcp.run(transport="streamable-http", host="127.0.0.1", port=9090)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Server stopped by user")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Server error: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()