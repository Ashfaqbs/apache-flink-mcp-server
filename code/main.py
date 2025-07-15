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