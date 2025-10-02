# proxy.py - Transport Bridging for claude, Link : https://gofastmcp.com/servers/proxy#transport-bridging
from fastmcp import FastMCP

mcp = FastMCP.as_proxy("http://127.0.0.1:9090/mcp/", name="Kafka MCP Server")

if __name__ == "__main__":
    mcp.run()