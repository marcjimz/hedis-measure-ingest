"""
Databricks Integration Package

Services for interacting with Databricks resources:
- Unity Catalog functions
- Model serving endpoints
- Agent deployments

Note: Imports are lazy to avoid loading pyspark in mock mode.
Use direct imports from submodules instead:
    from databricks.uc_functions import UCFunctionsService
    from databricks.agent_service import AgentService
"""

# Lazy imports - only import when actually used
# This prevents pyspark from being loaded in mock mode

__all__ = [
    "UCFunctionsService",
    "AgentService",
    "MockAgentService"
]

def __getattr__(name):
    """Lazy import to avoid loading pyspark in mock mode."""
    if name == "UCFunctionsService":
        from databricks.uc_functions import UCFunctionsService
        return UCFunctionsService
    elif name == "AgentService":
        from databricks.agent_service import AgentService
        return AgentService
    elif name == "MockAgentService":
        from databricks.mock_agent_service import MockAgentService
        return MockAgentService
    raise AttributeError(f"module 'databricks' has no attribute '{name}'")
