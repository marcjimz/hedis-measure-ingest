"""
HEDIS Chat Agent

A production-ready LangGraph-based chat agent for HEDIS measure analysis.

Features:
- Answer questions about HEDIS measures using vector search and measure definitions
- Integration with Unity Catalog functions (measure lookup, document search, query expansion)
- Configurable persistence with PostgreSQL checkpointing
- Streaming and non-streaming support
- Thread-based conversation management
"""

from typing import Any, Generator, Optional, Sequence, Union, List, Dict
import uuid
import os
import pathlib
import sys
import json
from psycopg import Connection
from psycopg_pool import ConnectionPool

# Add agent root to path for deployment
agent_root = pathlib.Path(__file__).resolve().parent
src_path = agent_root / 'code'
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

import mlflow
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    DatabricksFunctionClient,
    set_uc_function_client,
)
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph, MessagesState
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from langgraph.checkpoint.postgres import PostgresSaver
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# Import HEDIS prompts
try:
    from src.agents.prompts.hedis import HEDIS_CHAT_AGENT_SYSTEM_PROMPT
except ImportError:
    # Fallback for different import contexts
    from agents.prompts.hedis import HEDIS_CHAT_AGENT_SYSTEM_PROMPT

# Enable MLflow tracing for LangChain/LangGraph
mlflow.autolog()

class HEDISChatAgent(ChatAgent):
    """
    HEDIS Chat Agent for answering questions about HEDIS measures.

    Supports:
    - Answer questions about HEDIS measures using UC tools
    - Configurable persistence with PostgreSQL
    - Streaming and non-streaming responses
    - Thread-based conversation management
    """

    def __init__(
        self,
        model: LanguageModelLike,
        tools: Union[Sequence[BaseTool], ToolNode],
        conn_string: Optional[str] = None,
        connection_pool: Optional[ConnectionPool] = None,
        enable_persistence: bool = False,
        effective_year: Optional[int] = None
    ):
        """
        Initialize HEDIS Chat Agent.

        Args:
            model: Language model for chat responses
            tools: Tools for measure lookup and vector search
            conn_string: Database connection string for persistence (optional, deprecated)
            connection_pool: Database connection pool with auto-refresh (preferred)
            enable_persistence: Enable PostgreSQL checkpointing (default: False)
            effective_year: HEDIS measurement year (optional, auto-detected if None)
        """
        self.model = model
        self.tools = tools
        self.conn_string = conn_string
        self.connection_pool = connection_pool
        self.enable_persistence = enable_persistence
        self.effective_year = effective_year or 2025  # Will be updated by factory
        self.thread_id = "default"

        # Validate configuration
        if enable_persistence and not (conn_string or connection_pool):
            raise ValueError("Connection string or connection pool required when persistence is enabled")

    def _convert_to_langchain_messages(self, messages: List[Union[ChatAgentMessage, dict]]):
        """
        Convert messages to LangChain message objects for graph execution.

        Args:
            messages: List of ChatAgentMessage or dict messages

        Returns:
            List of LangChain message objects (HumanMessage, AIMessage, etc.)
        """
        from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage

        result = []
        for msg in messages:
            # Extract message data
            if isinstance(msg, dict):
                role = msg.get("role", "user")
                content = msg.get("content", "")
                tool_calls = msg.get("tool_calls")
                tool_call_id = msg.get("tool_call_id")
                name = msg.get("name")
            else:
                # ChatAgentMessage or similar object
                role = getattr(msg, 'role', 'user')
                content = getattr(msg, 'content', '')
                tool_calls = getattr(msg, 'tool_calls', None)
                tool_call_id = getattr(msg, 'tool_call_id', None)
                name = getattr(msg, 'name', None)

            # Create appropriate LangChain message type
            if role == "system":
                result.append(SystemMessage(content=content))
            elif role == "assistant":
                # AIMessage with optional tool calls
                if tool_calls:
                    result.append(AIMessage(content=content, tool_calls=tool_calls))
                else:
                    result.append(AIMessage(content=content))
            elif role == "tool":
                # ToolMessage needs tool_call_id
                result.append(ToolMessage(content=content, tool_call_id=tool_call_id or str(uuid.uuid4()), name=name))
            else:  # "user" or default
                result.append(HumanMessage(content=content))

        return result

    def _build_system_prompt(self) -> str:
        """
        Build the system prompt for the agent.

        Returns:
            Formatted system prompt
        """
        return HEDIS_CHAT_AGENT_SYSTEM_PROMPT.format(
            effective_year=self.effective_year
        )

    def _parse_message(self, msg) -> ChatAgentMessage:
        """
        Parse different message types into ChatAgentMessage.

        Args:
            msg: Message object (dict or LangChain message)

        Returns:
            ChatAgentMessage instance
        """
        if isinstance(msg, dict):
            return ChatAgentMessage(
                id=msg.get("id", str(uuid.uuid4())),
                role=msg.get("role", "assistant"),
                content=msg.get("content", ""),
                name=msg.get("name"),
                tool_calls=msg.get("tool_calls"),
                tool_call_id=msg.get("tool_call_id"),
                attachments=msg.get("attachments")
            )
        else:
            # Handle LangChain message objects
            from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

            content = getattr(msg, 'content', '')

            # Determine role based on message type
            if isinstance(msg, AIMessage):
                role = 'assistant'
            elif isinstance(msg, HumanMessage):
                role = 'user'
            elif isinstance(msg, SystemMessage):
                role = 'system'
            elif isinstance(msg, ToolMessage):
                role = 'tool'
            else:
                role = getattr(msg, 'role', 'assistant')

            # Handle tool calls - transform LangChain format to ChatAgentMessage format
            tool_calls = None
            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                if isinstance(msg.tool_calls, list):
                    tool_calls = []
                    for tc in msg.tool_calls:
                        # LangChain tool_call: {'name': str, 'args': dict, 'id': str, 'type': str}
                        # ChatAgentMessage needs: {'id': str, 'type': 'function', 'function': {'name': str, 'arguments': str}}
                        if isinstance(tc, dict):
                            tool_calls.append({
                                'id': tc.get('id', str(uuid.uuid4())),
                                'type': 'function',
                                'function': {
                                    'name': tc.get('name', ''),
                                    'arguments': json.dumps(tc.get('args', {}))
                                }
                            })
                        else:
                            tool_calls.append({
                                'id': getattr(tc, 'id', str(uuid.uuid4())),
                                'type': 'function',
                                'function': {
                                    'name': getattr(tc, 'name', ''),
                                    'arguments': json.dumps(getattr(tc, 'args', {}))
                                }
                            })
                else:
                    tool_calls = msg.tool_calls
            elif hasattr(msg, 'additional_kwargs') and msg.additional_kwargs.get('tool_calls'):
                tool_calls = msg.additional_kwargs.get('tool_calls')

            return ChatAgentMessage(
                id=getattr(msg, 'id', str(uuid.uuid4())),
                role=role,
                content=content,
                name=getattr(msg, 'name', None),
                tool_calls=tool_calls,
                tool_call_id=getattr(msg, 'tool_call_id', None),
                attachments=getattr(msg, 'attachments', None)
            )

    def _create_agent_graph(
        self,
        checkpointer: Optional[PostgresSaver] = None
    ) -> CompiledStateGraph:
        """
        Create the LangGraph agent with optional checkpointing and parallel tool execution.

        Args:
            checkpointer: Optional PostgresSaver for persistence

        Returns:
            Compiled LangGraph state graph
        """
        # Bind tools to model
        model_with_tools = self.model.bind_tools(self.tools) if self.tools else self.model

        # Build system prompt
        system_prompt = self._build_system_prompt()

        def call_model(state: MessagesState, config: RunnableConfig):
            """Call the model with system prompt prepended."""
            from langchain_core.messages import SystemMessage

            messages_with_system = state["messages"]

            # Prepend system prompt if not already present
            if system_prompt:
                # Check if first message is already a system message
                has_system = False
                if messages_with_system:
                    first_msg = messages_with_system[0]
                    if isinstance(first_msg, SystemMessage):
                        has_system = True
                    elif isinstance(first_msg, dict) and first_msg.get("role") == "system":
                        has_system = True

                if not has_system:
                    # Prepend SystemMessage object
                    messages_with_system = [
                        SystemMessage(content=system_prompt)
                    ] + messages_with_system

            response = model_with_tools.invoke(messages_with_system, config)
            return {"messages": [response]}

        # Build the graph
        workflow = StateGraph(MessagesState)
        workflow.add_node("agent", call_model)

        if self.tools:
            # Add tools node with parallel execution enabled
            # Use standard ToolNode which executes tools in parallel by default
            workflow.add_node("tools", ToolNode(self.tools))
            workflow.set_entry_point("agent")

            # Agent -> Tools (conditionally) -> Agent
            # Inline conditional logic to avoid separate evaluation step
            workflow.add_conditional_edges(
                "agent",
                lambda state: "tools" if self._has_tool_calls(state["messages"][-1]) else END,
            )
            workflow.add_edge("tools", "agent")
        else:
            # No tools
            workflow.set_entry_point("agent")
            workflow.add_edge("agent", END)

        # Compile with or without checkpointer
        if checkpointer:
            graph = workflow.compile(checkpointer=checkpointer)
        else:
            graph = workflow.compile()

        return graph

    def _has_tool_calls(self, message) -> bool:
        """
        Check if a message has tool calls without being traced as a separate step.

        Args:
            message: Last message to check for tool calls

        Returns:
            True if message has tool calls, False otherwise
        """
        if hasattr(message, 'tool_calls') and message.tool_calls:
            return True
        elif isinstance(message, dict) and message.get("tool_calls"):
            return True
        elif hasattr(message, 'additional_kwargs'):
            return bool(message.additional_kwargs.get("tool_calls"))
        return False

    def predict(
        self,
        messages: List[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        """
        Generate a response for the given messages.

        Args:
            messages: List of chat messages
            context: Optional chat context
            custom_inputs: Optional custom inputs (thread_id)

        Returns:
            ChatAgentResponse with messages and custom outputs
        """
        # Extract configuration from custom_inputs
        custom_inputs = custom_inputs or {}
        thread_id = custom_inputs.get("thread_id")

        # Handle persistence and threading
        if self.enable_persistence and (self.connection_pool or self.conn_string):
            # Use persistent mode with checkpointing
            if thread_id:
                # Continuing conversation - send only new message
                messages_to_send = [messages[-1]] if messages else []
            else:
                # New conversation
                thread_id = str(uuid.uuid4())
                messages_to_send = messages

            config = {"configurable": {"thread_id": thread_id}}

            # Prefer connection pool over connection string
            if self.connection_pool:
                # Use connection pool (has automatic credential refresh)
                with self.connection_pool.connection() as conn:
                    checkpointer = PostgresSaver(conn)
                    agent = self._create_agent_graph(checkpointer)

                    langchain_messages = self._convert_to_langchain_messages(messages_to_send)
                    result = agent.invoke({"messages": langchain_messages}, config)

                    # Parse output messages
                    out_messages = []
                    if result.get("messages"):
                        for msg in result["messages"]:
                            out_messages.append(self._parse_message(msg))
            else:
                # Fall back to connection string (deprecated)
                with Connection.connect(self.conn_string) as conn:
                    checkpointer = PostgresSaver(conn)
                    agent = self._create_agent_graph(checkpointer)

                    langchain_messages = self._convert_to_langchain_messages(messages_to_send)
                    result = agent.invoke({"messages": langchain_messages}, config)

                    # Parse output messages
                    out_messages = []
                    if result.get("messages"):
                        for msg in result["messages"]:
                            out_messages.append(self._parse_message(msg))
        else:
            # Non-persistent mode - no threading
            thread_id = thread_id or str(uuid.uuid4())
            agent = self._create_agent_graph(None)

            langchain_messages = self._convert_to_langchain_messages(messages)
            result = agent.invoke({"messages": langchain_messages})

            # Parse output messages
            out_messages = []
            if result.get("messages"):
                for msg in result["messages"]:
                    out_messages.append(self._parse_message(msg))

        # Build custom outputs
        custom_outputs = {
            "thread_id": thread_id,
            "effective_year": self.effective_year,
            "persistence_enabled": self.enable_persistence
        }

        # Return response
        try:
            return ChatAgentResponse(
                messages=out_messages,
                custom_outputs=custom_outputs
            )
        except TypeError:
            response = ChatAgentResponse(messages=out_messages)
            response.custom_outputs = custom_outputs
            return response

    def predict_stream(
        self,
        messages: List[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        """
        Generate a streaming response for the given messages.

        Args:
            messages: List of chat messages
            context: Optional chat context
            custom_inputs: Optional custom inputs (thread_id)

        Yields:
            ChatAgentChunk objects with message deltas
        """
        # Extract configuration
        custom_inputs = custom_inputs or {}
        thread_id = custom_inputs.get("thread_id")

        # Handle persistence and threading
        if self.enable_persistence and (self.connection_pool or self.conn_string):
            if thread_id:
                messages_to_send = [messages[-1]] if messages else []
            else:
                thread_id = str(uuid.uuid4())
                messages_to_send = messages

            config = {"configurable": {"thread_id": thread_id}}

            # Prefer connection pool over connection string
            if self.connection_pool:
                # Use connection pool (has automatic credential refresh)
                with self.connection_pool.connection() as conn:
                    checkpointer = PostgresSaver(conn)
                    agent = self._create_agent_graph(checkpointer)

                    langchain_messages = self._convert_to_langchain_messages(messages_to_send)

                    for chunk in agent.stream({"messages": langchain_messages}, config, stream_mode="values"):
                        if chunk.get("messages"):
                            for msg in chunk["messages"]:
                                parsed_msg = self._parse_message(msg)
                                yield ChatAgentChunk(delta=parsed_msg.__dict__)
            else:
                # Fall back to connection string (deprecated)
                with Connection.connect(self.conn_string) as conn:
                    checkpointer = PostgresSaver(conn)
                    agent = self._create_agent_graph(checkpointer)

                    langchain_messages = self._convert_to_langchain_messages(messages_to_send)

                    for chunk in agent.stream({"messages": langchain_messages}, config, stream_mode="values"):
                        if chunk.get("messages"):
                            for msg in chunk["messages"]:
                                parsed_msg = self._parse_message(msg)
                                yield ChatAgentChunk(delta=parsed_msg.__dict__)
        else:
            # Non-persistent streaming
            thread_id = thread_id or str(uuid.uuid4())
            agent = self._create_agent_graph(None)

            langchain_messages = self._convert_to_langchain_messages(messages)

            for chunk in agent.stream({"messages": langchain_messages}, stream_mode="values"):
                if chunk.get("messages"):
                    for msg in chunk["messages"]:
                        parsed_msg = self._parse_message(msg)
                        yield ChatAgentChunk(delta=parsed_msg.__dict__)

        # Don't yield custom_outputs in streaming mode as it causes validation errors
        # Custom outputs can be retrieved from the final response if needed


# ============================================================================
# Factory for Creating HEDIS Chat Agent
# ============================================================================

class HEDISChatAgentFactory:
    """
    Factory for building HEDIS Chat Agent with Unity Catalog tools.
    """

    # Default UC functions for HEDIS operations
    DEFAULT_UC_FUNCTIONS = [
        "measures_definition_lookup",  # Tool for looking up measure definitions
        "measures_document_search",    # Tool for semantic search over HEDIS chunks
        "measures_search_expansion"    # Tool for AI-powered query expansion
    ]

    @staticmethod
    def _get_latest_effective_year(catalog_name: str, schema_name: str) -> int:
        """
        Query the database to get the latest effective_year from hedis_measures_definitions.

        Args:
            catalog_name: Unity Catalog catalog name
            schema_name: Unity Catalog schema name

        Returns:
            Latest effective_year as integer, or 2025 if query fails
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            result = spark.sql(f"""
                SELECT MAX(effective_year) as max_year
                FROM {catalog_name}.{schema_name}.hedis_measures_definitions
            """).collect()

            if result and result[0].max_year:
                return int(result[0].max_year)
        except Exception as e:
            print(f"Warning: Could not auto-detect effective_year: {e}")

        return 2025  # Fallback

    @staticmethod
    def create(
        *,
        endpoint_name: str,
        uc_function_names: Optional[Sequence[str]] = None,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        conn_string: Optional[str] = None,
        connection_pool: Optional[ConnectionPool] = None,
        enable_persistence: bool = False,
        effective_year: Optional[int] = None,
        databricks_function_client: Optional[DatabricksFunctionClient] = None,
    ) -> ChatAgent:
        """
        Build and return a HEDIS Chat Agent.

        Args:
            endpoint_name: Databricks model serving endpoint
            uc_function_names: UC function names for tools (default: measure_lookup, vector_search)
            catalog_name: Unity Catalog catalog name (for namespaced functions)
            schema_name: Unity Catalog schema name (for namespaced functions)
            conn_string: Database connection string (deprecated, use connection_pool)
            connection_pool: Database connection pool with auto-refresh (preferred)
            enable_persistence: Enable PostgreSQL checkpointing
            effective_year: HEDIS measurement year (optional, auto-detected if None)
            databricks_function_client: Optional Databricks function client

        Returns:
            HEDISChatAgent instance
        """
        # Validate persistence configuration
        if enable_persistence and not (conn_string or connection_pool):
            raise ValueError("Connection string or connection pool required when persistence is enabled")

        # Auto-detect effective_year if not provided
        if effective_year is None and catalog_name and schema_name:
            effective_year = HEDISChatAgentFactory._get_latest_effective_year(catalog_name, schema_name)
            print(f"Auto-detected effective_year: {effective_year}")
        elif effective_year is None:
            effective_year = 2025
            print(f"Using default effective_year: {effective_year}")

        # Wire UC function client
        client = databricks_function_client or DatabricksFunctionClient()
        set_uc_function_client(client)

        # Initialize LLM
        llm = ChatDatabricks(endpoint=endpoint_name)

        # Build fully-qualified function names if catalog/schema provided
        tool_names = list(uc_function_names or HEDISChatAgentFactory.DEFAULT_UC_FUNCTIONS)
        if catalog_name and schema_name:
            tool_names = [
                f"{catalog_name}.{schema_name}.{name}" if "." not in name else name
                for name in tool_names
            ]

        # Create UC toolkit
        uc_toolkit = UCFunctionToolkit(function_names=tool_names)
        tools = uc_toolkit.tools

        # Return agent
        return HEDISChatAgent(
            model=llm,
            tools=tools,
            conn_string=conn_string,
            connection_pool=connection_pool,
            enable_persistence=enable_persistence,
            effective_year=effective_year
        )

    @staticmethod
    def from_defaults(
        conn_string: Optional[str] = None,
        enable_persistence: bool = False,
        effective_year: Optional[int] = None
    ) -> ChatAgent:
        """
        Create HEDIS Chat Agent with default settings.

        Args:
            conn_string: Optional database connection string
            enable_persistence: Enable persistence (default: False)
            effective_year: HEDIS measurement year (optional, auto-detected if None)

        Returns:
            HEDISChatAgent with default configuration
        """
        endpoint = os.getenv("ENDPOINT_NAME", "databricks-meta-llama-3-3-70b-instruct")
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("UC_SCHEMA", "hedis_measurements")

        return HEDISChatAgentFactory.create(
            endpoint_name=endpoint,
            catalog_name=catalog,
            schema_name=schema,
            conn_string=conn_string,
            enable_persistence=enable_persistence,
            effective_year=effective_year
        )


# ============================================================================
# MLflow Model Setup - Module Level (runs on import)
# ============================================================================

# Get configuration from environment and model_config
endpoint_name = os.getenv("ENDPOINT_NAME", "databricks-meta-llama-3-3-70b-instruct")
catalog_name = os.getenv("UC_CATALOG", "main")
schema_name = os.getenv("UC_SCHEMA", "hedis_measurements")
effective_year_str = os.getenv("EFFECTIVE_YEAR")
effective_year = int(effective_year_str) if effective_year_str else None

# Try to read model_config for deployment configuration
try:
    model_config = mlflow.models.ModelConfig(development_config="model_config.yaml")
    enable_persistence = model_config.get("enable_persistence", False)
    lakebase_instance = model_config.get("lakebase_instance")
except Exception:
    # Model config not available, use defaults
    enable_persistence = False
    lakebase_instance = None

# Get connection pool if persistence enabled
# When deployed with DatabricksLakebase resource, use passthrough authentication
connection_pool = None
if enable_persistence and lakebase_instance:
    try:
        from src.database.lakebase import LakebaseDatabase
        from databricks.sdk import WorkspaceClient

        # Use passthrough authentication - credentials handled automatically
        w = WorkspaceClient()
        lakebase_db = LakebaseDatabase()  # Uses default authentication

        lakebase_db.initialize_connection(
            user=w.current_user.me().user_name,
            instance_name=lakebase_instance,
            setup_checkpointer=True
        )

        # Get connection pool with auto-refresh
        connection_pool = lakebase_db.get_connection_pool()
        print(f"Lakebase connection pool initialized via passthrough authentication")
    except Exception as e:
        print(f"Warning: Could not connect to Lakebase: {e}")
        print(f"Agent will run in stateless mode")
        enable_persistence = False

# Create agent
AGENT = HEDISChatAgentFactory.create(
    endpoint_name=endpoint_name,
    catalog_name=catalog_name,
    schema_name=schema_name,
    connection_pool=connection_pool,
    enable_persistence=enable_persistence,
    effective_year=effective_year
)

# Set as MLflow model - this runs on module import
mlflow.models.set_model(AGENT)

print(f"HEDIS Chat Agent initialized:")
print(f"  - Endpoint: {endpoint_name}")
print(f"  - Catalog: {catalog_name}")
print(f"  - Schema: {schema_name}")
print(f"  - Effective Year: {AGENT.effective_year}")
print(f"  - Persistence: {enable_persistence}")
if enable_persistence and lakebase_instance:
    print(f"  - Lakebase Instance: {lakebase_instance}")
