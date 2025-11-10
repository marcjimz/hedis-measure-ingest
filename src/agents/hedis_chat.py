"""
HEDIS Chat Agent

A production-ready LangGraph-based chat agent for HEDIS measure analysis with two modes:
1. QnA Mode: Answer questions about HEDIS measures using vector search and measure definitions
2. Compliance Mode: Evaluate patient encounters for HEDIS measure compliance

Features:
- Dual mode operation (QnA and Compliance)
- Intent detection and patient data extraction
- Integration with MeasureLookupTool and VectorSearchTool (Unity Catalog functions)
- Configurable persistence with PostgreSQL checkpointing
- Streaming and non-streaming support
- Comprehensive message handling and state management
"""

from typing import Any, Generator, Optional, Sequence, Union, List, Dict
import uuid
import os
import pathlib
import sys
import json
from psycopg import Connection

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
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from langgraph.checkpoint.postgres import PostgresSaver
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# Import HEDIS prompts
try:
    from src.agents.prompts.hedis import (
        HEDIS_CHAT_AGENT_SYSTEM_PROMPT,
        HEDIS_QNA_MODE_PROMPT,
        HEDIS_COMPLIANCE_MODE_PROMPT,
        HEDIS_INTENT_DETECTION_PROMPT,
        HEDIS_INTENT_DETECTION_SCHEMA,
        HEDIS_PATIENT_DATA_EXTRACTION_PROMPT,
        HEDIS_PATIENT_DATA_EXTRACTION_SCHEMA,
    )
except ImportError:
    # Fallback for different import contexts
    from agents.prompts.hedis import (
        HEDIS_CHAT_AGENT_SYSTEM_PROMPT,
        HEDIS_QNA_MODE_PROMPT,
        HEDIS_COMPLIANCE_MODE_PROMPT,
        HEDIS_INTENT_DETECTION_PROMPT,
        HEDIS_INTENT_DETECTION_SCHEMA,
        HEDIS_PATIENT_DATA_EXTRACTION_PROMPT,
        HEDIS_PATIENT_DATA_EXTRACTION_SCHEMA,
    )

mlflow.autolog()


class HEDISChatAgent(ChatAgent):
    """
    HEDIS Chat Agent with dual-mode operation (QnA and Compliance).

    Supports:
    - QnA Mode: Answer questions about HEDIS measures
    - Compliance Mode: Evaluate patient encounters for compliance
    - Configurable persistence with PostgreSQL
    - Streaming and non-streaming responses
    - Intent detection and patient data extraction
    """

    def __init__(
        self,
        model: LanguageModelLike,
        tools: Union[Sequence[BaseTool], ToolNode],
        conn_string: Optional[str] = None,
        enable_persistence: bool = False,
        effective_year: int = 2025,
        default_mode: str = "QNA_MODE"
    ):
        """
        Initialize HEDIS Chat Agent.

        Args:
            model: Language model for chat responses
            tools: Tools for measure lookup and vector search
            conn_string: Database connection string for persistence (optional)
            enable_persistence: Enable PostgreSQL checkpointing (default: False)
            effective_year: HEDIS measurement year (default: 2025)
            default_mode: Default operation mode - "QNA_MODE" or "COMPLIANCE_MODE"
        """
        self.model = model
        self.tools = tools
        self.conn_string = conn_string
        self.enable_persistence = enable_persistence
        self.effective_year = effective_year
        self.default_mode = default_mode
        self.thread_id = "default"

        # Validate configuration
        if enable_persistence and not conn_string:
            raise ValueError("Connection string required when persistence is enabled")

    def _convert_messages_to_dict(self, messages: List[Union[ChatAgentMessage, dict]]) -> List[dict]:
        """
        Convert messages to dict format, handling both ChatAgentMessage objects and dicts.
        """
        result = []
        for msg in messages:
            if isinstance(msg, dict):
                # Already a dict, ensure it has an ID
                if "id" not in msg:
                    msg["id"] = str(uuid.uuid4())
                result.append(msg)
            elif hasattr(msg, 'model_dump_compat'):
                # MLflow ChatAgentMessage with model_dump_compat method
                msg_dict = msg.model_dump_compat(exclude_none=True)
                if "id" not in msg_dict:
                    msg_dict["id"] = str(uuid.uuid4())
                result.append(msg_dict)
            elif hasattr(msg, 'model_dump'):
                # Pydantic v2 style
                msg_dict = msg.model_dump(exclude_none=True)
                if "id" not in msg_dict:
                    msg_dict["id"] = str(uuid.uuid4())
                result.append(msg_dict)
            elif hasattr(msg, 'dict'):
                # Pydantic v1 style
                msg_dict = msg.dict(exclude_none=True)
                if "id" not in msg_dict:
                    msg_dict["id"] = str(uuid.uuid4())
                result.append(msg_dict)
            else:
                # Fallback: try to extract role and content
                result.append({
                    "id": getattr(msg, 'id', str(uuid.uuid4())),
                    "role": getattr(msg, 'role', 'user'),
                    "content": getattr(msg, 'content', str(msg))
                })
        return result

    def _detect_intent(self, user_message: str) -> Dict[str, Any]:
        """
        Detect user intent to determine QnA vs Compliance mode.

        Args:
            user_message: User's message text

        Returns:
            Dict with mode, confidence, reasoning, measure_mentioned, patient_data_present
        """
        # Use a simple LLM call for intent detection
        prompt = HEDIS_INTENT_DETECTION_PROMPT.format(user_message=user_message)

        try:
            # Use structured output if available
            if hasattr(self.model, 'with_structured_output'):
                intent_model = self.model.with_structured_output(
                    HEDIS_INTENT_DETECTION_SCHEMA["schema"]
                )
                result = intent_model.invoke(prompt)
                return result
            else:
                # Fallback to regular completion with JSON parsing
                response = self.model.invoke(prompt)
                content = response.content if hasattr(response, 'content') else str(response)
                # Extract JSON from markdown code blocks if present
                if "```json" in content:
                    json_start = content.find("```json") + 7
                    json_end = content.find("```", json_start)
                    content = content[json_start:json_end].strip()
                elif "```" in content:
                    json_start = content.find("```") + 3
                    json_end = content.find("```", json_start)
                    content = content[json_start:json_end].strip()
                return json.loads(content)
        except Exception as e:
            # Default to QnA mode on error
            return {
                "mode": self.default_mode,
                "confidence": "low",
                "reasoning": f"Intent detection failed: {str(e)}. Using default mode.",
                "measure_mentioned": None,
                "patient_data_present": False
            }

    def _extract_patient_data(self, user_message: str) -> Dict[str, Any]:
        """
        Extract structured patient data from user message for compliance evaluation.

        Args:
            user_message: User's message containing patient encounter data

        Returns:
            Structured patient encounter data
        """
        prompt = HEDIS_PATIENT_DATA_EXTRACTION_PROMPT.format(user_message=user_message)

        try:
            # Use structured output if available
            if hasattr(self.model, 'with_structured_output'):
                extraction_model = self.model.with_structured_output(
                    HEDIS_PATIENT_DATA_EXTRACTION_SCHEMA["schema"]
                )
                result = extraction_model.invoke(prompt)
                return result
            else:
                # Fallback to regular completion with JSON parsing
                response = self.model.invoke(prompt)
                content = response.content if hasattr(response, 'content') else str(response)
                # Extract JSON from markdown code blocks if present
                if "```json" in content:
                    json_start = content.find("```json") + 7
                    json_end = content.find("```", json_start)
                    content = content[json_start:json_end].strip()
                elif "```" in content:
                    json_start = content.find("```") + 3
                    json_end = content.find("```", json_start)
                    content = content[json_start:json_end].strip()
                return json.loads(content)
        except Exception as e:
            # Return empty patient data structure on error
            return {
                "patient_demographics": {},
                "enrollment": {},
                "encounter_details": {},
                "clinical_codes": {},
                "diagnoses": [],
                "procedures": [],
                "exclusion_indicators": {},
                "measure_context": {},
                "extraction_error": str(e)
            }

    def _build_system_prompt(self, mode: str, intent_data: Optional[Dict] = None) -> str:
        """
        Build the system prompt based on mode and context.

        Args:
            mode: Operation mode (QNA_MODE or COMPLIANCE_MODE)
            intent_data: Optional intent detection data

        Returns:
            Formatted system prompt
        """
        base_prompt = HEDIS_CHAT_AGENT_SYSTEM_PROMPT.format(
            effective_year=self.effective_year
        )

        mode_context = ""
        if mode == "QNA_MODE":
            mode_context = "\n\n**CURRENT MODE: Question & Answer**\nYou are answering questions about HEDIS measures. Use vector search and measure lookup tools to find relevant information."
        elif mode == "COMPLIANCE_MODE":
            mode_context = "\n\n**CURRENT MODE: Compliance Determination**\nYou are evaluating a patient encounter for HEDIS measure compliance. Follow the step-by-step evaluation process."

        return base_prompt + mode_context

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

            # Handle tool calls
            tool_calls = None
            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                if isinstance(msg.tool_calls, list):
                    tool_calls = []
                    for tc in msg.tool_calls:
                        if hasattr(tc, 'dict'):
                            tool_calls.append(tc.dict())
                        elif isinstance(tc, dict):
                            tool_calls.append(tc)
                        else:
                            tool_calls.append({
                                'id': getattr(tc, 'id', str(uuid.uuid4())),
                                'type': getattr(tc, 'type', 'function'),
                                'function': getattr(tc, 'function', {})
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
        checkpointer: Optional[PostgresSaver] = None,
        system_prompt: Optional[str] = None
    ) -> CompiledStateGraph:
        """
        Create the LangGraph agent with optional checkpointing.

        Args:
            checkpointer: Optional PostgresSaver for persistence
            system_prompt: Optional system prompt override

        Returns:
            Compiled LangGraph state graph
        """
        # Bind tools to model
        model_with_tools = self.model.bind_tools(self.tools) if self.tools else self.model

        # Use default system prompt if not provided
        if system_prompt is None:
            system_prompt = self._build_system_prompt(self.default_mode)

        def should_continue(state: ChatAgentState):
            """Determine if we should continue to tools or end."""
            messages = state["messages"]
            last = messages[-1]

            if hasattr(last, 'tool_calls'):
                return "continue" if last.tool_calls else "end"
            elif isinstance(last, dict):
                return "continue" if last.get("tool_calls") else "end"
            else:
                additional_kwargs = getattr(last, 'additional_kwargs', {})
                return "continue" if additional_kwargs.get("tool_calls") else "end"

        def call_model(state: ChatAgentState, config: RunnableConfig):
            """Call the model with system prompt prepended."""
            messages_with_system = state["messages"]

            # Prepend system prompt if not already present
            if system_prompt:
                if not messages_with_system or messages_with_system[0].get("role") != "system":
                    messages_with_system = [
                        {"role": "system", "content": system_prompt}
                    ] + messages_with_system

            response = model_with_tools.invoke(messages_with_system, config)
            return {"messages": [response]}

        # Build the graph
        workflow = StateGraph(ChatAgentState)
        workflow.add_node("agent", call_model)

        if self.tools:
            # Add tools node
            workflow.add_node("tools", ChatAgentToolNode(self.tools))
            workflow.set_entry_point("agent")

            # Agent -> Tools (conditionally) -> Agent
            workflow.add_conditional_edges(
                "agent",
                should_continue,
                {"continue": "tools", "end": END}
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
            custom_inputs: Optional custom inputs (thread_id, mode, etc.)

        Returns:
            ChatAgentResponse with messages and custom outputs
        """
        # Extract configuration from custom_inputs
        custom_inputs = custom_inputs or {}
        thread_id = custom_inputs.get("thread_id")
        force_mode = custom_inputs.get("mode")  # Optional mode override

        # Get the last user message for intent detection
        last_user_message = None
        for msg in reversed(messages):
            msg_dict = msg if isinstance(msg, dict) else msg.model_dump_compat()
            if msg_dict.get("role") == "user":
                last_user_message = msg_dict.get("content", "")
                break

        # Detect intent and determine mode
        if force_mode:
            mode = force_mode
            intent_data = {"mode": mode, "confidence": "high", "reasoning": "Mode forced by custom_inputs"}
        elif last_user_message:
            intent_data = self._detect_intent(last_user_message)
            mode = intent_data.get("mode", self.default_mode)
        else:
            mode = self.default_mode
            intent_data = {"mode": mode, "confidence": "low", "reasoning": "No user message found"}

        # Extract patient data if in compliance mode
        patient_data = None
        if mode == "COMPLIANCE_MODE" and last_user_message:
            patient_data = self._extract_patient_data(last_user_message)

        # Build system prompt for this mode
        system_prompt = self._build_system_prompt(mode, intent_data)

        # Handle persistence and threading
        if self.enable_persistence and self.conn_string:
            # Use persistent mode with checkpointing
            if thread_id:
                # Continuing conversation - send only new message
                messages_to_send = [messages[-1]] if messages else []
            else:
                # New conversation
                thread_id = str(uuid.uuid4())
                messages_to_send = messages

            config = {"configurable": {"thread_id": thread_id}}

            with Connection.connect(self.conn_string) as conn:
                checkpointer = PostgresSaver(conn)
                agent = self._create_agent_graph(checkpointer, system_prompt)

                converted_messages = self._convert_messages_to_dict(messages_to_send)
                result = agent.invoke({"messages": converted_messages}, config)

                # Parse output messages
                out_messages = []
                if result.get("messages"):
                    for msg in result["messages"]:
                        out_messages.append(self._parse_message(msg))
        else:
            # Non-persistent mode - no threading
            thread_id = thread_id or str(uuid.uuid4())
            agent = self._create_agent_graph(None, system_prompt)

            converted_messages = self._convert_messages_to_dict(messages)
            result = agent.invoke({"messages": converted_messages})

            # Parse output messages
            out_messages = []
            if result.get("messages"):
                for msg in result["messages"]:
                    out_messages.append(self._parse_message(msg))

        # Build custom outputs
        custom_outputs = {
            "thread_id": thread_id,
            "mode": mode,
            "intent_data": intent_data,
            "effective_year": self.effective_year,
            "persistence_enabled": self.enable_persistence
        }

        if patient_data:
            custom_outputs["patient_data"] = patient_data

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
            custom_inputs: Optional custom inputs (thread_id, mode, etc.)

        Yields:
            ChatAgentChunk objects with message deltas
        """
        # Extract configuration
        custom_inputs = custom_inputs or {}
        thread_id = custom_inputs.get("thread_id")
        force_mode = custom_inputs.get("mode")

        # Get last user message for intent detection
        last_user_message = None
        for msg in reversed(messages):
            msg_dict = msg if isinstance(msg, dict) else msg.model_dump_compat()
            if msg_dict.get("role") == "user":
                last_user_message = msg_dict.get("content", "")
                break

        # Detect intent and determine mode
        if force_mode:
            mode = force_mode
            intent_data = {"mode": mode, "confidence": "high", "reasoning": "Mode forced by custom_inputs"}
        elif last_user_message:
            intent_data = self._detect_intent(last_user_message)
            mode = intent_data.get("mode", self.default_mode)
        else:
            mode = self.default_mode
            intent_data = {"mode": mode, "confidence": "low", "reasoning": "No user message found"}

        # Extract patient data if in compliance mode
        patient_data = None
        if mode == "COMPLIANCE_MODE" and last_user_message:
            patient_data = self._extract_patient_data(last_user_message)

        # Build system prompt
        system_prompt = self._build_system_prompt(mode, intent_data)

        # Handle persistence and threading
        if self.enable_persistence and self.conn_string:
            if thread_id:
                messages_to_send = [messages[-1]] if messages else []
            else:
                thread_id = str(uuid.uuid4())
                messages_to_send = messages

            config = {"configurable": {"thread_id": thread_id}}

            with Connection.connect(self.conn_string) as conn:
                checkpointer = PostgresSaver(conn)
                agent = self._create_agent_graph(checkpointer, system_prompt)

                converted_messages = self._convert_messages_to_dict(messages_to_send)

                for chunk in agent.stream({"messages": converted_messages}, config, stream_mode="values"):
                    if chunk.get("messages"):
                        for msg in chunk["messages"]:
                            parsed_msg = self._parse_message(msg)
                            yield ChatAgentChunk(delta=parsed_msg.__dict__)
        else:
            # Non-persistent streaming
            thread_id = thread_id or str(uuid.uuid4())
            agent = self._create_agent_graph(None, system_prompt)

            converted_messages = self._convert_messages_to_dict(messages)

            for chunk in agent.stream({"messages": converted_messages}, stream_mode="values"):
                if chunk.get("messages"):
                    for msg in chunk["messages"]:
                        parsed_msg = self._parse_message(msg)
                        yield ChatAgentChunk(delta=parsed_msg.__dict__)

        # Yield final metadata chunk
        custom_outputs = {
            "thread_id": thread_id,
            "mode": mode,
            "intent_data": intent_data,
            "effective_year": self.effective_year,
            "persistence_enabled": self.enable_persistence
        }

        if patient_data:
            custom_outputs["patient_data"] = patient_data

        yield ChatAgentChunk(delta={"custom_outputs": custom_outputs})


# ============================================================================
# Factory for Creating HEDIS Chat Agent
# ============================================================================

class HEDISChatAgentFactory:
    """
    Factory for building HEDIS Chat Agent with Unity Catalog tools.
    """

    # Default UC functions for HEDIS operations
    DEFAULT_UC_FUNCTIONS = [
        "measure_definition_lookup",  # Tool for looking up measure definitions
        "measures_document_search"     # Tool for semantic search over HEDIS chunks
    ]

    @staticmethod
    def create(
        *,
        endpoint_name: str,
        uc_function_names: Optional[Sequence[str]] = None,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        conn_string: Optional[str] = None,
        enable_persistence: bool = False,
        effective_year: int = 2025,
        default_mode: str = "QNA_MODE",
        databricks_function_client: Optional[DatabricksFunctionClient] = None,
    ) -> ChatAgent:
        """
        Build and return a HEDIS Chat Agent.

        Args:
            endpoint_name: Databricks model serving endpoint
            uc_function_names: UC function names for tools (default: measure_lookup, vector_search)
            catalog_name: Unity Catalog catalog name (for namespaced functions)
            schema_name: Unity Catalog schema name (for namespaced functions)
            conn_string: Database connection string (required if enable_persistence=True)
            enable_persistence: Enable PostgreSQL checkpointing
            effective_year: HEDIS measurement year
            default_mode: Default operation mode (QNA_MODE or COMPLIANCE_MODE)
            databricks_function_client: Optional Databricks function client

        Returns:
            HEDISChatAgent instance
        """
        # Validate persistence configuration
        if enable_persistence and not conn_string:
            raise ValueError("Connection string required when persistence is enabled")

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
            enable_persistence=enable_persistence,
            effective_year=effective_year,
            default_mode=default_mode
        )

    @staticmethod
    def from_defaults(
        conn_string: Optional[str] = None,
        enable_persistence: bool = False,
        effective_year: int = 2025
    ) -> ChatAgent:
        """
        Create HEDIS Chat Agent with default settings.

        Args:
            conn_string: Optional database connection string
            enable_persistence: Enable persistence (default: False)
            effective_year: HEDIS measurement year (default: 2025)

        Returns:
            HEDISChatAgent with default configuration
        """
        endpoint = os.getenv("ENDPOINT_NAME", "databricks-meta-llama-3-3-70b-instruct")
        catalog = os.getenv("UC_CATALOG", "main")
        schema = os.getenv("UC_SCHEMA", "hedis_pipeline")

        return HEDISChatAgentFactory.create(
            endpoint_name=endpoint,
            catalog_name=catalog,
            schema_name=schema,
            conn_string=conn_string,
            enable_persistence=enable_persistence,
            effective_year=effective_year,
            default_mode="QNA_MODE"
        )


# ============================================================================
# MLflow Model Setup
# ============================================================================

# Create agent instance for MLflow deployment
if __name__ == "__main__":
    # Get configuration from environment
    endpoint_name = os.getenv("ENDPOINT_NAME", "databricks-meta-llama-3-3-70b-instruct")
    catalog_name = os.getenv("UC_CATALOG", "main")
    schema_name = os.getenv("UC_SCHEMA", "hedis_pipeline")
    effective_year = int(os.getenv("EFFECTIVE_YEAR", "2025"))
    enable_persistence = os.getenv("ENABLE_PERSISTENCE", "false").lower() == "true"

    # Get connection string if persistence enabled
    conn_string = None
    if enable_persistence:
        conn_string = os.getenv("DB_CONNECTION_STRING")
        if not conn_string:
            # Try to initialize from LakebaseDatabase
            try:
                from src.database.lakebase import LakebaseDatabase
                lakebase_db = LakebaseDatabase(host=os.getenv("DATABRICKS_HOST"))
                conn_string = lakebase_db.initialize_connection(
                    user=os.getenv("DATABRICKS_CLIENT_ID"),
                    instance_name=os.getenv("LAKEBASE_INSTANCE")
                )
            except ImportError:
                print("Warning: LakebaseDatabase not available and no DB_CONNECTION_STRING provided")

    # Create agent
    AGENT = HEDISChatAgentFactory.create(
        endpoint_name=endpoint_name,
        catalog_name=catalog_name,
        schema_name=schema_name,
        conn_string=conn_string,
        enable_persistence=enable_persistence,
        effective_year=effective_year,
        default_mode="QNA_MODE"
    )

    # Set as MLflow model
    mlflow.models.set_model(AGENT)

    print(f"HEDIS Chat Agent initialized:")
    print(f"  - Endpoint: {endpoint_name}")
    print(f"  - Catalog: {catalog_name}")
    print(f"  - Schema: {schema_name}")
    print(f"  - Effective Year: {effective_year}")
    print(f"  - Persistence: {enable_persistence}")
    print(f"  - Mode: QNA_MODE (with automatic intent detection)")
