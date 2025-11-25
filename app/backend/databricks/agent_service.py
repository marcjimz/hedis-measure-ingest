"""
Agent Service

Service for interacting with the deployed HEDIS Chat Agent.
Handles chat completion requests and streaming responses.
"""

import logging
import requests
import json
from typing import List, Dict, Any, Optional, AsyncGenerator
from datetime import datetime
import uuid

from config import settings
from models.chat import Message

logger = logging.getLogger(__name__)


class AgentService:
    """
    Service for calling the deployed HEDIS Chat Agent.

    Supports both:
    1. Direct agent invocation (if running in same environment)
    2. HTTP calls to deployed agent endpoint
    """

    def __init__(self):
        """Initialize the agent service."""
        self.agent_endpoint = settings.agent_endpoint
        self.effective_year = settings.effective_year

        # Determine if we should use local or remote agent
        self.use_remote_agent = self.agent_endpoint is not None

        if self.use_remote_agent:
            logger.info(f"Agent service initialized with remote endpoint: {self.agent_endpoint}")
        else:
            logger.info("Agent service initialized with local agent")
            # Import local agent if needed
            try:
                from src.agents.hedis_chat import HEDISChatAgentFactory
                self.local_agent_factory = HEDISChatAgentFactory
                logger.info("Local agent factory imported successfully")
            except Exception as e:
                logger.error(f"Failed to import local agent: {e}")
                self.local_agent_factory = None

    async def chat_completion(
        self,
        messages: List[Message],
        chat_id: Optional[str] = None,
        patient_context: Optional[str] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """
        Get a chat completion from the agent.

        Args:
            messages: List of messages in the conversation
            chat_id: Optional chat ID for stateful conversations
            patient_context: Optional patient identifier
            stream: Whether to stream the response

        Returns:
            Dictionary with agent response
        """
        if self.use_remote_agent:
            return await self._remote_chat_completion(messages, chat_id, patient_context, stream)
        else:
            return await self._local_chat_completion(messages, chat_id, patient_context, stream)

    async def _remote_chat_completion(
        self,
        messages: List[Message],
        chat_id: Optional[str],
        patient_context: Optional[str],
        stream: bool
    ) -> Dict[str, Any]:
        """
        Call the remote agent endpoint.

        Args:
            messages: List of messages
            chat_id: Optional chat ID
            patient_context: Optional patient context
            stream: Whether to stream

        Returns:
            Agent response dictionary
        """
        try:
            # Build request payload
            payload = {
                "messages": [
                    {
                        "role": msg.role,
                        "content": msg.content,
                        "timestamp": msg.timestamp.isoformat()
                    }
                    for msg in messages
                ],
                "custom_inputs": {
                    "thread_id": chat_id,
                    "patient": patient_context
                }
            }

            # Make HTTP request to agent endpoint
            endpoint_url = f"{self.agent_endpoint}/predict"
            if stream:
                endpoint_url = f"{self.agent_endpoint}/predict_stream"

            headers = {
                "Content-Type": "application/json"
            }

            # Add authentication if needed
            if settings.databricks_token:
                headers["Authorization"] = f"Bearer {settings.databricks_token}"

            response = requests.post(
                endpoint_url,
                json=payload,
                headers=headers,
                stream=stream,
                timeout=120
            )
            response.raise_for_status()

            if stream:
                # Handle streaming response
                return {"stream": response.iter_lines()}
            else:
                # Handle non-streaming response
                result = response.json()
                return self._parse_agent_response(result)

        except Exception as e:
            logger.error(f"Error calling remote agent: {e}", exc_info=True)
            raise

    async def _local_chat_completion(
        self,
        messages: List[Message],
        chat_id: Optional[str],
        patient_context: Optional[str],
        stream: bool
    ) -> Dict[str, Any]:
        """
        Call the local agent directly.

        Args:
            messages: List of messages
            chat_id: Optional chat ID
            patient_context: Optional patient context
            stream: Whether to stream

        Returns:
            Agent response dictionary
        """
        try:
            if not self.local_agent_factory:
                raise RuntimeError("Local agent factory not available")

            # Create agent instance
            agent = self.local_agent_factory.create(
                endpoint_name=settings.llm_endpoint,
                catalog_name=settings.uc_catalog,
                schema_name=settings.uc_schema,
                enable_persistence=settings.postgres_enabled,
                effective_year=self.effective_year
            )

            # Convert messages to agent format
            from mlflow.types.agent import ChatAgentMessage
            agent_messages = [
                ChatAgentMessage(
                    role=msg.role,
                    content=msg.content
                )
                for msg in messages
            ]

            # Call agent
            custom_inputs = {"thread_id": chat_id}
            if patient_context:
                custom_inputs["patient"] = patient_context

            if stream:
                # Return streaming response
                response_stream = agent.predict_stream(
                    messages=agent_messages,
                    custom_inputs=custom_inputs
                )
                return {"stream": response_stream}
            else:
                # Return non-streaming response
                response = agent.predict(
                    messages=agent_messages,
                    custom_inputs=custom_inputs
                )
                return self._parse_agent_response(response)

        except Exception as e:
            logger.error(f"Error calling local agent: {e}", exc_info=True)
            raise

    def _parse_agent_response(self, response: Any) -> Dict[str, Any]:
        """
        Parse agent response into standard format.

        Args:
            response: Agent response object

        Returns:
            Dictionary with parsed response
        """
        try:
            # Handle MLflow agent response
            if hasattr(response, 'messages'):
                messages = response.messages
                custom_outputs = getattr(response, 'custom_outputs', {})

                # Extract the latest assistant message
                assistant_message = None
                for msg in reversed(messages):
                    if msg.role == "assistant":
                        assistant_message = msg
                        break

                if not assistant_message:
                    raise ValueError("No assistant message in response")

                return {
                    "content": assistant_message.content,
                    "role": assistant_message.role,
                    "message_id": getattr(assistant_message, 'id', str(uuid.uuid4())),
                    "timestamp": datetime.utcnow().isoformat(),
                    "thread_id": custom_outputs.get("thread_id"),
                    "metadata": custom_outputs
                }

            # Handle dictionary response
            elif isinstance(response, dict):
                return response

            else:
                raise ValueError(f"Unknown response type: {type(response)}")

        except Exception as e:
            logger.error(f"Error parsing agent response: {e}", exc_info=True)
            raise

    async def generate_ai_suggestion(
        self,
        chat_messages: List[Message],
        patient_context: Optional[str] = None
    ) -> str:
        """
        Generate an AI suggestion for a review request.

        Uses the agent to provide a suggested response that reviewers can evaluate.

        Args:
            chat_messages: Messages from the chat
            patient_context: Optional patient context

        Returns:
            AI-generated suggestion text
        """
        try:
            # Add a system message requesting a suggestion
            suggestion_messages = chat_messages + [
                Message(
                    role="system",
                    content="Based on this conversation, provide a comprehensive suggested response "
                            "that addresses the user's question about HEDIS measures. This will be "
                            "reviewed by a human expert."
                )
            ]

            # Get response from agent
            response = await self.chat_completion(
                messages=suggestion_messages,
                patient_context=patient_context,
                stream=False
            )

            return response.get("content", "No suggestion generated")

        except Exception as e:
            logger.error(f"Error generating AI suggestion: {e}", exc_info=True)
            return "Error generating AI suggestion"

    async def health_check(self) -> bool:
        """
        Check if the agent service is accessible.

        Returns:
            True if healthy, False otherwise
        """
        try:
            if self.use_remote_agent:
                # Check remote endpoint
                response = requests.get(
                    f"{self.agent_endpoint}/health",
                    timeout=10
                )
                return response.status_code == 200
            else:
                # Check local agent
                return self.local_agent_factory is not None
        except Exception as e:
            logger.error(f"Agent health check failed: {e}", exc_info=True)
            return False
