"""
Mock Agent Service

Mock implementation that returns fake AI responses for local testing.
"""

import asyncio
from typing import List, Dict, Any, Optional
from models.chat import Message


class MockAgentService:
    """Mock agent service that returns fake HEDIS responses."""

    MOCK_RESPONSES = [
        "Based on HEDIS 2025 guidelines, the BCS (Breast Cancer Screening) measure evaluates the percentage of women aged 50-74 who had a mammogram to screen for breast cancer within the past 2 years.",
        "The COL (Colorectal Cancer Screening) measure assesses whether adults aged 45-75 received appropriate colorectal cancer screening. Acceptable screening methods include colonoscopy, FIT test, or CT colonography.",
        "The HBD (Hemoglobin A1c Control for Patients With Diabetes) measure evaluates diabetes management. It tracks whether patients with diabetes had their HbA1c tested and whether their HbA1c levels are adequately controlled.",
        "According to HEDIS specifications, the initial population for this measure includes all enrolled members who meet the age criteria during the measurement year.",
        "Exclusions for this measure include members with specific medical conditions or documented refusals. Please refer to the detailed specifications for the complete list of exclusions."
    ]

    def __init__(self):
        """Initialize mock agent service."""
        self.response_index = 0

    async def chat_completion(
        self,
        messages: List[Message],
        chat_id: str,
        patient_context: Optional[str] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """
        Generate a mock AI response.

        Args:
            messages: Chat history
            chat_id: Chat identifier
            patient_context: Optional patient context
            stream: Whether to stream the response

        Returns:
            Response dict with content or stream
        """
        # Get the last user message
        user_message = None
        for msg in reversed(messages):
            if msg.role == "user":
                user_message = msg.content
                break

        # Generate context-aware response
        response_content = self._generate_response(user_message)

        if stream:
            # Return a mock stream
            return {
                "stream": self._create_mock_stream(response_content)
            }
        else:
            # Return complete response
            return {
                "content": response_content,
                "model": "mock-model",
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150
                }
            }

    def _generate_response(self, user_message: Optional[str]) -> str:
        """Generate a contextual response based on user message."""
        if not user_message:
            return self.MOCK_RESPONSES[0]

        user_message_lower = user_message.lower()

        # Match keywords to appropriate responses
        if "bcs" in user_message_lower or "breast cancer" in user_message_lower:
            return self.MOCK_RESPONSES[0]
        elif "col" in user_message_lower or "colorectal" in user_message_lower:
            return self.MOCK_RESPONSES[1]
        elif "hbd" in user_message_lower or "diabetes" in user_message_lower or "a1c" in user_message_lower:
            return self.MOCK_RESPONSES[2]
        elif "population" in user_message_lower or "initial" in user_message_lower:
            return self.MOCK_RESPONSES[3]
        elif "exclusion" in user_message_lower:
            return self.MOCK_RESPONSES[4]
        else:
            # Rotate through responses
            response = self.MOCK_RESPONSES[self.response_index % len(self.MOCK_RESPONSES)]
            self.response_index += 1
            return response

    def _create_mock_stream(self, content: str):
        """Create a mock streaming generator."""
        class MockChunk:
            def __init__(self, content_chunk: str):
                self.delta = {"content": content_chunk}

        # Split content into words for streaming
        words = content.split()
        for word in words:
            yield MockChunk(word + " ")

    async def generate_ai_suggestion(
        self,
        chat_history: List[Message],
        context: Optional[str] = None
    ) -> str:
        """Generate an AI suggestion for a review."""
        return "Based on the conversation, the AI response appears to be accurate according to HEDIS 2025 specifications. The information provided aligns with the measure definitions and criteria."
