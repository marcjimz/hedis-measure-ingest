"""
Mock Chat History Manager

In-memory implementation for local testing without Databricks.
Stores all data in dictionaries for quick local development.
"""

import uuid
from typing import Optional, List, Dict
from datetime import datetime

from backend.models.chat import (
    Chat,
    ChatCreate,
    ChatUpdate,
    ChatListResponse,
    ChatListItem,
    Message,
    Pagination,
    ChatStatus
)
from backend.models.review import (
    Review,
    ReviewCreate,
    ReviewUpdate,
    ReviewListResponse
)


class MockChatHistoryManager:
    """In-memory mock implementation of chat history manager."""

    def __init__(self):
        """Initialize with empty in-memory storage."""
        self.chats: Dict[str, Dict] = {}
        self.messages: Dict[str, List[Dict]] = {}  # chat_id -> messages
        self.reviews: Dict[str, Dict] = {}

        # Add some sample data
        self._initialize_sample_data()

    def _initialize_sample_data(self):
        """Add sample chats for testing."""
        # Sample chat 1
        chat1_id = "chat_001"
        self.chats[chat1_id] = {
            "id": chat1_id,
            "userId": "user_001",
            "title": "Question about BCS measure",
            "patient": "Patient_123",
            "status": "active",
            "createdAt": "2025-01-15T10:00:00Z",
            "updatedAt": "2025-01-15T10:05:00Z",
            "deleted": False
        }
        self.messages[chat1_id] = [
            {
                "id": "msg_001",
                "role": "user",
                "content": "What are the criteria for the BCS measure?",
                "timestamp": "2025-01-15T10:00:00Z"
            },
            {
                "id": "msg_002",
                "role": "assistant",
                "content": "The BCS (Breast Cancer Screening) measure evaluates the percentage of women aged 50-74 who had a mammogram to screen for breast cancer within the past 2 years.",
                "timestamp": "2025-01-15T10:00:05Z"
            }
        ]

        # Sample chat 2
        chat2_id = "chat_002"
        self.chats[chat2_id] = {
            "id": chat2_id,
            "userId": "user_001",
            "title": "Diabetes screening question",
            "patient": None,
            "status": "active",
            "createdAt": "2025-01-16T14:30:00Z",
            "updatedAt": "2025-01-16T14:35:00Z",
            "deleted": False
        }
        self.messages[chat2_id] = [
            {
                "id": "msg_003",
                "role": "user",
                "content": "What is the HBD measure?",
                "timestamp": "2025-01-16T14:30:00Z"
            },
            {
                "id": "msg_004",
                "role": "assistant",
                "content": "The HBD (Hemoglobin A1c Control for Patients With Diabetes) measure assesses diabetes management by tracking HbA1c testing and control levels.",
                "timestamp": "2025-01-16T14:30:10Z"
            }
        ]

    async def create_chat(
        self,
        user_id: str,
        patient: Optional[str] = None,
        title: str = "New Chat"
    ) -> str:
        """Create a new chat session."""
        chat_id = f"chat_{uuid.uuid4().hex[:8]}"
        now = datetime.utcnow().isoformat() + "Z"

        self.chats[chat_id] = {
            "id": chat_id,
            "userId": user_id,
            "title": title,
            "patient": patient,
            "status": "active",
            "createdAt": now,
            "updatedAt": now,
            "deleted": False
        }
        self.messages[chat_id] = []

        return chat_id

    async def get_chat(self, chat_id: str) -> Optional[Chat]:
        """Get a chat by ID with all messages."""
        if chat_id not in self.chats or self.chats[chat_id]["deleted"]:
            return None

        chat_data = self.chats[chat_id]
        messages = [
            Message(**msg)
            for msg in self.messages.get(chat_id, [])
        ]

        return Chat(
            id=chat_data["id"],
            userId=chat_data["userId"],
            title=chat_data["title"],
            patient=chat_data.get("patient"),
            status=chat_data["status"],
            createdAt=chat_data["createdAt"],
            updatedAt=chat_data["updatedAt"],
            messages=messages,
            messageCount=len(messages)
        )

    async def list_chats(
        self,
        user_id: str,
        status: Optional[ChatStatus] = None,
        page: int = 1,
        limit: int = 20
    ) -> ChatListResponse:
        """List chats for a user with pagination."""
        # Filter chats
        filtered = [
            chat for chat in self.chats.values()
            if chat["userId"] == user_id
            and not chat["deleted"]
            and (status is None or chat["status"] == status)
        ]

        # Sort by updated time (most recent first)
        filtered.sort(key=lambda x: x["updatedAt"], reverse=True)

        # Paginate
        total = len(filtered)
        start = (page - 1) * limit
        end = start + limit
        page_items = filtered[start:end]

        # Convert to ChatListItem
        chat_items = []
        for chat in page_items:
            messages = self.messages.get(chat["id"], [])
            last_message = messages[-1]["content"] if messages else ""
            last_message_time = messages[-1]["timestamp"] if messages else chat["createdAt"]

            chat_items.append(ChatListItem(
                id=chat["id"],
                title=chat["title"],
                patient=chat.get("patient"),
                status=chat["status"],
                lastMessage=last_message,
                lastMessageTime=last_message_time,
                createdAt=chat["createdAt"],
                messageCount=len(messages)
            ))

        return ChatListResponse(
            chats=chat_items,
            pagination=Pagination(
                page=page,
                limit=limit,
                total=total,
                totalPages=(total + limit - 1) // limit
            )
        )

    async def update_chat(self, chat_id: str, update: ChatUpdate) -> bool:
        """Update chat metadata."""
        if chat_id not in self.chats or self.chats[chat_id]["deleted"]:
            return False

        chat = self.chats[chat_id]

        if update.title is not None:
            chat["title"] = update.title
        if update.patient is not None:
            chat["patient"] = update.patient
        if update.status is not None:
            chat["status"] = update.status

        chat["updatedAt"] = datetime.utcnow().isoformat() + "Z"
        return True

    async def delete_chat(self, chat_id: str) -> bool:
        """Delete a chat (soft delete)."""
        if chat_id not in self.chats:
            return False

        self.chats[chat_id]["deleted"] = True
        self.chats[chat_id]["updatedAt"] = datetime.utcnow().isoformat() + "Z"
        return True

    async def save_message(
        self,
        chat_id: str,
        role: str,
        content: str,
        timestamp: datetime
    ) -> str:
        """Save a message to a chat."""
        if chat_id not in self.chats:
            raise ValueError(f"Chat {chat_id} not found")

        message_id = f"msg_{uuid.uuid4().hex[:8]}"

        message = {
            "id": message_id,
            "role": role,
            "content": content,
            "timestamp": timestamp.isoformat() + "Z"
        }

        if chat_id not in self.messages:
            self.messages[chat_id] = []

        self.messages[chat_id].append(message)

        # Update chat timestamp
        self.chats[chat_id]["updatedAt"] = timestamp.isoformat() + "Z"

        return message_id

    async def get_chat_history(self, chat_id: str) -> List[Message]:
        """Get all messages for a chat."""
        if chat_id not in self.messages:
            return []

        return [Message(**msg) for msg in self.messages[chat_id]]

    # Review methods (simplified for mock)

    async def create_review(self, review: ReviewCreate) -> str:
        """Create a new review."""
        review_id = f"review_{uuid.uuid4().hex[:8]}"
        now = datetime.utcnow().isoformat() + "Z"

        self.reviews[review_id] = {
            "id": review_id,
            "chatId": review.chatId,
            "status": review.status or "pending",
            "requestedBy": {"id": review.requestedBy, "name": "User"},
            "assignedTo": None,
            "aiSuggestion": review.aiSuggestion,
            "aiSuggestionFeedback": None,
            "createdAt": now,
            "updatedAt": now
        }

        return review_id

    async def get_review(self, review_id: str) -> Optional[Dict]:
        """Get a review by ID."""
        return self.reviews.get(review_id)

    async def list_reviews(
        self,
        status: Optional[str] = None,
        assigned_to: Optional[str] = None,
        page: int = 1,
        limit: int = 20
    ) -> ReviewListResponse:
        """List reviews with filtering."""
        # Simple mock - return empty list
        return ReviewListResponse(
            reviews=[],
            pagination=Pagination(
                page=page,
                limit=limit,
                total=0,
                totalPages=0
            )
        )

    async def update_review(self, review_id: str, update: ReviewUpdate) -> bool:
        """Update a review."""
        if review_id not in self.reviews:
            return False

        review = self.reviews[review_id]

        if update.status is not None:
            review["status"] = update.status
        if update.assignedTo is not None:
            review["assignedTo"] = {"id": update.assignedTo, "name": "Reviewer"}
        if update.aiSuggestionFeedback is not None:
            review["aiSuggestionFeedback"] = update.aiSuggestionFeedback

        review["updatedAt"] = datetime.utcnow().isoformat() + "Z"
        return True
