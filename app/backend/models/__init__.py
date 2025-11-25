"""
Pydantic Models Package

Contains all request/response schemas for the API.
"""

from app.backend.models.chat import (
    Message,
    MessageCreate,
    Chat,
    ChatCreate,
    ChatUpdate,
    ChatListItem,
    ChatListResponse,
    ChatMessageRequest,
    ChatMessageResponse,
    Pagination
)

from app.backend.models.review import (
    User,
    Review,
    ReviewCreate,
    ReviewUpdate,
    ReviewListItem,
    ReviewListResponse
)

__all__ = [
    "Message",
    "MessageCreate",
    "Chat",
    "ChatCreate",
    "ChatUpdate",
    "ChatListItem",
    "ChatListResponse",
    "ChatMessageRequest",
    "ChatMessageResponse",
    "Pagination",
    "User",
    "Review",
    "ReviewCreate",
    "ReviewUpdate",
    "ReviewListItem",
    "ReviewListResponse"
]
