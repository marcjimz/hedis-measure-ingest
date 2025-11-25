"""
Chat Models

Pydantic models for chat-related requests and responses.
Follows the API specifications for HEDIS chat application.
"""

from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, Field
import uuid


# Enums for chat status
ChatStatus = Literal["active", "under_review", "completed", "returned"]
MessageRole = Literal["user", "assistant", "system", "tool"]


class Message(BaseModel):
    """Message model."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    role: MessageRole
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    name: Optional[str] = None
    tool_calls: Optional[List[dict]] = None
    tool_call_id: Optional[str] = None
    attachments: Optional[List[dict]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "msg_123",
                "role": "assistant",
                "content": "The CWP measure stands for Comprehensive Diabetes Care...",
                "timestamp": "2025-01-15T14:30:00Z"
            }
        }


class MessageCreate(BaseModel):
    """Model for creating a new message."""
    role: MessageRole
    content: str
    name: Optional[str] = None


class Chat(BaseModel):
    """Complete chat model with messages."""
    id: str
    userId: str
    title: str
    patient: Optional[str] = None
    status: ChatStatus = "active"
    messages: List[Message] = []
    createdAt: datetime
    updatedAt: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": "chat_123",
                "userId": "user_456",
                "title": "Question about CWP measure",
                "patient": "Patient_789",
                "status": "active",
                "messages": [],
                "createdAt": "2025-01-15T14:00:00Z",
                "updatedAt": "2025-01-15T14:30:00Z"
            }
        }


class ChatCreate(BaseModel):
    """Model for creating a new chat."""
    userId: str
    patient: Optional[str] = None
    title: Optional[str] = "New Chat"

    class Config:
        json_schema_extra = {
            "example": {
                "userId": "user_456",
                "patient": "Patient_789",
                "title": "Question about diabetes care"
            }
        }


class ChatUpdate(BaseModel):
    """Model for updating chat metadata."""
    title: Optional[str] = None
    patient: Optional[str] = None
    status: Optional[ChatStatus] = None

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Updated title",
                "status": "under_review"
            }
        }


class ChatListItem(BaseModel):
    """Chat summary for list views."""
    id: str
    title: str
    patient: Optional[str] = None
    status: ChatStatus
    lastMessage: str
    lastMessageTime: datetime
    createdAt: datetime
    messageCount: int

    class Config:
        json_schema_extra = {
            "example": {
                "id": "chat_123",
                "title": "Question about CWP",
                "patient": "Patient_789",
                "status": "active",
                "lastMessage": "Thank you for the explanation...",
                "lastMessageTime": "2025-01-15T14:30:00Z",
                "createdAt": "2025-01-15T14:00:00Z",
                "messageCount": 5
            }
        }


class Pagination(BaseModel):
    """Pagination metadata."""
    page: int
    limit: int
    total: int
    totalPages: int

    class Config:
        json_schema_extra = {
            "example": {
                "page": 1,
                "limit": 20,
                "total": 45,
                "totalPages": 3
            }
        }


class ChatListResponse(BaseModel):
    """Response model for chat list endpoint."""
    chats: List[ChatListItem]
    pagination: Pagination

    class Config:
        json_schema_extra = {
            "example": {
                "chats": [
                    {
                        "id": "chat_123",
                        "title": "Question about CWP",
                        "patient": "Patient_789",
                        "status": "active",
                        "lastMessage": "Thank you...",
                        "lastMessageTime": "2025-01-15T14:30:00Z",
                        "createdAt": "2025-01-15T14:00:00Z",
                        "messageCount": 5
                    }
                ],
                "pagination": {
                    "page": 1,
                    "limit": 20,
                    "total": 45,
                    "totalPages": 3
                }
            }
        }


class ChatContext(BaseModel):
    """Context information for a chat message."""
    patient: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "patient": "Patient_789"
            }
        }


class ChatMessageRequest(BaseModel):
    """Request model for sending a chat message."""
    chatId: Optional[str] = None
    message: str
    context: ChatContext = Field(default_factory=ChatContext)

    class Config:
        json_schema_extra = {
            "example": {
                "chatId": "chat_123",
                "message": "What are the requirements for the CWP measure?",
                "context": {
                    "patient": "Patient_789"
                }
            }
        }


class ChatMessageResponse(BaseModel):
    """Response model for chat message endpoint."""
    chatId: str
    userMessage: Message
    assistantMessage: Message

    class Config:
        json_schema_extra = {
            "example": {
                "chatId": "chat_123",
                "userMessage": {
                    "id": "msg_user_1",
                    "role": "user",
                    "content": "What are the requirements for CWP?",
                    "timestamp": "2025-01-15T14:30:00Z"
                },
                "assistantMessage": {
                    "id": "msg_asst_1",
                    "role": "assistant",
                    "content": "The CWP measure...",
                    "timestamp": "2025-01-15T14:30:05Z"
                }
            }
        }
