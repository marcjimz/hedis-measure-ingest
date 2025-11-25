"""
Review Models

Pydantic models for review-related requests and responses.
Follows the API specifications for HEDIS chat application.
"""

from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, Field
import uuid

from backend.models.chat import Message


# Enums for review status
ReviewStatus = Literal["pending", "assigned", "resolved"]
AIFeedback = Literal["thumbs-up", "thumbs-down"]


class User(BaseModel):
    """User model for review assignments."""
    id: str
    name: str

    class Config:
        json_schema_extra = {
            "example": {
                "id": "user_123",
                "name": "Dr. Jane Smith"
            }
        }


class Review(BaseModel):
    """Complete review model."""
    id: str
    chatId: str
    status: ReviewStatus
    patient: Optional[str] = None
    requestedBy: User
    assignedTo: Optional[User] = None
    aiSuggestion: str
    aiSuggestionFeedback: Optional[AIFeedback] = None
    response: Optional[str] = None
    feedback: Optional[str] = None
    aiQualityRating: Optional[Literal[1, 2, 3, 4, 5]] = None
    chatContext: Optional[dict] = None  # Contains patient and messages
    createdAt: datetime
    updatedAt: datetime
    resolvedAt: Optional[datetime] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "review_123",
                "chatId": "chat_456",
                "status": "pending",
                "patient": "Patient_789",
                "requestedBy": {
                    "id": "user_123",
                    "name": "Dr. John Doe"
                },
                "assignedTo": None,
                "aiSuggestion": "Based on the HEDIS specifications...",
                "aiSuggestionFeedback": None,
                "response": None,
                "feedback": None,
                "aiQualityRating": None,
                "createdAt": "2025-01-15T14:00:00Z",
                "updatedAt": "2025-01-15T14:00:00Z",
                "resolvedAt": None
            }
        }


class ReviewCreate(BaseModel):
    """Model for creating a new review request."""
    chatId: str
    requestedBy: str  # User ID
    reason: Optional[str] = None
    aiSuggestion: str

    class Config:
        json_schema_extra = {
            "example": {
                "chatId": "chat_456",
                "requestedBy": "user_123",
                "reason": "Need expert validation on measure interpretation",
                "aiSuggestion": "Based on the HEDIS specifications for CWP..."
            }
        }


class ReviewUpdate(BaseModel):
    """Model for updating a review."""
    status: Optional[ReviewStatus] = None
    assignedTo: Optional[str] = None  # User ID
    aiSuggestionFeedback: Optional[AIFeedback] = None
    response: Optional[str] = None
    feedback: Optional[str] = None
    aiQualityRating: Optional[Literal[1, 2, 3, 4, 5]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "status": "assigned",
                "assignedTo": "user_456",
                "aiSuggestionFeedback": "thumbs-up"
            }
        }


class ReviewListItem(BaseModel):
    """Review summary for list views."""
    id: str
    chatId: str
    status: ReviewStatus
    patient: Optional[str] = None
    requestedBy: User
    assignedTo: Optional[User] = None
    aiSuggestion: str
    aiSuggestionFeedback: Optional[AIFeedback] = None
    createdAt: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": "review_123",
                "chatId": "chat_456",
                "status": "pending",
                "patient": "Patient_789",
                "requestedBy": {
                    "id": "user_123",
                    "name": "Dr. John Doe"
                },
                "assignedTo": None,
                "aiSuggestion": "Based on the HEDIS specifications...",
                "aiSuggestionFeedback": None,
                "createdAt": "2025-01-15T14:00:00Z"
            }
        }


class Pagination(BaseModel):
    """Pagination metadata."""
    page: int
    limit: int
    total: int
    totalPages: int


class ReviewListResponse(BaseModel):
    """Response model for review list endpoint."""
    reviews: List[ReviewListItem]
    pagination: Pagination

    class Config:
        json_schema_extra = {
            "example": {
                "reviews": [
                    {
                        "id": "review_123",
                        "chatId": "chat_456",
                        "status": "pending",
                        "patient": "Patient_789",
                        "requestedBy": {
                            "id": "user_123",
                            "name": "Dr. John Doe"
                        },
                        "assignedTo": None,
                        "aiSuggestion": "Based on the HEDIS...",
                        "aiSuggestionFeedback": None,
                        "createdAt": "2025-01-15T14:00:00Z"
                    }
                ],
                "pagination": {
                    "page": 1,
                    "limit": 20,
                    "total": 15,
                    "totalPages": 1
                }
            }
        }


class ChatContextForReview(BaseModel):
    """Chat context model included in review details."""
    patient: Optional[str] = None
    messages: List[Message] = []

    class Config:
        json_schema_extra = {
            "example": {
                "patient": "Patient_789",
                "messages": [
                    {
                        "role": "user",
                        "content": "What are the CWP requirements?",
                        "timestamp": "2025-01-15T14:00:00Z"
                    },
                    {
                        "role": "assistant",
                        "content": "The CWP measure...",
                        "timestamp": "2025-01-15T14:01:00Z"
                    }
                ]
            }
        }
