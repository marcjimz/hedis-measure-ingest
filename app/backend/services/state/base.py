"""
Abstract Base Class for Chat History State Management

This module defines the abstract interface for managing chat history, messages, and reviews.
It uses Pydantic models for type safety and validation, and defines CRUD operations for
multi-turn conversation management.

Design Patterns Used:
- Abstract Base Class (ABC): Defines interface contract for implementations
- Repository Pattern: Encapsulates data access logic
- Factory Method Pattern: Subclasses implement storage-specific operations
- Data Transfer Objects (DTOs): Pydantic models for type-safe data transfer

Future Implementations:
To create a new state management backend (e.g., LakebaseStateManager):

1. Create a new file (e.g., lakebase.py) in this directory
2. Import the abstract base class and models:
   ```python
   from app.backend.services.state.base import (
       ChatHistoryStateManager,
       ChatModel,
       MessageModel,
       ReviewModel,
       PaginatedResponse
   )
   ```
3. Implement all abstract methods with your storage backend logic
4. Handle connection management in __init__
5. Implement proper error handling and logging
6. Add backend-specific optimizations (e.g., indexes, caching)
7. Register your implementation in __init__.py

Example:
```python
class LakebaseStateManager(ChatHistoryStateManager):
    def __init__(self, connection_pool):
        self.pool = connection_pool

    async def create_chat(self, chat: ChatCreate) -> ChatModel:
        # Your Lakebase PostgreSQL implementation
        pass
```
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# Enumerations
# ============================================================================

class ChatStatus(str, Enum):
    """Status of a chat conversation."""
    ACTIVE = "active"
    COMPLETED = "completed"
    ARCHIVED = "archived"
    DELETED = "deleted"


class MessageRole(str, Enum):
    """Role of a message sender."""
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class ReviewStatus(str, Enum):
    """Status of a chat review."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVISION = "needs_revision"


# ============================================================================
# Pydantic Models - Data Transfer Objects
# ============================================================================

class ChatBase(BaseModel):
    """Base model for chat conversations."""
    user_id: str = Field(..., description="User identifier")
    title: Optional[str] = Field(None, max_length=255, description="Chat title")
    patient: Optional[str] = Field(None, max_length=255, description="Patient identifier or name")
    status: ChatStatus = Field(default=ChatStatus.ACTIVE, description="Chat status")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "user123",
                "title": "HEDIS Measure Inquiry - BCS",
                "patient": "Patient-12345",
                "status": "active",
                "metadata": {"source": "web_app", "session_id": "abc-123"}
            }
        }
    )


class ChatCreate(ChatBase):
    """Model for creating a new chat."""
    pass


class ChatUpdate(BaseModel):
    """Model for updating an existing chat."""
    title: Optional[str] = Field(None, max_length=255)
    patient: Optional[str] = Field(None, max_length=255)
    status: Optional[ChatStatus] = None
    metadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "title": "Updated HEDIS Measure Inquiry",
                "status": "completed"
            }
        }
    )


class ChatModel(ChatBase):
    """Complete chat model with database fields."""
    id: UUID = Field(default_factory=uuid4, description="Unique chat identifier")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "user_id": "user123",
                "title": "HEDIS Measure Inquiry - BCS",
                "patient": "Patient-12345",
                "status": "active",
                "metadata": {"source": "web_app"},
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-01-15T10:30:00Z"
            }
        }
    )


class MessageBase(BaseModel):
    """Base model for chat messages."""
    chat_id: UUID = Field(..., description="Parent chat identifier")
    role: MessageRole = Field(..., description="Message sender role")
    content: str = Field(..., description="Message content")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "chat_id": "550e8400-e29b-41d4-a716-446655440000",
                "role": "user",
                "content": "What are the criteria for BCS measure?",
                "metadata": {"token_count": 10}
            }
        }
    )


class MessageCreate(MessageBase):
    """Model for creating a new message."""
    pass


class MessageModel(MessageBase):
    """Complete message model with database fields."""
    id: UUID = Field(default_factory=uuid4, description="Unique message identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message timestamp")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "660e8400-e29b-41d4-a716-446655440001",
                "chat_id": "550e8400-e29b-41d4-a716-446655440000",
                "role": "user",
                "content": "What are the criteria for BCS measure?",
                "metadata": {"token_count": 10},
                "timestamp": "2025-01-15T10:30:05Z"
            }
        }
    )


class ReviewBase(BaseModel):
    """Base model for chat reviews."""
    chat_id: UUID = Field(..., description="Chat being reviewed")
    status: ReviewStatus = Field(default=ReviewStatus.PENDING, description="Review status")
    assigned_to: Optional[str] = Field(None, description="Reviewer user ID")
    ai_suggestion: Optional[str] = Field(None, description="AI-generated suggestion")
    reviewer_notes: Optional[str] = Field(None, description="Reviewer's notes")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "chat_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "assigned_to": "reviewer@example.com",
                "ai_suggestion": "Response accuracy: 95%, suggest approval",
                "reviewer_notes": None,
                "metadata": {"priority": "high"}
            }
        }
    )


class ReviewCreate(ReviewBase):
    """Model for creating a new review."""
    pass


class ReviewUpdate(BaseModel):
    """Model for updating an existing review."""
    status: Optional[ReviewStatus] = None
    assigned_to: Optional[str] = None
    reviewer_notes: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "approved",
                "reviewer_notes": "Verified accuracy with HEDIS 2025 guidelines"
            }
        }
    )


class ReviewModel(ReviewBase):
    """Complete review model with database fields."""
    id: UUID = Field(default_factory=uuid4, description="Unique review identifier")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "770e8400-e29b-41d4-a716-446655440002",
                "chat_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "approved",
                "assigned_to": "reviewer@example.com",
                "ai_suggestion": "Response accuracy: 95%, suggest approval",
                "reviewer_notes": "Verified accuracy with HEDIS 2025 guidelines",
                "metadata": {"priority": "high"},
                "created_at": "2025-01-15T10:35:00Z",
                "updated_at": "2025-01-15T10:40:00Z"
            }
        }
    )


# ============================================================================
# Pagination Support
# ============================================================================

T = TypeVar('T')


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response model."""
    items: List[T] = Field(..., description="List of items in current page")
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, le=1000, description="Number of items per page")
    total_pages: int = Field(..., description="Total number of pages")

    @classmethod
    def create(cls, items: List[T], total: int, page: int, page_size: int) -> "PaginatedResponse[T]":
        """
        Create a paginated response.

        Args:
            items: List of items for current page
            total: Total number of items across all pages
            page: Current page number (1-indexed)
            page_size: Number of items per page

        Returns:
            PaginatedResponse instance
        """
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        return cls(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "items": [{"id": "uuid1"}, {"id": "uuid2"}],
                "total": 100,
                "page": 1,
                "page_size": 20,
                "total_pages": 5
            }
        }
    )


# ============================================================================
# Filter Models
# ============================================================================

class ChatFilter(BaseModel):
    """Filter criteria for querying chats."""
    user_id: Optional[str] = None
    patient: Optional[str] = None
    status: Optional[ChatStatus] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    search_term: Optional[str] = Field(None, description="Search in title or content")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "user123",
                "status": "active",
                "created_after": "2025-01-01T00:00:00Z"
            }
        }
    )


class MessageFilter(BaseModel):
    """Filter criteria for querying messages."""
    chat_id: Optional[UUID] = None
    role: Optional[MessageRole] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    search_term: Optional[str] = Field(None, description="Search in message content")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "chat_id": "550e8400-e29b-41d4-a716-446655440000",
                "role": "user"
            }
        }
    )


class ReviewFilter(BaseModel):
    """Filter criteria for querying reviews."""
    chat_id: Optional[UUID] = None
    status: Optional[ReviewStatus] = None
    assigned_to: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "pending",
                "assigned_to": "reviewer@example.com"
            }
        }
    )


# ============================================================================
# Abstract Base Class for State Management
# ============================================================================

class ChatHistoryStateManager(ABC):
    """
    Abstract base class for chat history state management.

    This class defines the interface contract for managing chat conversations,
    messages, and reviews. Implementations should handle:

    - Connection management and pooling
    - Transaction management
    - Error handling and logging
    - Performance optimization (indexes, caching)
    - Concurrent access control

    Design Considerations:
    - All methods should be thread-safe
    - Use connection pooling for database backends
    - Implement proper error handling with custom exceptions
    - Consider adding caching for frequently accessed data
    - Use batch operations where possible for better performance
    - Implement soft deletes instead of hard deletes

    Storage Backend Requirements:
    - Support for UUID primary keys
    - Support for JSON/JSONB for metadata fields
    - Support for timestamp fields with timezone
    - Support for enum types or string constraints
    - Support for foreign key relationships
    - Support for indexes on frequently queried fields
    """

    # ========================================================================
    # Chat Operations
    # ========================================================================

    @abstractmethod
    def create_chat(self, chat: ChatCreate) -> ChatModel:
        """
        Create a new chat conversation.

        Args:
            chat: Chat creation data

        Returns:
            Created chat with generated ID and timestamps

        Raises:
            ValueError: If validation fails
            Exception: If creation fails
        """
        pass

    @abstractmethod
    def get_chat(self, chat_id: UUID) -> Optional[ChatModel]:
        """
        Retrieve a chat by ID.

        Args:
            chat_id: Unique chat identifier

        Returns:
            Chat model if found, None otherwise

        Raises:
            Exception: If retrieval fails
        """
        pass

    @abstractmethod
    def update_chat(self, chat_id: UUID, chat_update: ChatUpdate) -> Optional[ChatModel]:
        """
        Update an existing chat.

        Args:
            chat_id: Unique chat identifier
            chat_update: Fields to update

        Returns:
            Updated chat model if found, None otherwise

        Raises:
            ValueError: If validation fails
            Exception: If update fails
        """
        pass

    @abstractmethod
    def delete_chat(self, chat_id: UUID) -> bool:
        """
        Delete a chat and all associated messages.

        Note: Consider implementing soft delete by setting status to DELETED
        instead of removing the record permanently.

        Args:
            chat_id: Unique chat identifier

        Returns:
            True if deleted, False if not found

        Raises:
            Exception: If deletion fails
        """
        pass

    @abstractmethod
    def list_chats(
        self,
        filters: Optional[ChatFilter] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "created_at",
        sort_desc: bool = True
    ) -> PaginatedResponse[ChatModel]:
        """
        List chats with filtering, pagination, and sorting.

        Args:
            filters: Optional filter criteria
            page: Page number (1-indexed)
            page_size: Number of items per page
            sort_by: Field to sort by
            sort_desc: Sort in descending order if True

        Returns:
            Paginated response with chat list

        Raises:
            ValueError: If pagination parameters are invalid
            Exception: If query fails
        """
        pass

    # ========================================================================
    # Message Operations
    # ========================================================================

    @abstractmethod
    def create_message(self, message: MessageCreate) -> MessageModel:
        """
        Create a new message in a chat.

        Args:
            message: Message creation data

        Returns:
            Created message with generated ID and timestamp

        Raises:
            ValueError: If validation fails or chat not found
            Exception: If creation fails
        """
        pass

    @abstractmethod
    def get_message(self, message_id: UUID) -> Optional[MessageModel]:
        """
        Retrieve a message by ID.

        Args:
            message_id: Unique message identifier

        Returns:
            Message model if found, None otherwise

        Raises:
            Exception: If retrieval fails
        """
        pass

    @abstractmethod
    def list_messages(
        self,
        chat_id: UUID,
        filters: Optional[MessageFilter] = None,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "timestamp",
        sort_desc: bool = False
    ) -> PaginatedResponse[MessageModel]:
        """
        List messages for a chat with filtering and pagination.

        Args:
            chat_id: Parent chat identifier
            filters: Optional filter criteria
            page: Page number (1-indexed)
            page_size: Number of items per page
            sort_by: Field to sort by
            sort_desc: Sort in descending order if True

        Returns:
            Paginated response with message list

        Raises:
            ValueError: If pagination parameters are invalid
            Exception: If query fails
        """
        pass

    @abstractmethod
    def delete_message(self, message_id: UUID) -> bool:
        """
        Delete a message.

        Args:
            message_id: Unique message identifier

        Returns:
            True if deleted, False if not found

        Raises:
            Exception: If deletion fails
        """
        pass

    # ========================================================================
    # Review Operations
    # ========================================================================

    @abstractmethod
    def create_review(self, review: ReviewCreate) -> ReviewModel:
        """
        Create a new review for a chat.

        Args:
            review: Review creation data

        Returns:
            Created review with generated ID and timestamps

        Raises:
            ValueError: If validation fails or chat not found
            Exception: If creation fails
        """
        pass

    @abstractmethod
    def get_review(self, review_id: UUID) -> Optional[ReviewModel]:
        """
        Retrieve a review by ID.

        Args:
            review_id: Unique review identifier

        Returns:
            Review model if found, None otherwise

        Raises:
            Exception: If retrieval fails
        """
        pass

    @abstractmethod
    def get_review_by_chat(self, chat_id: UUID) -> Optional[ReviewModel]:
        """
        Retrieve the review for a specific chat.

        Args:
            chat_id: Chat identifier

        Returns:
            Review model if found, None otherwise

        Raises:
            Exception: If retrieval fails
        """
        pass

    @abstractmethod
    def update_review(self, review_id: UUID, review_update: ReviewUpdate) -> Optional[ReviewModel]:
        """
        Update an existing review.

        Args:
            review_id: Unique review identifier
            review_update: Fields to update

        Returns:
            Updated review model if found, None otherwise

        Raises:
            ValueError: If validation fails
            Exception: If update fails
        """
        pass

    @abstractmethod
    def list_reviews(
        self,
        filters: Optional[ReviewFilter] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "created_at",
        sort_desc: bool = True
    ) -> PaginatedResponse[ReviewModel]:
        """
        List reviews with filtering and pagination.

        Args:
            filters: Optional filter criteria
            page: Page number (1-indexed)
            page_size: Number of items per page
            sort_by: Field to sort by
            sort_desc: Sort in descending order if True

        Returns:
            Paginated response with review list

        Raises:
            ValueError: If pagination parameters are invalid
            Exception: If query fails
        """
        pass

    @abstractmethod
    def delete_review(self, review_id: UUID) -> bool:
        """
        Delete a review.

        Args:
            review_id: Unique review identifier

        Returns:
            True if deleted, False if not found

        Raises:
            Exception: If deletion fails
        """
        pass

    # ========================================================================
    # Batch Operations (Optional but Recommended)
    # ========================================================================

    def create_messages_batch(self, messages: List[MessageCreate]) -> List[MessageModel]:
        """
        Create multiple messages in a single operation.

        Default implementation calls create_message for each item.
        Implementations should override this for better performance.

        Args:
            messages: List of messages to create

        Returns:
            List of created messages

        Raises:
            ValueError: If validation fails
            Exception: If creation fails
        """
        return [self.create_message(msg) for msg in messages]

    # ========================================================================
    # Utility Methods
    # ========================================================================

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the state manager is healthy and can connect to storage.

        Returns:
            True if healthy, False otherwise
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close connections and clean up resources.

        This should be called when shutting down the application.
        """
        pass
