"""
Abstract Chat History Manager

Defines the interface for chat history and state management.
Multiple implementations can be created (Delta Tables, PostgreSQL, etc.)
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any
from models.chat import (
    Message,
    Chat,
    ChatListItem,
    ChatListResponse,
    ChatCreate,
    ChatUpdate,
    ChatStatus,
    Pagination
)
from models.review import Review, ReviewCreate, ReviewUpdate


class ChatHistoryManager(ABC):
    """
    Abstract base class for chat history and state management.

    Defines the interface for storing and retrieving:
    - Chat sessions
    - Messages within chats
    - Reviews
    - User interactions

    Implementations can use different backends:
    - DeltaTableChatHistoryManager: Uses Delta tables on Databricks
    - PostgreSQLChatHistoryManager: Uses PostgreSQL/Lakebase
    - RedisChatHistoryManager: Uses Redis for fast access
    """

    # ============================================================================
    # Chat Management
    # ============================================================================

    @abstractmethod
    async def create_chat(
        self,
        user_id: str,
        patient: Optional[str] = None,
        title: str = "New Chat"
    ) -> str:
        """
        Create a new chat session.

        Args:
            user_id: User who owns the chat
            patient: Optional patient identifier
            title: Chat title

        Returns:
            chat_id: Unique identifier for the chat
        """
        pass

    @abstractmethod
    async def get_chat(self, chat_id: str) -> Optional[Chat]:
        """
        Retrieve a complete chat with all messages.

        Args:
            chat_id: Chat identifier

        Returns:
            Chat object with messages, or None if not found
        """
        pass

    @abstractmethod
    async def update_chat_status(
        self,
        chat_id: str,
        status: ChatStatus
    ) -> bool:
        """
        Update the status of a chat.

        Args:
            chat_id: Chat identifier
            status: New status (active, under_review, completed, returned)

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def update_chat(
        self,
        chat_id: str,
        update: ChatUpdate
    ) -> bool:
        """
        Update chat metadata (title, patient, status).

        Args:
            chat_id: Chat identifier
            update: ChatUpdate model with fields to update

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def list_chats(
        self,
        user_id: str,
        status: Optional[ChatStatus] = None,
        page: int = 1,
        limit: int = 20
    ) -> ChatListResponse:
        """
        List all chats for a user with optional filtering.

        Args:
            user_id: User identifier
            status: Optional status filter
            page: Page number (1-indexed)
            limit: Results per page

        Returns:
            ChatListResponse with chat summaries and pagination
        """
        pass

    @abstractmethod
    async def delete_chat(self, chat_id: str) -> bool:
        """
        Delete a chat (soft delete recommended).

        Args:
            chat_id: Chat identifier

        Returns:
            True if successful, False otherwise
        """
        pass

    # ============================================================================
    # Message Management
    # ============================================================================

    @abstractmethod
    async def save_message(
        self,
        chat_id: str,
        role: str,
        content: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Save a message to a chat.

        Args:
            chat_id: Chat identifier
            role: Message role (user, assistant, system, tool)
            content: Message content
            timestamp: Optional timestamp (defaults to now)

        Returns:
            message_id: Unique identifier for the message
        """
        pass

    @abstractmethod
    async def get_chat_history(
        self,
        chat_id: str,
        limit: Optional[int] = None
    ) -> List[Message]:
        """
        Retrieve message history for a chat.

        Args:
            chat_id: Chat identifier
            limit: Optional limit on number of messages

        Returns:
            List of Message objects ordered by timestamp
        """
        pass

    # ============================================================================
    # Review Management
    # ============================================================================

    @abstractmethod
    async def create_review(
        self,
        review_create: ReviewCreate
    ) -> str:
        """
        Create a new review request.

        Args:
            review_create: ReviewCreate model with review details

        Returns:
            review_id: Unique identifier for the review
        """
        pass

    @abstractmethod
    async def get_review(self, review_id: str) -> Optional[Review]:
        """
        Retrieve a review by ID.

        Args:
            review_id: Review identifier

        Returns:
            Review object or None if not found
        """
        pass

    @abstractmethod
    async def update_review(
        self,
        review_id: str,
        update: ReviewUpdate
    ) -> bool:
        """
        Update a review.

        Args:
            review_id: Review identifier
            update: ReviewUpdate model with fields to update

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def list_reviews(
        self,
        status: Optional[str] = None,
        assigned_to: Optional[str] = None,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        List reviews with optional filtering.

        Args:
            status: Optional status filter (pending, assigned, resolved)
            assigned_to: Optional user ID filter (use "me" for current user)
            page: Page number (1-indexed)
            limit: Results per page

        Returns:
            Dictionary with reviews list and pagination metadata
        """
        pass

    # ============================================================================
    # Utility Methods
    # ============================================================================

    @abstractmethod
    async def get_user_info(self, user_id: str) -> Optional[Dict[str, str]]:
        """
        Get user information (id and name).

        Args:
            user_id: User identifier

        Returns:
            Dictionary with user info or None if not found
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the storage backend is accessible.

        Returns:
            True if healthy, False otherwise
        """
        pass
