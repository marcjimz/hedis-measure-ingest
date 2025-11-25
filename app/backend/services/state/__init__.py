"""
Chat History State Management Module

This module provides a flexible, extensible architecture for managing chat
conversation history, messages, and reviews with multiple storage backends.

Available Implementations:
- DeltaTableStateManager: Databricks Delta Tables (production-ready)
- Future: LakebaseStateManager for PostgreSQL via Lakebase

Quick Start:
```python
from app.backend.services.state import (
    DeltaTableStateManager,
    ChatCreate, MessageCreate, ReviewCreate,
    ChatStatus, MessageRole, ReviewStatus
)

# Initialize state manager
manager = DeltaTableStateManager(
    catalog="main",
    schema="chat_history"
)

# Create a chat
chat = manager.create_chat(ChatCreate(
    user_id="user123",
    title="HEDIS Measure Inquiry",
    patient="Patient-001"
))

# Add messages
manager.create_message(MessageCreate(
    chat_id=chat.id,
    role=MessageRole.USER,
    content="What are the BCS measure criteria?"
))

# List user's chats
chats = manager.list_chats(
    filters=ChatFilter(user_id="user123"),
    page=1,
    page_size=20
)
```

Architecture:
- Abstract base class defines interface contract
- Pydantic models ensure type safety and validation
- Implementations can swap storage backends seamlessly
- Repository pattern encapsulates data access logic
"""

# Abstract base and models
from app.backend.services.state.base import (
    # Abstract base class
    ChatHistoryStateManager,

    # Enums
    ChatStatus,
    MessageRole,
    ReviewStatus,

    # Chat models
    ChatBase,
    ChatCreate,
    ChatUpdate,
    ChatModel,
    ChatFilter,

    # Message models
    MessageBase,
    MessageCreate,
    MessageModel,
    MessageFilter,

    # Review models
    ReviewBase,
    ReviewCreate,
    ReviewUpdate,
    ReviewModel,
    ReviewFilter,

    # Pagination
    PaginatedResponse,
)

# Concrete implementations
from app.backend.services.state.delta_table import (
    DeltaTableStateManager,
    CHATS_TABLE_SCHEMA,
    MESSAGES_TABLE_SCHEMA,
    REVIEWS_TABLE_SCHEMA,
)


__all__ = [
    # Abstract base
    "ChatHistoryStateManager",

    # Implementations
    "DeltaTableStateManager",

    # Enums
    "ChatStatus",
    "MessageRole",
    "ReviewStatus",

    # Chat models
    "ChatBase",
    "ChatCreate",
    "ChatUpdate",
    "ChatModel",
    "ChatFilter",

    # Message models
    "MessageBase",
    "MessageCreate",
    "MessageModel",
    "MessageFilter",

    # Review models
    "ReviewBase",
    "ReviewCreate",
    "ReviewUpdate",
    "ReviewModel",
    "ReviewFilter",

    # Pagination
    "PaginatedResponse",

    # Schemas
    "CHATS_TABLE_SCHEMA",
    "MESSAGES_TABLE_SCHEMA",
    "REVIEWS_TABLE_SCHEMA",
]


# Version information
__version__ = "1.0.0"
__author__ = "HEDIS Measure Ingest Team"
