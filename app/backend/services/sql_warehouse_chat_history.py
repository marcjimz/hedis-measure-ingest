"""
SQL Warehouse Chat History Manager

Concrete implementation using Databricks SQL Warehouse for Delta table access.
Uses Databricks SDK's Statement Execution API to interact with Unity Catalog tables.
No PySpark dependency - works in Databricks Apps environment.
"""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from services.chat_history_manager import ChatHistoryManager
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
from models.review import (
    Review,
    ReviewCreate,
    ReviewUpdate,
    ReviewListItem,
    ReviewListResponse,
    User
)
from config import settings

logger = logging.getLogger(__name__)


class SQLWarehouseChatHistoryManager(ChatHistoryManager):
    """
    Chat history manager using SQL Warehouse to access Delta tables on Databricks.

    Tables:
    - {catalog}.{schema}.{chats_table}: Chat sessions
    - {catalog}.{schema}.{messages_table}: Messages
    - {catalog}.{schema}.{reviews_table}: Reviews

    Schema for chats table:
        - id (STRING): Chat ID
        - user_id (STRING): User who owns the chat
        - title (STRING): Chat title
        - patient (STRING): Optional patient identifier
        - status (STRING): Chat status
        - created_at (TIMESTAMP): Creation timestamp
        - updated_at (TIMESTAMP): Last update timestamp
        - deleted (BOOLEAN): Soft delete flag

    Schema for messages table:
        - id (STRING): Message ID
        - chat_id (STRING): Parent chat ID
        - role (STRING): Message role
        - content (STRING): Message content
        - timestamp (TIMESTAMP): Message timestamp
        - name (STRING): Optional name
        - tool_calls (STRING): Optional JSON-encoded tool calls
        - tool_call_id (STRING): Optional tool call ID

    Schema for reviews table:
        - id (STRING): Review ID
        - chat_id (STRING): Associated chat ID
        - status (STRING): Review status
        - patient (STRING): Optional patient identifier
        - requested_by_id (STRING): User ID who requested review
        - requested_by_name (STRING): User name who requested review
        - assigned_to_id (STRING): Optional assigned reviewer ID
        - assigned_to_name (STRING): Optional assigned reviewer name
        - ai_suggestion (STRING): AI-generated suggestion
        - ai_suggestion_feedback (STRING): Optional feedback on AI suggestion
        - response (STRING): Optional reviewer response
        - feedback (STRING): Optional additional feedback
        - ai_quality_rating (INT): Optional quality rating (1-5)
        - created_at (TIMESTAMP): Creation timestamp
        - updated_at (TIMESTAMP): Last update timestamp
        - resolved_at (TIMESTAMP): Optional resolution timestamp
    """

    def __init__(self):
        """Initialize the Delta table chat history manager."""
        self.catalog = settings.uc_catalog
        self.schema = settings.uc_schema
        self.chats_table = f"{self.catalog}.{self.schema}.{settings.chats_table}"
        self.messages_table = f"{self.catalog}.{self.schema}.{settings.messages_table}"
        self.reviews_table = f"{self.catalog}.{self.schema}.{settings.reviews_table}"

        # Initialize Databricks workspace client and SQL warehouse
        try:
            # Get Databricks credentials from environment
            host = os.environ.get("DATABRICKS_HOST") or settings.databricks_host
            token = os.environ.get("DATABRICKS_TOKEN") or settings.databricks_token

            if host and token:
                self.w = WorkspaceClient(host=host, token=token)
            else:
                # In Databricks environment, SDK will use default auth
                self.w = WorkspaceClient()

            self.warehouse_id = settings.sql_warehouse_id
            if not self.warehouse_id:
                raise ValueError("SQL_WAREHOUSE_ID must be set for Delta table access")

            logger.info(f"Initialized Delta table manager with SQL Warehouse: {self.warehouse_id}")
            logger.info(f"Catalog: {self.catalog}.{self.schema}")
        except Exception as e:
            logger.error(f"Failed to initialize SQL Warehouse client: {e}")
            raise

    def execute_sql(self, sql: str, wait_timeout: str = "30s") -> List[Dict[str, Any]]:
        """
        Execute SQL query against SQL Warehouse and return results.

        Args:
            sql: SQL query to execute
            wait_timeout: Timeout for query execution (e.g., "30s", "5m")

        Returns:
            List of result rows as dictionaries
        """
        try:
            logger.debug(f"Executing SQL: {sql[:200]}...")

            # Execute SQL statement
            response = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout=wait_timeout
            )

            # Check if execution was successful
            if response.status.state != StatementState.SUCCEEDED:
                raise Exception(f"SQL execution failed: {response.status.state}")

            # Parse results
            results = []
            if response.result and response.result.data_array:
                # Get column names
                columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []

                # Convert rows to dictionaries
                for row in response.result.data_array:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = columns[i] if i < len(columns) else f"col_{i}"
                        row_dict[col_name] = value
                    results.append(row_dict)

            return results

        except Exception as e:
            logger.error(f"Error executing SQL: {e}", exc_info=True)
            raise

    # ============================================================================
    # Chat Management
    # ============================================================================

    async def create_chat(
        self,
        user_id: str,
        patient: Optional[str] = None,
        title: str = "New Chat"
    ) -> str:
        """Create a new chat session."""
        chat_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        try:
            patient_value = f"'{patient}'" if patient else "NULL"
            sql = f"""
                INSERT INTO {self.chats_table}
                (id, user_id, title, patient, status, created_at, updated_at, deleted)
                VALUES (
                    '{chat_id}',
                    '{user_id}',
                    '{title}',
                    {patient_value},
                    'active',
                    '{now}',
                    '{now}',
                    false
                )
            """

            self.execute_sql(sql)
            logger.info(f"Created chat {chat_id} for user {user_id}")
            return chat_id

        except Exception as e:
            logger.error(f"Error creating chat: {e}", exc_info=True)
            raise

    async def get_chat(self, chat_id: str) -> Optional[Chat]:
        """Retrieve a complete chat with all messages."""
        try:
            # Get chat metadata
            sql = f"""
                SELECT id, user_id, title, patient, status, created_at, updated_at
                FROM {self.chats_table}
                WHERE id = '{chat_id}' AND deleted = false
            """

            results = self.execute_sql(sql)

            if not results:
                return None

            chat_row = results[0]

            # Get messages
            messages = await self.get_chat_history(chat_id)

            return Chat(
                id=chat_row["id"],
                userId=chat_row["user_id"],
                title=chat_row["title"],
                patient=chat_row.get("patient"),
                status=chat_row["status"],
                messages=messages,
                createdAt=chat_row["created_at"],
                updatedAt=chat_row["updated_at"]
            )

        except Exception as e:
            logger.error(f"Error retrieving chat {chat_id}: {e}", exc_info=True)
            return None

    async def update_chat_status(
        self,
        chat_id: str,
        status: ChatStatus
    ) -> bool:
        """Update the status of a chat."""
        try:
            now = datetime.utcnow().isoformat()
            sql = f"""
                UPDATE {self.chats_table}
                SET status = '{status}', updated_at = '{now}'
                WHERE id = '{chat_id}'
            """
            self.execute_sql(sql)
            logger.info(f"Updated chat {chat_id} status to {status}")
            return True

        except Exception as e:
            logger.error(f"Error updating chat status: {e}", exc_info=True)
            return False

    async def update_chat(
        self,
        chat_id: str,
        update: ChatUpdate
    ) -> bool:
        """Update chat metadata."""
        try:
            now = datetime.utcnow()
            updates = []

            if update.title is not None:
                updates.append(f"title = '{update.title}'")
            if update.patient is not None:
                updates.append(f"patient = '{update.patient}'")
            if update.status is not None:
                updates.append(f"status = '{update.status}'")

            if not updates:
                return True

            updates.append(f"updated_at = '{now}'")
            update_sql = ", ".join(updates)

            self.spark.sql(f"""
                UPDATE {self.chats_table}
                SET {update_sql}
                WHERE id = '{chat_id}'
            """)

            logger.info(f"Updated chat {chat_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating chat: {e}", exc_info=True)
            return False

    async def list_chats(
        self,
        user_id: str,
        status: Optional[ChatStatus] = None,
        page: int = 1,
        limit: int = 20
    ) -> ChatListResponse:
        """List all chats for a user with optional filtering."""
        try:
            # Build filter clause
            where_clause = f"user_id = '{user_id}' AND deleted = false"
            if status:
                where_clause += f" AND status = '{status}'"

            # Get total count
            count_df = self.spark.sql(f"""
                SELECT COUNT(*) as total
                FROM {self.chats_table}
                WHERE {where_clause}
            """)
            total = count_df.first().total

            # Calculate pagination
            offset = (page - 1) * limit
            total_pages = (total + limit - 1) // limit

            # Get paginated chats with message stats
            chats_df = self.spark.sql(f"""
                WITH chat_messages AS (
                    SELECT
                        c.id,
                        c.title,
                        c.patient,
                        c.status,
                        c.created_at,
                        m.content as last_message,
                        m.timestamp as last_message_time,
                        COUNT(m.id) as message_count
                    FROM {self.chats_table} c
                    LEFT JOIN {self.messages_table} m ON c.id = m.chat_id
                    WHERE {where_clause}
                    GROUP BY c.id, c.title, c.patient, c.status, c.created_at, m.content, m.timestamp
                )
                SELECT
                    id,
                    title,
                    patient,
                    status,
                    COALESCE(last_message, '') as lastMessage,
                    COALESCE(last_message_time, created_at) as lastMessageTime,
                    created_at as createdAt,
                    COALESCE(message_count, 0) as messageCount
                FROM chat_messages
                ORDER BY lastMessageTime DESC
                LIMIT {limit} OFFSET {offset}
            """)

            # Convert to ChatListItem objects
            chats = [
                ChatListItem(
                    id=row.id,
                    title=row.title,
                    patient=row.patient,
                    status=row.status,
                    lastMessage=row.lastMessage[:100],  # Truncate for summary
                    lastMessageTime=row.lastMessageTime,
                    createdAt=row.createdAt,
                    messageCount=row.messageCount
                )
                for row in chats_df.collect()
            ]

            pagination = Pagination(
                page=page,
                limit=limit,
                total=total,
                totalPages=total_pages
            )

            return ChatListResponse(chats=chats, pagination=pagination)

        except Exception as e:
            logger.error(f"Error listing chats: {e}", exc_info=True)
            return ChatListResponse(
                chats=[],
                pagination=Pagination(page=page, limit=limit, total=0, totalPages=0)
            )

    async def delete_chat(self, chat_id: str) -> bool:
        """Delete a chat (soft delete)."""
        try:
            now = datetime.utcnow()
            self.spark.sql(f"""
                UPDATE {self.chats_table}
                SET deleted = true, updated_at = '{now}'
                WHERE id = '{chat_id}'
            """)
            logger.info(f"Deleted chat {chat_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting chat: {e}", exc_info=True)
            return False

    # ============================================================================
    # Message Management
    # ============================================================================

    async def save_message(
        self,
        chat_id: str,
        role: str,
        content: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """Save a message to a chat."""
        message_id = str(uuid.uuid4())
        ts = (timestamp or datetime.utcnow()).isoformat()

        try:
            # Escape single quotes in content
            escaped_content = content.replace("'", "''")

            sql = f"""
                INSERT INTO {self.messages_table}
                (id, chat_id, role, content, timestamp, name, tool_calls, tool_call_id)
                VALUES (
                    '{message_id}',
                    '{chat_id}',
                    '{role}',
                    '{escaped_content}',
                    '{ts}',
                    NULL,
                    NULL,
                    NULL
                )
            """

            self.execute_sql(sql)

            # Update chat timestamp
            await self.update_chat_status(chat_id, "active")

            logger.info(f"Saved message {message_id} to chat {chat_id}")
            return message_id

        except Exception as e:
            logger.error(f"Error saving message: {e}", exc_info=True)
            raise

    async def get_chat_history(
        self,
        chat_id: str,
        limit: Optional[int] = None
    ) -> List[Message]:
        """Retrieve message history for a chat."""
        try:
            limit_clause = f"LIMIT {limit}" if limit else ""

            sql = f"""
                SELECT id, role, content, timestamp, name, tool_calls, tool_call_id
                FROM {self.messages_table}
                WHERE chat_id = '{chat_id}'
                ORDER BY timestamp ASC
                {limit_clause}
            """

            results = self.execute_sql(sql)

            messages = [
                Message(
                    id=row["id"],
                    role=row["role"],
                    content=row["content"],
                    timestamp=row["timestamp"],
                    name=row.get("name"),
                    tool_calls=row.get("tool_calls"),
                    tool_call_id=row.get("tool_call_id")
                )
                for row in results
            ]

            return messages

        except Exception as e:
            logger.error(f"Error retrieving chat history: {e}", exc_info=True)
            return []

    # ============================================================================
    # Review Management
    # ============================================================================

    async def create_review(
        self,
        review_create: ReviewCreate
    ) -> str:
        """Create a new review request."""
        review_id = str(uuid.uuid4())
        now = datetime.utcnow()

        try:
            # Get user info for requested_by
            user_info = await self.get_user_info(review_create.requestedBy)
            user_name = user_info.get("name", "Unknown") if user_info else "Unknown"

            data = [{
                "id": review_id,
                "chat_id": review_create.chatId,
                "status": "pending",
                "patient": None,  # Will be populated from chat context
                "requested_by_id": review_create.requestedBy,
                "requested_by_name": user_name,
                "assigned_to_id": None,
                "assigned_to_name": None,
                "ai_suggestion": review_create.aiSuggestion,
                "ai_suggestion_feedback": None,
                "response": None,
                "feedback": review_create.reason,
                "ai_quality_rating": None,
                "created_at": now,
                "updated_at": now,
                "resolved_at": None
            }]

            df = self.spark.createDataFrame(data)
            df.write.format("delta").mode("append").saveAsTable(self.reviews_table)

            # Update chat status to under_review
            await self.update_chat_status(review_create.chatId, "under_review")

            logger.info(f"Created review {review_id} for chat {review_create.chatId}")
            return review_id

        except Exception as e:
            logger.error(f"Error creating review: {e}", exc_info=True)
            raise

    async def get_review(self, review_id: str) -> Optional[Review]:
        """Retrieve a review by ID."""
        try:
            review_df = self.spark.sql(f"""
                SELECT *
                FROM {self.reviews_table}
                WHERE id = '{review_id}'
            """)

            if review_df.count() == 0:
                return None

            row = review_df.first()

            # Build user objects
            requested_by = User(id=row.requested_by_id, name=row.requested_by_name)
            assigned_to = None
            if row.assigned_to_id:
                assigned_to = User(id=row.assigned_to_id, name=row.assigned_to_name)

            # Get chat context
            chat = await self.get_chat(row.chat_id)
            chat_context = None
            if chat:
                chat_context = {
                    "patient": chat.patient,
                    "messages": [msg.dict() for msg in chat.messages]
                }

            return Review(
                id=row.id,
                chatId=row.chat_id,
                status=row.status,
                patient=row.patient,
                requestedBy=requested_by,
                assignedTo=assigned_to,
                aiSuggestion=row.ai_suggestion,
                aiSuggestionFeedback=row.ai_suggestion_feedback,
                response=row.response,
                feedback=row.feedback,
                aiQualityRating=row.ai_quality_rating,
                chatContext=chat_context,
                createdAt=row.created_at,
                updatedAt=row.updated_at,
                resolvedAt=row.resolved_at
            )

        except Exception as e:
            logger.error(f"Error retrieving review {review_id}: {e}", exc_info=True)
            return None

    async def update_review(
        self,
        review_id: str,
        update: ReviewUpdate
    ) -> bool:
        """Update a review."""
        try:
            now = datetime.utcnow()
            updates = []

            if update.status is not None:
                updates.append(f"status = '{update.status}'")
                if update.status == "resolved":
                    updates.append(f"resolved_at = '{now}'")

            if update.assignedTo is not None:
                user_info = await self.get_user_info(update.assignedTo)
                user_name = user_info.get("name", "Unknown") if user_info else "Unknown"
                updates.append(f"assigned_to_id = '{update.assignedTo}'")
                updates.append(f"assigned_to_name = '{user_name}'")

            if update.aiSuggestionFeedback is not None:
                updates.append(f"ai_suggestion_feedback = '{update.aiSuggestionFeedback}'")

            if update.response is not None:
                updates.append(f"response = '{update.response}'")

            if update.feedback is not None:
                updates.append(f"feedback = '{update.feedback}'")

            if update.aiQualityRating is not None:
                updates.append(f"ai_quality_rating = {update.aiQualityRating}")

            if not updates:
                return True

            updates.append(f"updated_at = '{now}'")
            update_sql = ", ".join(updates)

            self.spark.sql(f"""
                UPDATE {self.reviews_table}
                SET {update_sql}
                WHERE id = '{review_id}'
            """)

            logger.info(f"Updated review {review_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating review: {e}", exc_info=True)
            return False

    async def list_reviews(
        self,
        status: Optional[str] = None,
        assigned_to: Optional[str] = None,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """List reviews with optional filtering."""
        try:
            # Build filter clause
            where_clauses = []
            if status:
                where_clauses.append(f"status = '{status}'")
            if assigned_to:
                if assigned_to == "me":
                    # TODO: Get current user from context
                    pass
                else:
                    where_clauses.append(f"assigned_to_id = '{assigned_to}'")

            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

            # Get total count
            count_df = self.spark.sql(f"""
                SELECT COUNT(*) as total
                FROM {self.reviews_table}
                WHERE {where_clause}
            """)
            total = count_df.first().total

            # Calculate pagination
            offset = (page - 1) * limit
            total_pages = (total + limit - 1) // limit

            # Get paginated reviews
            reviews_df = self.spark.sql(f"""
                SELECT *
                FROM {self.reviews_table}
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT {limit} OFFSET {offset}
            """)

            # Convert to ReviewListItem objects
            reviews = []
            for row in reviews_df.collect():
                requested_by = User(id=row.requested_by_id, name=row.requested_by_name)
                assigned_to_obj = None
                if row.assigned_to_id:
                    assigned_to_obj = User(id=row.assigned_to_id, name=row.assigned_to_name)

                reviews.append(ReviewListItem(
                    id=row.id,
                    chatId=row.chat_id,
                    status=row.status,
                    patient=row.patient,
                    requestedBy=requested_by,
                    assignedTo=assigned_to_obj,
                    aiSuggestion=row.ai_suggestion,
                    aiSuggestionFeedback=row.ai_suggestion_feedback,
                    createdAt=row.created_at
                ))

            pagination = Pagination(
                page=page,
                limit=limit,
                total=total,
                totalPages=total_pages
            )

            return {
                "reviews": reviews,
                "pagination": pagination
            }

        except Exception as e:
            logger.error(f"Error listing reviews: {e}", exc_info=True)
            return {
                "reviews": [],
                "pagination": Pagination(page=page, limit=limit, total=0, totalPages=0)
            }

    # ============================================================================
    # Utility Methods
    # ============================================================================

    async def get_user_info(self, user_id: str) -> Optional[Dict[str, str]]:
        """
        Get user information.

        TODO: Implement user management or integrate with identity provider.
        For now, returns mock data.
        """
        return {
            "id": user_id,
            "name": f"User {user_id[:8]}"
        }

    async def health_check(self) -> bool:
        """Check if Delta tables are accessible via SQL Warehouse."""
        try:
            # Try to query the chats table
            sql = f"SELECT 1 FROM {self.chats_table} LIMIT 1"
            self.execute_sql(sql)
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)
            return False
