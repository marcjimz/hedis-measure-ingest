"""
Delta Table State Manager Implementation

This module implements ChatHistoryStateManager using Databricks Delta Tables.
It provides efficient, ACID-compliant storage for chat conversations with:

- Automatic schema evolution
- Time travel capabilities
- Optimized partition strategies for performance
- Merge-based upserts for efficient updates
- Built-in versioning and audit trails

Design Features:
- Partition by user_id and date for query optimization
- Z-ORDER clustering on frequently queried columns
- Optimistic concurrency control with Delta's ACID guarantees
- Batch operations for improved throughput
- Automatic compaction and optimization

Table Schemas:
See SQL DDL statements in the module for complete table definitions.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    MapType, BooleanType
)
from delta.tables import DeltaTable

from backend.services.state.base import (
    ChatHistoryStateManager,
    ChatCreate, ChatUpdate, ChatModel, ChatFilter,
    MessageCreate, MessageModel, MessageFilter,
    ReviewCreate, ReviewUpdate, ReviewModel, ReviewFilter,
    PaginatedResponse,
    ChatStatus, MessageRole, ReviewStatus
)


logger = logging.getLogger(__name__)


# ============================================================================
# Table Schema Definitions (SQL DDL)
# ============================================================================

CHATS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.chats (
    id STRING NOT NULL,
    user_id STRING NOT NULL,
    title STRING,
    patient STRING,
    status STRING NOT NULL,
    metadata MAP<STRING, STRING>,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    partition_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE))
)
USING DELTA
PARTITIONED BY (user_id, partition_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
)
COMMENT 'Chat conversations with partition optimization for user queries';

-- Performance optimization indexes
OPTIMIZE {catalog}.{schema}.chats ZORDER BY (id, status, created_at);
"""

MESSAGES_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.messages (
    id STRING NOT NULL,
    chat_id STRING NOT NULL,
    role STRING NOT NULL,
    content STRING NOT NULL,
    metadata MAP<STRING, STRING>,
    timestamp TIMESTAMP NOT NULL,
    partition_date DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (chat_id, partition_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Chat messages partitioned by chat_id for efficient conversation retrieval';

-- Performance optimization indexes
OPTIMIZE {catalog}.{schema}.messages ZORDER BY (id, chat_id, timestamp);
"""

REVIEWS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.reviews (
    id STRING NOT NULL,
    chat_id STRING NOT NULL,
    status STRING NOT NULL,
    assigned_to STRING,
    ai_suggestion STRING,
    reviewer_notes STRING,
    metadata MAP<STRING, STRING>,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    partition_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE))
)
USING DELTA
PARTITIONED BY (status, partition_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Chat reviews partitioned by status for efficient workflow queries';

-- Performance optimization indexes
OPTIMIZE {catalog}.{schema}.reviews ZORDER BY (id, chat_id, assigned_to);
"""


# ============================================================================
# Delta Table State Manager Implementation
# ============================================================================

class DeltaTableStateManager(ChatHistoryStateManager):
    """
    State manager implementation using Databricks Delta Tables.

    This implementation provides ACID-compliant, high-performance storage
    for chat conversations using Delta Lake features:

    - Automatic schema evolution for metadata fields
    - Time travel for audit and recovery
    - Optimistic concurrency control
    - Partition pruning for fast queries
    - Z-ORDER clustering for multi-dimensional queries
    - Change Data Feed for downstream processing

    Features:
    - Thread-safe operations with Delta's ACID guarantees
    - Efficient batch operations
    - Automatic compaction and optimization
    - Built-in versioning and audit trails

    Args:
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        spark: Optional SparkSession (creates new if not provided)
        auto_create_tables: Automatically create tables if they don't exist

    Example:
        ```python
        manager = DeltaTableStateManager(
            catalog="main",
            schema="chat_history"
        )

        # Create a chat
        chat = manager.create_chat(ChatCreate(
            user_id="user123",
            title="HEDIS BCS Inquiry",
            patient="Patient-001"
        ))

        # Add messages
        message = manager.create_message(MessageCreate(
            chat_id=chat.id,
            role=MessageRole.USER,
            content="What are the BCS measure criteria?"
        ))

        # List chats with filters
        chats = manager.list_chats(
            filters=ChatFilter(user_id="user123", status=ChatStatus.ACTIVE),
            page=1,
            page_size=20
        )
        ```
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        spark: Optional[SparkSession] = None,
        auto_create_tables: bool = True
    ):
        """
        Initialize Delta Table state manager.

        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            spark: Optional SparkSession (creates new if not provided)
            auto_create_tables: Create tables if they don't exist
        """
        self.catalog = catalog
        self.schema = schema
        self.spark = spark or SparkSession.builder.getOrCreate()

        # Fully qualified table names
        self.chats_table = f"{catalog}.{schema}.chats"
        self.messages_table = f"{catalog}.{schema}.messages"
        self.reviews_table = f"{catalog}.{schema}.reviews"

        # Initialize tables if requested
        if auto_create_tables:
            self._create_tables()

        logger.info(
            f"Initialized DeltaTableStateManager: catalog={catalog}, schema={schema}"
        )

    def _create_tables(self) -> None:
        """
        Create Delta tables if they don't exist.

        This method is idempotent and safe to call multiple times.
        """
        try:
            # Create chats table
            sql = CHATS_TABLE_SCHEMA.format(catalog=self.catalog, schema=self.schema)
            for statement in sql.split(';'):
                if statement.strip():
                    self.spark.sql(statement)
            logger.info(f"Created/verified chats table: {self.chats_table}")

            # Create messages table
            sql = MESSAGES_TABLE_SCHEMA.format(catalog=self.catalog, schema=self.schema)
            for statement in sql.split(';'):
                if statement.strip():
                    self.spark.sql(statement)
            logger.info(f"Created/verified messages table: {self.messages_table}")

            # Create reviews table
            sql = REVIEWS_TABLE_SCHEMA.format(catalog=self.catalog, schema=self.schema)
            for statement in sql.split(';'):
                if statement.strip():
                    self.spark.sql(statement)
            logger.info(f"Created/verified reviews table: {self.reviews_table}")

        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _dict_to_spark_map(self, d: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        """
        Convert Python dict to Spark MAP<STRING, STRING>.

        Args:
            d: Python dictionary

        Returns:
            Dictionary with all values converted to strings
        """
        if d is None:
            return None
        return {str(k): str(v) for k, v in d.items()}

    def _spark_map_to_dict(self, m: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        """
        Convert Spark MAP<STRING, STRING> to Python dict.

        Args:
            m: Spark map

        Returns:
            Python dictionary
        """
        if m is None:
            return None
        return dict(m)

    def _row_to_chat_model(self, row) -> ChatModel:
        """Convert Spark Row to ChatModel."""
        return ChatModel(
            id=UUID(row.id),
            user_id=row.user_id,
            title=row.title,
            patient=row.patient,
            status=ChatStatus(row.status),
            metadata=self._spark_map_to_dict(row.metadata),
            created_at=row.created_at,
            updated_at=row.updated_at
        )

    def _row_to_message_model(self, row) -> MessageModel:
        """Convert Spark Row to MessageModel."""
        return MessageModel(
            id=UUID(row.id),
            chat_id=UUID(row.chat_id),
            role=MessageRole(row.role),
            content=row.content,
            metadata=self._spark_map_to_dict(row.metadata),
            timestamp=row.timestamp
        )

    def _row_to_review_model(self, row) -> ReviewModel:
        """Convert Spark Row to ReviewModel."""
        return ReviewModel(
            id=UUID(row.id),
            chat_id=UUID(row.chat_id),
            status=ReviewStatus(row.status),
            assigned_to=row.assigned_to,
            ai_suggestion=row.ai_suggestion,
            reviewer_notes=row.reviewer_notes,
            metadata=self._spark_map_to_dict(row.metadata),
            created_at=row.created_at,
            updated_at=row.updated_at
        )

    # ========================================================================
    # Chat Operations
    # ========================================================================

    def create_chat(self, chat: ChatCreate) -> ChatModel:
        """
        Create a new chat conversation.

        Uses Delta Lake ACID guarantees for safe concurrent writes.
        """
        try:
            chat_id = uuid4()
            now = datetime.utcnow()

            # Create DataFrame with single row
            data = [(
                str(chat_id),
                chat.user_id,
                chat.title,
                chat.patient,
                chat.status.value,
                self._dict_to_spark_map(chat.metadata),
                now,
                now
            )]

            df = self.spark.createDataFrame(
                data,
                schema=["id", "user_id", "title", "patient", "status", "metadata", "created_at", "updated_at"]
            )

            # Write to Delta table
            df.write.format("delta").mode("append").saveAsTable(self.chats_table)

            logger.info(f"Created chat: {chat_id}")

            return ChatModel(
                id=chat_id,
                user_id=chat.user_id,
                title=chat.title,
                patient=chat.patient,
                status=chat.status,
                metadata=chat.metadata,
                created_at=now,
                updated_at=now
            )

        except Exception as e:
            logger.error(f"Error creating chat: {str(e)}", exc_info=True)
            raise

    def get_chat(self, chat_id: UUID) -> Optional[ChatModel]:
        """Retrieve a chat by ID."""
        try:
            df = self.spark.table(self.chats_table).filter(F.col("id") == str(chat_id))
            rows = df.collect()

            if not rows:
                return None

            return self._row_to_chat_model(rows[0])

        except Exception as e:
            logger.error(f"Error getting chat {chat_id}: {str(e)}", exc_info=True)
            raise

    def update_chat(self, chat_id: UUID, chat_update: ChatUpdate) -> Optional[ChatModel]:
        """
        Update an existing chat using Delta MERGE.

        Delta's MERGE provides atomic updates with ACID guarantees.
        """
        try:
            # Check if chat exists
            existing = self.get_chat(chat_id)
            if not existing:
                return None

            # Build update dict with only non-None values
            updates = {}
            if chat_update.title is not None:
                updates["title"] = chat_update.title
            if chat_update.patient is not None:
                updates["patient"] = chat_update.patient
            if chat_update.status is not None:
                updates["status"] = chat_update.status.value
            if chat_update.metadata is not None:
                updates["metadata"] = self._dict_to_spark_map(chat_update.metadata)

            if not updates:
                return existing  # No updates to apply

            updates["updated_at"] = datetime.utcnow()

            # Create update DataFrame
            update_data = [(str(chat_id),) + tuple(updates.values())]
            columns = ["id"] + list(updates.keys())
            update_df = self.spark.createDataFrame(update_data, schema=columns)

            # Use Delta MERGE for atomic update
            delta_table = DeltaTable.forName(self.spark, self.chats_table)
            delta_table.alias("target").merge(
                update_df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdate(set=updates).execute()

            logger.info(f"Updated chat: {chat_id}")

            # Return updated chat
            return self.get_chat(chat_id)

        except Exception as e:
            logger.error(f"Error updating chat {chat_id}: {str(e)}", exc_info=True)
            raise

    def delete_chat(self, chat_id: UUID) -> bool:
        """
        Soft delete a chat by setting status to DELETED.

        Note: This is a soft delete. Use Delta's VACUUM command
        for permanent deletion if needed.
        """
        try:
            result = self.update_chat(
                chat_id,
                ChatUpdate(status=ChatStatus.DELETED)
            )
            return result is not None

        except Exception as e:
            logger.error(f"Error deleting chat {chat_id}: {str(e)}", exc_info=True)
            raise

    def list_chats(
        self,
        filters: Optional[ChatFilter] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "created_at",
        sort_desc: bool = True
    ) -> PaginatedResponse[ChatModel]:
        """
        List chats with filtering and pagination.

        Leverages Delta partition pruning for optimal performance.
        """
        try:
            # Start with base table
            df = self.spark.table(self.chats_table)

            # Apply filters
            if filters:
                if filters.user_id:
                    df = df.filter(F.col("user_id") == filters.user_id)
                if filters.patient:
                    df = df.filter(F.col("patient") == filters.patient)
                if filters.status:
                    df = df.filter(F.col("status") == filters.status.value)
                if filters.created_after:
                    df = df.filter(F.col("created_at") >= filters.created_after)
                if filters.created_before:
                    df = df.filter(F.col("created_at") <= filters.created_before)
                if filters.search_term:
                    df = df.filter(
                        F.col("title").contains(filters.search_term)
                    )

            # Get total count
            total = df.count()

            # Apply sorting
            df = df.orderBy(F.col(sort_by).desc() if sort_desc else F.col(sort_by).asc())

            # Apply pagination
            offset = (page - 1) * page_size
            df = df.limit(page_size).offset(offset)

            # Collect results
            rows = df.collect()
            items = [self._row_to_chat_model(row) for row in rows]

            return PaginatedResponse.create(items, total, page, page_size)

        except Exception as e:
            logger.error(f"Error listing chats: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # Message Operations
    # ========================================================================

    def create_message(self, message: MessageCreate) -> MessageModel:
        """Create a new message in a chat."""
        try:
            message_id = uuid4()
            now = datetime.utcnow()

            # Create DataFrame
            data = [(
                str(message_id),
                str(message.chat_id),
                message.role.value,
                message.content,
                self._dict_to_spark_map(message.metadata),
                now
            )]

            df = self.spark.createDataFrame(
                data,
                schema=["id", "chat_id", "role", "content", "metadata", "timestamp"]
            )

            # Write to Delta table
            df.write.format("delta").mode("append").saveAsTable(self.messages_table)

            logger.info(f"Created message: {message_id}")

            return MessageModel(
                id=message_id,
                chat_id=message.chat_id,
                role=message.role,
                content=message.content,
                metadata=message.metadata,
                timestamp=now
            )

        except Exception as e:
            logger.error(f"Error creating message: {str(e)}", exc_info=True)
            raise

    def get_message(self, message_id: UUID) -> Optional[MessageModel]:
        """Retrieve a message by ID."""
        try:
            df = self.spark.table(self.messages_table).filter(
                F.col("id") == str(message_id)
            )
            rows = df.collect()

            if not rows:
                return None

            return self._row_to_message_model(rows[0])

        except Exception as e:
            logger.error(f"Error getting message {message_id}: {str(e)}", exc_info=True)
            raise

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
        List messages for a chat.

        Partition pruning by chat_id ensures fast queries.
        """
        try:
            # Start with chat filter
            df = self.spark.table(self.messages_table).filter(
                F.col("chat_id") == str(chat_id)
            )

            # Apply additional filters
            if filters:
                if filters.role:
                    df = df.filter(F.col("role") == filters.role.value)
                if filters.created_after:
                    df = df.filter(F.col("timestamp") >= filters.created_after)
                if filters.created_before:
                    df = df.filter(F.col("timestamp") <= filters.created_before)
                if filters.search_term:
                    df = df.filter(F.col("content").contains(filters.search_term))

            # Get total count
            total = df.count()

            # Apply sorting
            df = df.orderBy(F.col(sort_by).desc() if sort_desc else F.col(sort_by).asc())

            # Apply pagination
            offset = (page - 1) * page_size
            df = df.limit(page_size).offset(offset)

            # Collect results
            rows = df.collect()
            items = [self._row_to_message_model(row) for row in rows]

            return PaginatedResponse.create(items, total, page, page_size)

        except Exception as e:
            logger.error(f"Error listing messages: {str(e)}", exc_info=True)
            raise

    def delete_message(self, message_id: UUID) -> bool:
        """
        Hard delete a message.

        Note: Consider implementing soft delete if audit trail is needed.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, self.messages_table)
            delta_table.delete(F.col("id") == str(message_id))
            logger.info(f"Deleted message: {message_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting message {message_id}: {str(e)}", exc_info=True)
            return False

    # ========================================================================
    # Review Operations
    # ========================================================================

    def create_review(self, review: ReviewCreate) -> ReviewModel:
        """Create a new review for a chat."""
        try:
            review_id = uuid4()
            now = datetime.utcnow()

            # Create DataFrame
            data = [(
                str(review_id),
                str(review.chat_id),
                review.status.value,
                review.assigned_to,
                review.ai_suggestion,
                review.reviewer_notes,
                self._dict_to_spark_map(review.metadata),
                now,
                now
            )]

            df = self.spark.createDataFrame(
                data,
                schema=[
                    "id", "chat_id", "status", "assigned_to",
                    "ai_suggestion", "reviewer_notes", "metadata",
                    "created_at", "updated_at"
                ]
            )

            # Write to Delta table
            df.write.format("delta").mode("append").saveAsTable(self.reviews_table)

            logger.info(f"Created review: {review_id}")

            return ReviewModel(
                id=review_id,
                chat_id=review.chat_id,
                status=review.status,
                assigned_to=review.assigned_to,
                ai_suggestion=review.ai_suggestion,
                reviewer_notes=review.reviewer_notes,
                metadata=review.metadata,
                created_at=now,
                updated_at=now
            )

        except Exception as e:
            logger.error(f"Error creating review: {str(e)}", exc_info=True)
            raise

    def get_review(self, review_id: UUID) -> Optional[ReviewModel]:
        """Retrieve a review by ID."""
        try:
            df = self.spark.table(self.reviews_table).filter(
                F.col("id") == str(review_id)
            )
            rows = df.collect()

            if not rows:
                return None

            return self._row_to_review_model(rows[0])

        except Exception as e:
            logger.error(f"Error getting review {review_id}: {str(e)}", exc_info=True)
            raise

    def get_review_by_chat(self, chat_id: UUID) -> Optional[ReviewModel]:
        """Retrieve the review for a specific chat."""
        try:
            df = self.spark.table(self.reviews_table).filter(
                F.col("chat_id") == str(chat_id)
            )
            rows = df.collect()

            if not rows:
                return None

            return self._row_to_review_model(rows[0])

        except Exception as e:
            logger.error(f"Error getting review for chat {chat_id}: {str(e)}", exc_info=True)
            raise

    def update_review(self, review_id: UUID, review_update: ReviewUpdate) -> Optional[ReviewModel]:
        """Update an existing review using Delta MERGE."""
        try:
            # Check if review exists
            existing = self.get_review(review_id)
            if not existing:
                return None

            # Build update dict
            updates = {}
            if review_update.status is not None:
                updates["status"] = review_update.status.value
            if review_update.assigned_to is not None:
                updates["assigned_to"] = review_update.assigned_to
            if review_update.reviewer_notes is not None:
                updates["reviewer_notes"] = review_update.reviewer_notes
            if review_update.metadata is not None:
                updates["metadata"] = self._dict_to_spark_map(review_update.metadata)

            if not updates:
                return existing

            updates["updated_at"] = datetime.utcnow()

            # Create update DataFrame
            update_data = [(str(review_id),) + tuple(updates.values())]
            columns = ["id"] + list(updates.keys())
            update_df = self.spark.createDataFrame(update_data, schema=columns)

            # Use Delta MERGE
            delta_table = DeltaTable.forName(self.spark, self.reviews_table)
            delta_table.alias("target").merge(
                update_df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdate(set=updates).execute()

            logger.info(f"Updated review: {review_id}")

            return self.get_review(review_id)

        except Exception as e:
            logger.error(f"Error updating review {review_id}: {str(e)}", exc_info=True)
            raise

    def list_reviews(
        self,
        filters: Optional[ReviewFilter] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "created_at",
        sort_desc: bool = True
    ) -> PaginatedResponse[ReviewModel]:
        """List reviews with filtering and pagination."""
        try:
            df = self.spark.table(self.reviews_table)

            # Apply filters
            if filters:
                if filters.chat_id:
                    df = df.filter(F.col("chat_id") == str(filters.chat_id))
                if filters.status:
                    df = df.filter(F.col("status") == filters.status.value)
                if filters.assigned_to:
                    df = df.filter(F.col("assigned_to") == filters.assigned_to)
                if filters.created_after:
                    df = df.filter(F.col("created_at") >= filters.created_after)
                if filters.created_before:
                    df = df.filter(F.col("created_at") <= filters.created_before)

            # Get total count
            total = df.count()

            # Apply sorting
            df = df.orderBy(F.col(sort_by).desc() if sort_desc else F.col(sort_by).asc())

            # Apply pagination
            offset = (page - 1) * page_size
            df = df.limit(page_size).offset(offset)

            # Collect results
            rows = df.collect()
            items = [self._row_to_review_model(row) for row in rows]

            return PaginatedResponse.create(items, total, page, page_size)

        except Exception as e:
            logger.error(f"Error listing reviews: {str(e)}", exc_info=True)
            raise

    def delete_review(self, review_id: UUID) -> bool:
        """Hard delete a review."""
        try:
            delta_table = DeltaTable.forName(self.spark, self.reviews_table)
            delta_table.delete(F.col("id") == str(review_id))
            logger.info(f"Deleted review: {review_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting review {review_id}: {str(e)}", exc_info=True)
            return False

    # ========================================================================
    # Batch Operations
    # ========================================================================

    def create_messages_batch(self, messages: List[MessageCreate]) -> List[MessageModel]:
        """
        Create multiple messages in a single batch operation.

        This is much more efficient than individual inserts.
        """
        try:
            if not messages:
                return []

            now = datetime.utcnow()
            data = []
            result_models = []

            for msg in messages:
                message_id = uuid4()
                data.append((
                    str(message_id),
                    str(msg.chat_id),
                    msg.role.value,
                    msg.content,
                    self._dict_to_spark_map(msg.metadata),
                    now
                ))
                result_models.append(MessageModel(
                    id=message_id,
                    chat_id=msg.chat_id,
                    role=msg.role,
                    content=msg.content,
                    metadata=msg.metadata,
                    timestamp=now
                ))

            # Create DataFrame and write in single operation
            df = self.spark.createDataFrame(
                data,
                schema=["id", "chat_id", "role", "content", "metadata", "timestamp"]
            )
            df.write.format("delta").mode("append").saveAsTable(self.messages_table)

            logger.info(f"Created {len(messages)} messages in batch")

            return result_models

        except Exception as e:
            logger.error(f"Error creating messages batch: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def health_check(self) -> bool:
        """
        Check if the state manager can connect to Delta tables.

        Returns:
            True if healthy, False otherwise
        """
        try:
            # Try to query each table
            self.spark.table(self.chats_table).limit(1).count()
            self.spark.table(self.messages_table).limit(1).count()
            self.spark.table(self.reviews_table).limit(1).count()
            return True
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}", exc_info=True)
            return False

    def close(self) -> None:
        """
        Close connections and clean up resources.

        For Delta tables, this is typically a no-op since SparkSession
        is managed externally. Consider calling spark.stop() if you
        own the SparkSession.
        """
        logger.info("DeltaTableStateManager close called")
        # Note: Don't stop SparkSession here - it may be shared
        pass

    # ========================================================================
    # Advanced Operations (Delta-specific)
    # ========================================================================

    def optimize_tables(self) -> None:
        """
        Optimize Delta tables with Z-ORDER clustering.

        Run this periodically for better query performance.
        """
        try:
            # Optimize chats table
            self.spark.sql(f"""
                OPTIMIZE {self.chats_table}
                ZORDER BY (id, status, created_at)
            """)
            logger.info(f"Optimized {self.chats_table}")

            # Optimize messages table
            self.spark.sql(f"""
                OPTIMIZE {self.messages_table}
                ZORDER BY (id, chat_id, timestamp)
            """)
            logger.info(f"Optimized {self.messages_table}")

            # Optimize reviews table
            self.spark.sql(f"""
                OPTIMIZE {self.reviews_table}
                ZORDER BY (id, chat_id, assigned_to)
            """)
            logger.info(f"Optimized {self.reviews_table}")

        except Exception as e:
            logger.error(f"Error optimizing tables: {str(e)}", exc_info=True)
            raise

    def vacuum_tables(self, retention_hours: int = 168) -> None:
        """
        Vacuum old versions of Delta tables.

        This permanently deletes old file versions. Default retention
        is 168 hours (7 days).

        Args:
            retention_hours: Minimum retention period in hours
        """
        try:
            self.spark.sql(f"VACUUM {self.chats_table} RETAIN {retention_hours} HOURS")
            self.spark.sql(f"VACUUM {self.messages_table} RETAIN {retention_hours} HOURS")
            self.spark.sql(f"VACUUM {self.reviews_table} RETAIN {retention_hours} HOURS")
            logger.info(f"Vacuumed tables with {retention_hours} hour retention")

        except Exception as e:
            logger.error(f"Error vacuuming tables: {str(e)}", exc_info=True)
            raise

    def get_table_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get Delta table history for audit trails.

        Args:
            table_name: One of 'chats', 'messages', 'reviews'
            limit: Number of history entries to retrieve

        Returns:
            List of history entries
        """
        try:
            table_map = {
                'chats': self.chats_table,
                'messages': self.messages_table,
                'reviews': self.reviews_table
            }

            if table_name not in table_map:
                raise ValueError(f"Invalid table name: {table_name}")

            df = self.spark.sql(f"""
                DESCRIBE HISTORY {table_map[table_name]}
                LIMIT {limit}
            """)

            return [row.asDict() for row in df.collect()]

        except Exception as e:
            logger.error(f"Error getting table history: {str(e)}", exc_info=True)
            raise
