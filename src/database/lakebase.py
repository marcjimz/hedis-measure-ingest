"""
LakebaseDatabase module for managing PostgreSQL connections with Databricks.

This module provides a connection pool manager for PostgreSQL databases hosted on
Databricks Lakebase, with automatic credential rotation and integration with LangGraph's
PostgresSaver for conversation history persistence.
"""

import uuid
import logging
from typing import Optional
from datetime import datetime

import os
import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg_pool import ConnectionPool


logger = logging.getLogger(__name__)


class LakebaseDatabaseError(Exception):
    """Base exception for LakebaseDatabase errors."""
    pass


class ConnectionInitializationError(LakebaseDatabaseError):
    """Raised when database connection initialization fails."""
    pass


class CredentialGenerationError(LakebaseDatabaseError):
    """Raised when credential generation fails."""
    pass


class LakebaseDatabase:
    """
    Manages PostgreSQL connections to Databricks Lakebase with automatic credential rotation.

    This class provides connection pool management for PostgreSQL databases hosted on
    Databricks Lakebase. It handles:
    - Automatic credential generation and rotation
    - Connection pooling with custom connection factory
    - PostgresSaver setup for LangGraph checkpoint persistence
    - Support for both OAuth and passthrough authentication
    - Proper error handling and logging

    Attributes:
        host (Optional[str]): Databricks workspace host URL
        connection_pool (ConnectionPool): Pool of database connections
        client_id (Optional[str]): Databricks OAuth client ID (optional)
        client_secret (Optional[str]): Databricks OAuth client secret (optional)
        w (WorkspaceClient): Databricks workspace client
    """

    def __init__(self, host: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None):
        """
        Initialize the LakebaseDatabase instance.

        Args:
            host: Databricks workspace host URL (optional, uses default if not provided)
            client_id: Databricks OAuth client ID (optional, uses environment variable or default auth)
            client_secret: Databricks OAuth client secret (optional, uses environment variable or default auth)

        Notes:
            If client_id and client_secret are not provided, the WorkspaceClient will use
            default authentication (e.g., current user's token for passthrough authentication).
        """
        self.connection_pool: Optional[ConnectionPool] = None
        self.host = host
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")

        try:
            # Initialize WorkspaceClient with provided credentials or use default authentication
            if self.client_id and self.client_secret:
                self.w = WorkspaceClient(
                    host=self.host,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                logger.info(f"Initialized Databricks workspace client with OAuth credentials for host: {host}")
            else:
                # Use default authentication (passthrough or environment-based)
                self.w = WorkspaceClient(host=self.host) if self.host else WorkspaceClient()
                logger.info(f"Initialized Databricks workspace client with default authentication")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks workspace client: {str(e)}", exc_info=True)
            raise ConnectionInitializationError(f"Failed to initialize workspace client: {str(e)}") from e

    def get_connection_pool(self) -> Optional[ConnectionPool]:
        """
        Get the current connection pool.

        Returns:
            The active connection pool, or None if not initialized
        """
        return self.connection_pool

    def initialize_connection(
        self,
        user: str,
        instance_name: str,
        database: str = "databricks_postgres",
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        setup_checkpointer: bool = False
    ) -> str:
        """
        Initialize database connection using Databricks credentials.

        This method:
        1. Retrieves the database instance information
        2. Generates initial credentials
        3. Creates a connection pool with custom connection factory for token refresh
        4. Optionally sets up PostgresSaver for LangGraph checkpointing

        Args:
            user: Database username
            instance_name: Databricks database instance name
            database: Database name (default: "databricks_postgres")
            min_pool_size: Minimum number of connections in pool (default: 1)
            max_pool_size: Maximum number of connections in pool (default: 10)
            setup_checkpointer: Whether to run PostgresSaver.setup() (default: False)

        Returns:
            Connection string for the database

        Raises:
            ConnectionInitializationError: If connection initialization fails
            CredentialGenerationError: If credential generation fails
        """
        try:
            # Get database instance information
            logger.info(f"Retrieving database instance: {instance_name}")
            instance: DatabaseInstance = self.w.database.get_database_instance(name=instance_name)

            if not instance.read_write_dns:
                raise ConnectionInitializationError(
                    f"Database instance {instance_name} does not have read_write_dns configured"
                )

            # Generate initial credentials
            try:
                logger.info(f"Generating database credentials for instance: {instance_name}")
                cred = self.w.database.generate_database_credential(
                    request_id=str(uuid.uuid4()),
                    instance_names=[instance_name]
                )

                if not cred.token:
                    raise CredentialGenerationError("Failed to generate credential token")

            except Exception as e:
                logger.error(f"Error generating database credentials: {str(e)}", exc_info=True)
                raise CredentialGenerationError(f"Failed to generate credentials: {str(e)}") from e

            # Build connection parameters
            host = instance.read_write_dns
            port = 5432
            password = cred.token

            # Create connection string
            conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"

            # Create custom connection class with access to workspace client
            logger.info("Creating custom connection factory with token refresh capability")
            CustomConnection = self._create_connection_factory(instance_name)

            # Initialize connection pool
            logger.info(
                f"Initializing connection pool (min_size={min_pool_size}, max_size={max_pool_size})"
            )
            self.connection_pool = ConnectionPool(
                conninfo=f"dbname={database} user={user} host={host} sslmode=require",
                connection_class=CustomConnection,
                min_size=min_pool_size,
                max_size=max_pool_size,
                open=True
            )

            # Optionally setup PostgresSaver for checkpointing
            if setup_checkpointer:
                self.setup_postgres_saver()

            logger.info(f"Successfully initialized database connection to {instance_name}")
            return conn_string

        except (ConnectionInitializationError, CredentialGenerationError):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error initializing database connection: {str(e)}",
                exc_info=True
            )
            raise ConnectionInitializationError(
                f"Failed to initialize database connection: {str(e)}"
            ) from e

    def setup_postgres_saver(self) -> None:
        """
        Set up PostgresSaver tables for LangGraph checkpointing.

        This method should be called once during initial setup to create the necessary
        tables for storing conversation checkpoints. It requires appropriate database
        permissions (CREATE TABLE, INSERT).

        Raises:
            RuntimeError: If connection pool is not initialized
            Exception: If table creation fails
        """
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialized. Call initialize_connection() first.")

        try:
            logger.info("Setting up PostgresSaver tables for LangGraph checkpointing")
            with self.connection_pool.connection() as conn:
                checkpointer = PostgresSaver(conn)
                checkpointer.setup()
                logger.info("PostgresSaver setup completed successfully")
        except Exception as e:
            logger.error(f"Error setting up PostgresSaver: {str(e)}", exc_info=True)
            raise

    def get_postgres_saver(self) -> PostgresSaver:
        """
        Get a PostgresSaver instance for the current connection pool.

        Returns:
            PostgresSaver instance configured with the current connection pool

        Raises:
            RuntimeError: If connection pool is not initialized
        """
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialized. Call initialize_connection() first.")

        # Get a connection from the pool
        conn = self.connection_pool.connection()
        return PostgresSaver(conn)

    def get_conversation_history(
        self,
        session_id: Optional[str] = None,
        limit: int = 100
    ) -> Optional[list]:
        """
        Retrieve conversation history from database.

        Note: This is a placeholder method. Actual implementation depends on your
        specific schema and requirements for storing conversation history.

        Args:
            session_id: Optional session ID to filter conversations
            limit: Maximum number of records to return (default: 100)

        Returns:
            List of conversation records, or None if not implemented
        """
        logger.warning("get_conversation_history is not yet implemented")
        return None

    def close(self) -> None:
        """
        Close the connection pool and clean up resources.

        This method should be called when shutting down the application to ensure
        all connections are properly closed.
        """
        if self.connection_pool:
            logger.info("Closing database connection pool")
            try:
                self.connection_pool.close()
                self.connection_pool = None
                logger.info("Connection pool closed successfully")
            except Exception as e:
                logger.error(f"Error closing connection pool: {str(e)}", exc_info=True)
                raise

    def _create_connection_factory(self, instance_name: str):
        """
        Create a connection factory that automatically refreshes credentials.

        This factory creates a custom psycopg.Connection subclass that generates
        fresh Databricks credentials for each new connection, ensuring tokens
        don't expire during long-running operations.

        Args:
            instance_name: Databricks database instance name

        Returns:
            Custom connection class with automatic token refresh
        """
        workspace_client = self.w

        class CustomConnection(psycopg.Connection):
            """Custom connection class with automatic credential refresh."""

            @classmethod
            def connect(cls, conninfo: str = '', **kwargs):
                """
                Connect to the database with fresh credentials.

                This method generates a new credential token before each connection
                attempt, ensuring credentials are always valid.

                Args:
                    conninfo: Connection info string
                    **kwargs: Additional connection parameters

                Returns:
                    Connected database connection instance
                """
                try:
                    # Generate fresh credentials for this connection
                    cred = workspace_client.database.generate_database_credential(
                        request_id=str(uuid.uuid4()),
                        instance_names=[instance_name]
                    )
                    kwargs['password'] = cred.token

                    # Call the superclass's connect method with updated kwargs
                    return super().connect(conninfo, **kwargs)

                except Exception as e:
                    logger.error(
                        f"Error generating credentials in connection factory: {str(e)}",
                        exc_info=True
                    )
                    raise

        return CustomConnection

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection pool is closed."""
        self.close()
        return False
