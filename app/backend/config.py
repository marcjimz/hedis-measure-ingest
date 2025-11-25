"""
Backend Configuration Module

Centralizes all configuration settings for the FastAPI backend.
Loads from environment variables with sensible defaults.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    app_name: str = "HEDIS Chat API"
    app_version: str = "1.0.0"
    debug: bool = False

    # Mock Mode - Set to True for local development without Databricks
    mock_mode: bool = False

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # CORS
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:5173"]
    cors_credentials: bool = True
    cors_methods: list[str] = ["*"]
    cors_headers: list[str] = ["*"]

    # Databricks
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None

    # Unity Catalog
    uc_catalog: str = "marcin_demo2"
    uc_schema: str = "hedis_measurements"

    # LLM
    llm_endpoint: str = "databricks-claude-sonnet-4-5"

    # Agent
    agent_endpoint: Optional[str] = None  # Deployed agent endpoint URL
    effective_year: int = 2025

    # Vector Search
    vector_search_endpoint: str = "hedis_vector_endpoint"
    vector_index_name: str = "hedis_measures_index"

    # Delta Tables for Chat History
    chats_table: str = "hedis_chats"
    messages_table: str = "hedis_messages"
    reviews_table: str = "hedis_reviews"

    # PostgreSQL for State (optional - for LangGraph checkpointing)
    postgres_enabled: bool = False
    postgres_instance: Optional[str] = None  # Lakebase instance name
    postgres_database: str = "databricks_postgres"

    # Authentication (placeholder for future implementation)
    auth_enabled: bool = False
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 60

    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 1000
    rate_limit_window_seconds: int = 3600  # 1 hour

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
