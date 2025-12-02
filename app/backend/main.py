"""
FastAPI Application Entry Point

Main FastAPI application with middleware and router configuration.
Serves the HEDIS chat API backend.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import time

from config import settings
from routers import chats, reviews

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Lifespan context manager for startup/shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles startup and shutdown events.
    """
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Unity Catalog: {settings.uc_catalog}.{settings.uc_schema}")
    logger.info(f"LLM Endpoint: {settings.llm_endpoint}")
    logger.info(f"Effective Year: {settings.effective_year}")

    if settings.agent_endpoint:
        logger.info(f"Using deployed agent at: {settings.agent_endpoint}")

    if settings.postgres_enabled:
        logger.info(f"PostgreSQL persistence enabled: {settings.postgres_instance}")

    yield

    # Shutdown
    logger.info(f"Shutting down {settings.app_name}")


# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="API for HEDIS measure chat application with Databricks integration",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)


# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add processing time to response headers."""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests."""
    logger.info(f"{request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"{request.method} {request.url.path} - Status: {response.status_code}")
    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.debug else "An unexpected error occurred",
            "path": request.url.path
        }
    )


# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "app": settings.app_name,
        "version": settings.app_version,
        "catalog": f"{settings.uc_catalog}.{settings.uc_schema}",
        "effective_year": settings.effective_year,
        "mode": "mock" if settings.mock_mode else "production",
        "sql_warehouse_configured": bool(settings.sql_warehouse_id),
        "sql_warehouse_id": settings.sql_warehouse_id[:8] + "..." if settings.sql_warehouse_id else None
    }


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/api/docs",
        "health": "/health"
    }


# Include routers
app.include_router(chats.router, prefix="/api", tags=["Chats"])
app.include_router(reviews.router, prefix="/api", tags=["Reviews"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info"
    )
