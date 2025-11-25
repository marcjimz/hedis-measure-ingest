#!/usr/bin/env python3
"""
Quick start script for running the backend in mock mode.

This script sets up the environment for mock mode and starts the FastAPI server.
Perfect for local development without Databricks dependencies.

Usage:
    python app/backend/run_mock.py
"""

import os
import sys
from pathlib import Path

# Set mock mode before importing anything else
os.environ["MOCK_MODE"] = "true"
os.environ["DEBUG"] = "true"

# Add project root to path (go up two levels from this file)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("🚀 Starting HEDIS Chat API in MOCK MODE")
    print("=" * 60)
    print()
    print("📝 Mock mode enabled - no Databricks connection required")
    print("✅ Sample data loaded automatically")
    print("🌐 API will be available at: http://localhost:8000")
    print("📚 API docs available at: http://localhost:8000/api/docs")
    print()
    print("💡 Sample chats available:")
    print("   - chat_001: BCS measure question")
    print("   - chat_002: Diabetes screening question")
    print()
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    print()

    # Change to project root directory
    os.chdir(str(project_root))

    uvicorn.run(
        "app.backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
