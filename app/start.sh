#!/bin/bash
set -e

# Start uvicorn with the port from APP_PORT environment variable
exec uvicorn backend.main:app --host 0.0.0.0 --port "${APP_PORT:-8000}"
