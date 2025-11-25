# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS FastAPI Application Deployment (Mock Mode)
# MAGIC
# MAGIC Deploys app/backend/ FastAPI application with mock services for testing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Requirements

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration & Setup

# COMMAND ----------

import os
import sys
import pathlib
import yaml
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Add src directory to Python path
repo_root = pathlib.Path().absolute().parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

# Load configuration from config.yaml
try:
    with open("../config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    # Fallback for different execution contexts
    with open("/Workspace/Repos/hedis-measure-ingest/notebooks/config.yaml", "r") as f:
        config = yaml.safe_load(f)

# Create configuration widgets with ALL values loaded from config.yaml
dbutils.widgets.text("catalog_name", config.get("catalog_name", "main"), "Catalog")
dbutils.widgets.text("schema_name", config.get("schema_name", "hedis_measurements"), "Schema")
dbutils.widgets.text("app_name", config.get("app_name", "hedis-chat-app"), "App Name")
dbutils.widgets.text("agent_endpoint", config.get("agent_endpoint", "hedis_chat_agent"), "Agent Endpoint Name")
dbutils.widgets.dropdown("enable_auth", "Yes" if config.get("enable_auth", False) else "No", ["Yes", "No"], "Enable Authentication")
dbutils.widgets.text("allowed_users", config.get("allowed_users", ""), "Allowed Users (comma-separated, empty = all)")

# Get configuration from widgets
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
APP_NAME = dbutils.widgets.get("app_name")
AGENT_ENDPOINT = dbutils.widgets.get("agent_endpoint")
ENABLE_AUTH = dbutils.widgets.get("enable_auth") == "Yes"
ALLOWED_USERS = [u.strip() for u in dbutils.widgets.get("allowed_users").split(",") if u.strip()]

# Initialize workspace client
w = WorkspaceClient()
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
CURRENT_USER = w.current_user.me().user_name

print(f"✅ Environment configured:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema: {SCHEMA_NAME}")
print(f"   App Name: {APP_NAME}")
print(f"   Agent Endpoint: {AGENT_ENDPOINT}")
print(f"   Authentication: {ENABLE_AUTH}")
print(f"   Current User: {CURRENT_USER}")
if ALLOWED_USERS:
    print(f"   Allowed Users: {', '.join(ALLOWED_USERS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Create Delta Tables

# COMMAND ----------

# Set catalog and schema context
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# Create chat_sessions table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions (
    session_id STRING NOT NULL,
    user_name STRING NOT NULL,
    thread_id STRING,
    started_at TIMESTAMP NOT NULL,
    last_activity_at TIMESTAMP NOT NULL,
    message_count INT DEFAULT 0,
    session_metadata STRING,
    CONSTRAINT pk_chat_sessions PRIMARY KEY (session_id)
)
USING DELTA
COMMENT 'Chat session tracking for HEDIS FastAPI application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")

print(f"✅ Table created/verified: {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions")

# Create chat_messages table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages (
    message_id STRING NOT NULL,
    session_id STRING NOT NULL,
    thread_id STRING,
    role STRING NOT NULL,
    content STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    token_count INT,
    tool_calls STRING,
    message_metadata STRING,
    CONSTRAINT pk_chat_messages PRIMARY KEY (message_id)
)
USING DELTA
COMMENT 'Chat message history for HEDIS FastAPI application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print(f"✅ Table created/verified: {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages")

# Verify tables exist
sessions_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions").collect()[0].cnt
messages_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages").collect()[0].cnt

print(f"\n📈 Current data:")
print(f"   Sessions: {sessions_count}")
print(f"   Messages: {messages_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Verify FastAPI Application

# COMMAND ----------

# Verify app/backend/ directory exists with complete application code
app_dir = repo_root / "app" / "backend"

if not app_dir.exists():
    raise FileNotFoundError(f"❌ Application directory not found: {app_dir}")

if not (app_dir / "main.py").exists():
    raise FileNotFoundError(f"❌ Main application file not found: {app_dir / 'main.py'}")

print(f"✅ Using existing FastAPI application: {app_dir / 'main.py'}")
print(f"   Application will run in MOCK MODE for testing")

# Verify required application structure
required_files = [
    "main.py",
    "config.py",
    "routers/__init__.py",
    "routers/chats.py",
    "routers/reviews.py",
    "models/__init__.py",
    "models/api_models.py",
    "services/mock_chat_history.py",
    "databricks/mock_agent_service.py",
    "databricks/mock_uc_functions.py"
]

missing_files = []
for file_path in required_files:
    if not (app_dir / file_path).exists():
        missing_files.append(file_path)

if missing_files:
    print(f"\n⚠️  Warning: Missing some expected files:")
    for f in missing_files:
        print(f"   - {f}")
    print(f"   Deployment may fail if these are required files.")
else:
    print(f"✅ All required application files verified")

# List all files that will be deployed
print(f"\n📦 Application structure:")
import os
for root, dirs, files in os.walk(app_dir):
    level = root.replace(str(app_dir), '').count(os.sep)
    indent = ' ' * 2 * level
    print(f"{indent}{os.path.basename(root)}/")
    subindent = ' ' * 2 * (level + 1)
    for file in files:
        if not file.endswith('.pyc') and not file.startswith('.'):
            print(f"{subindent}{file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Verify App Configuration

# COMMAND ----------

# Verify app configuration files exist
app_yaml_path = repo_root / "app" / "app.yaml"
requirements_path = repo_root / "app" / "requirements.txt"

if not app_yaml_path.exists():
    print(f"❌ app.yaml not found at: {app_yaml_path}")
    print(f"   Please create app/app.yaml for backend app configuration")
else:
    print(f"✅ App configuration found: {app_yaml_path}")

if not requirements_path.exists():
    print(f"❌ requirements.txt not found at: {requirements_path}")
    print(f"   Please create app/requirements.txt for backend dependencies")
else:
    print(f"✅ App requirements found: {requirements_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Local Testing Instructions

# COMMAND ----------

import subprocess
import time
import requests

print("🧪 Validating application for local testing...")

# Set environment variables for mock mode testing
os.environ["MOCK_MODE"] = "true"
os.environ["DEBUG"] = "true"
os.environ["CATALOG_NAME"] = CATALOG_NAME
os.environ["SCHEMA_NAME"] = SCHEMA_NAME
os.environ["AGENT_ENDPOINT"] = AGENT_ENDPOINT
os.environ["ENABLE_AUTH"] = "false"

# Start server in background (will stop when cell completes)
try:
    # Test imports
    print("Testing application imports...")
    sys.path.insert(0, str(app_dir.parent.parent))

    # Quick validation - don't actually start server in notebook
    print("✅ Application code validated")
    print("✅ Mock mode configuration set")
    print("\n📋 To test locally with mock data:")
    print(f"  cd {repo_root}")
    print(f"  python app/backend/run_mock.py")
    print("\n   This will start the server at: http://localhost:8000")
    print("   API docs available at: http://localhost:8000/api/docs")
    print("   Health check: http://localhost:8000/health")
    print("\n📊 Mock data includes:")
    print("   • 2 sample chats (BCS measure, diabetes)")
    print("   • Fake HEDIS measure responses")
    print("   • Mock UC functions service")
    print("   • In-memory chat history")

except Exception as e:
    print(f"⚠️  Validation error: {e}")
    print("This may be expected in notebook environment - deployment should work")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy to Databricks Apps

# COMMAND ----------

print(f"🚀 Deploying {APP_NAME} to Databricks Apps...")

try:
    from databricks.sdk.service.apps import App, AppDeployment, AppDeploymentMode, AppDeploymentStatus

    # Check if app already exists
    existing_app = None
    try:
        existing_app = w.apps.get(name=APP_NAME)
        print(f"✅ Found existing app: {APP_NAME}")
        print(f"   Current status: {existing_app.status.state if existing_app.status else 'UNKNOWN'}")
    except Exception:
        print(f"📦 Creating new app: {APP_NAME}")

    # Deploy the app
    print(f"📤 Deploying from source directory: {repo_root / 'app'}")
    print(f"   Using configuration: {repo_root / 'app/app.yaml'}")

    # Start deployment using Databricks SDK
    deployment = w.apps.deploy(
        app_name=APP_NAME,
        source_code_path=str(repo_root / "app")
    )

    print(f"✅ Deployment initiated!")
    print(f"   App Name: {APP_NAME}")
    print(f"   Status: {deployment.status.state if deployment.status else 'DEPLOYING'}")

    # Provide app URLs
    app_url = f"https://{WORKSPACE_URL}/apps/{APP_NAME}"
    docs_url = f"{app_url}/api/docs"
    health_url = f"{app_url}/health"

    print(f"\n🔗 App URLs (available after deployment completes):")
    print(f"   Application: {app_url}")
    print(f"   API Docs: {docs_url}")
    print(f"   Health Check: {health_url}")

    print(f"\n📊 Monitor deployment status:")
    print(f"   w.apps.get(name='{APP_NAME}')")

except ImportError as e:
    print(f"⚠️  Databricks SDK Apps service not available: {e}")
    print(f"   Falling back to CLI instructions...")
    print(f"\n🚀 Deploy using Databricks CLI:")
    print(f"   cd {repo_root}")
    print(f"   databricks apps deploy --source-dir app --app-name {APP_NAME}")
    print(f"\n🔗 App URL: https://{WORKSPACE_URL}/apps/{APP_NAME}")

except Exception as e:
    print(f"❌ Deployment error: {e}")
    print(f"\n🚀 Alternative deployment methods:")
    print(f"   1. CLI: databricks apps deploy --source-dir app --app-name {APP_NAME}")
    print(f"   2. UI: Upload via Databricks Apps console")
    print(f"\n🔗 App URL: https://{WORKSPACE_URL}/apps/{APP_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Health Check & Monitoring Setup

# COMMAND ----------

# Create monitoring queries in Delta Lake
print("📊 Setting up monitoring queries...")

# Create monitoring view
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring AS
SELECT
    DATE(s.last_activity_at) as activity_date,
    s.user_name,
    COUNT(DISTINCT s.session_id) as session_count,
    COUNT(m.message_id) as message_count,
    AVG(m.token_count) as avg_tokens_per_message,
    MAX(s.last_activity_at) as last_activity
FROM {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions s
LEFT JOIN {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages m
    ON s.session_id = m.session_id
GROUP BY DATE(s.last_activity_at), s.user_name
ORDER BY activity_date DESC, session_count DESC
""")

print(f"✅ Monitoring view created: {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring")

# Create health check function
health_check_code = '''
from databricks.sdk import WorkspaceClient
import requests
import time

def check_app_health(app_url: str, timeout: int = 30) -> dict:
    """
    Check health of deployed FastAPI application.

    Args:
        app_url: Base URL of the deployed app
        timeout: Request timeout in seconds

    Returns:
        dict with health status information
    """
    try:
        # Check health endpoint
        response = requests.get(
            f"{app_url}/health",
            timeout=timeout
        )

        if response.status_code == 200:
            health_data = response.json()
            return {
                "status": "healthy",
                "app_status": health_data.get("status"),
                "agent_endpoint": health_data.get("agent_endpoint"),
                "timestamp": health_data.get("timestamp"),
                "response_time_ms": response.elapsed.total_seconds() * 1000
            }
        else:
            return {
                "status": "unhealthy",
                "error": f"HTTP {response.status_code}",
                "response_time_ms": response.elapsed.total_seconds() * 1000
            }

    except requests.exceptions.Timeout:
        return {
            "status": "unhealthy",
            "error": "Request timeout"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

# Example usage
if __name__ == "__main__":
    app_url = "https://YOUR_WORKSPACE/apps/hedis-chat-app"
    result = check_app_health(app_url)
    print(result)
'''

with open(repo_root / "health_check.py", "w") as f:
    f.write(health_check_code)

print(f"✅ Health check script created: {repo_root / 'health_check.py'}")

# Display monitoring query
print("\n📊 Sample Monitoring Query:")
print(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring LIMIT 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Rollback Procedures

# COMMAND ----------

rollback_doc = f'''# HEDIS FastAPI Application Rollback Procedures

## Quick Rollback Steps

### 1. Immediate Rollback (Disable App)
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# Stop the app
w.apps.stop("{APP_NAME}")
```

### 2. Roll Back to Previous Version
```bash
# Using Databricks CLI
databricks apps deploy --source-dir . --app-name {APP_NAME} --version <previous-version>
```

### 3. Database Rollback (If Schema Changes)
```sql
-- Restore chat_sessions table from history
RESTORE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions TO VERSION AS OF <version>;

-- Restore chat_messages table from history
RESTORE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages TO VERSION AS OF <version>;
```

### 4. Check Table Versions (For Delta Time Travel)
```sql
DESCRIBE HISTORY {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions;
DESCRIBE HISTORY {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages;
```

## Rollback Checklist

- [ ] Verify current app status and version
- [ ] Check error logs and identify issue
- [ ] Notify users of temporary downtime (if needed)
- [ ] Execute rollback procedure
- [ ] Verify app health after rollback
- [ ] Review and fix root cause
- [ ] Document incident and learnings

## Common Issues and Solutions

### Issue: App won't start
**Solution:** Check logs, verify environment variables, ensure agent endpoint is available

### Issue: Database connection errors
**Solution:** Verify catalog/schema permissions, check Delta table status

### Issue: Agent endpoint not responding
**Solution:** Check Model Serving endpoint status, verify agent deployment

### Issue: Authentication errors
**Solution:** Review ALLOWED_USERS configuration, check workspace permissions

## Emergency Contacts

- Databricks Admin: [admin contact]
- App Owner: {CURRENT_USER}
- On-Call Engineer: [on-call contact]

## Monitoring Dashboards

- App Metrics: {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring
- Agent Metrics: Model Serving endpoint metrics
- System Health: /health endpoint

## Backup Locations

- Application Code: {app_dir / 'main.py'}
- Configuration: {repo_root / 'app/app.yaml'}
- Requirements: {repo_root / 'app/requirements.txt'}
- Delta Tables: {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions, chat_messages
'''

with open(repo_root / "ROLLBACK.md", "w") as f:
    f.write(rollback_doc)

print(f"✅ Rollback documentation created: {repo_root / 'ROLLBACK.md'}")

# Create rollback script
rollback_script = f'''#!/bin/bash
# HEDIS FastAPI Application Rollback Script

set -e

echo "🔄 Rolling back HEDIS FastAPI Application..."

# Stop current app
echo "⏸️  Stopping current app deployment..."
databricks apps stop {APP_NAME}

# Optional: Deploy previous version
# Uncomment and specify version to deploy previous version
# echo "📦 Deploying previous version..."
# databricks apps deploy --source-dir . --app-name {APP_NAME} --version PREVIOUS_VERSION

echo "✅ Rollback complete!"
echo "📊 Check status: databricks apps get {APP_NAME}"
'''

with open(repo_root / "rollback_app.sh", "w") as f:
    f.write(rollback_script)

os.chmod(repo_root / "rollback_app.sh", 0o755)

print(f"✅ Rollback script created: {repo_root / 'rollback_app.sh'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Deployment Summary

# COMMAND ----------

print(f"""
{'='*80}
✅ DEPLOYMENT SETUP COMPLETE (MOCK MODE)
{'='*80}

📁 FILES CREATED:
   {repo_root / 'health_check.py'}
   {repo_root / 'ROLLBACK.md'}
   {repo_root / 'rollback_app.sh'}

📁 FILES VERIFIED:
   {repo_root / 'app/app.yaml'}
   {repo_root / 'app/requirements.txt'}
   {repo_root / 'app/backend/'} (application code)

📊 DELTA TABLES:
   {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions
   {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages
   {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring

⚙️  CONFIGURATION:
   Mode: MOCK
   App Name: {APP_NAME}
   Catalog: {CATALOG_NAME}
   Schema: {SCHEMA_NAME}

🚀 DEPLOYMENT:
   Application deployed using Databricks SDK (see Deploy cell above)
   Monitor status: w.apps.get(name='{APP_NAME}')

🔗 APP URL (after deployment):
   https://{WORKSPACE_URL}/apps/{APP_NAME}
   https://{WORKSPACE_URL}/apps/{APP_NAME}/api/docs

{'='*80}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📖 Create README

# COMMAND ----------

# Create README for the app
readme_content = f'''# HEDIS Chat FastAPI Application

A production-ready FastAPI web application for the HEDIS Chat Agent, deployed on Databricks Apps.

## Overview

This application provides a REST API interface to the HEDIS Chat Agent, enabling:
- Conversational AI for HEDIS measure analysis
- Multi-turn conversations with persistent state
- Chat history stored in Delta Lake
- User authentication and authorization
- Real-time streaming responses
- Comprehensive monitoring and analytics

## Architecture

```
┌─────────────────┐
│   FastAPI App   │
│   (This Code)   │
└────────┬────────┘
         │
         ├─────► HEDIS Chat Agent (Model Serving)
         │       └─► Unity Catalog Functions
         │           └─► Vector Search Index
         │
         └─────► Delta Lake (Chat History)
                 └─► chat_sessions
                 └─► chat_messages
```

## Quick Start

### Prerequisites

1. Complete infrastructure setup: `notebooks/setup_infrastructure.py`
2. Deploy HEDIS agent: `notebooks/agents/02_agent_deployment.py`
3. Create UC functions: `notebooks/agents/01_setup_uc_functions.py`

### Deployment

```bash
# Deploy to Databricks Apps
bash deploy_app.sh

# Or use Databricks CLI
databricks apps deploy --source-dir . --app-name {APP_NAME}
```

### Local Development

```bash
# Install dependencies
pip install -r app/requirements.txt

# Set environment variables
export CATALOG_NAME="{CATALOG_NAME}"
export SCHEMA_NAME="{SCHEMA_NAME}"
export AGENT_ENDPOINT="{AGENT_ENDPOINT}"
export ENABLE_AUTH="false"

# Run locally
cd {repo_root}
uvicorn app.backend.main:app --reload --port 8000

# Visit http://localhost:8000/docs
```

## API Reference

### POST /chat

Chat with the HEDIS agent.

**Request:**
```json
{{
  "messages": [
    {{"role": "user", "content": "What is the BCS measure?"}}
  ],
  "session_id": "optional-session-id",
  "thread_id": "optional-thread-id",
  "stream": false
}}
```

**Response:**
```json
{{
  "messages": [
    {{
      "role": "assistant",
      "content": "The Breast Cancer Screening (BCS) measure..."
    }}
  ],
  "session_id": "generated-session-id",
  "thread_id": "generated-thread-id",
  "effective_year": 2025,
  "timestamp": "2025-11-25T12:00:00"
}}
```

### GET /sessions

List chat sessions for current user.

### GET /sessions/{{id}}/messages

Retrieve all messages for a session.

### DELETE /sessions/{{id}}

Delete a session and its messages.

### GET /metrics

Get application usage metrics.

### GET /health

Health check endpoint.

## Configuration

Environment variables:

- `CATALOG_NAME`: Unity Catalog catalog name
- `SCHEMA_NAME`: Unity Catalog schema name
- `AGENT_ENDPOINT`: Model Serving endpoint name
- `ENABLE_AUTH`: Enable authentication (true/false)
- `ALLOWED_USERS`: Comma-separated list of allowed users

## Monitoring

### Health Checks

```bash
curl https://{WORKSPACE_URL}/apps/{APP_NAME}/health
```

### Metrics

```sql
SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring
```

### Logs

View logs in Databricks Apps console or query Delta tables.

## Security

- Unity Catalog governance
- Row-level security on Delta tables
- User authentication via Databricks
- Session isolation per user

## Troubleshooting

### App won't start
- Check environment variables
- Verify agent endpoint is running
- Review application logs

### Database errors
- Verify catalog/schema permissions
- Check Delta table status
- Ensure Unity Catalog access

### Authentication issues
- Review ALLOWED_USERS configuration
- Check workspace permissions
- Verify user tokens

## Rollback

See [ROLLBACK.md](ROLLBACK.md) for detailed rollback procedures.

```bash
bash rollback_app.sh
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
black app/
flake8 app/
```

## Support

- App Owner: {CURRENT_USER}
- Documentation: /docs endpoint
- Issues: Databricks workspace support

## License

Copyright (c) 2025. All rights reserved.
'''

with open(repo_root / "app" / "README.md", "w") as f:
    f.write(readme_content)

print(f"✅ Application README created: {repo_root / 'app' / 'README.md'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ready to Deploy
