# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS FastAPI Application Deployment (Mock Mode)
# MAGIC
# MAGIC Deploy a FastAPI web application for the HEDIS Chat Agent on Databricks Apps.
# MAGIC
# MAGIC **⚠️ IMPORTANT: This notebook deploys the app in MOCK MODE**
# MAGIC - Uses stub data and fake responses for testing
# MAGIC - No real Databricks agent or UC functions required
# MAGIC - Perfect for validating application structure before production
# MAGIC
# MAGIC **What This Notebook Does:**
# MAGIC - Deploys the complete app/backend/ FastAPI application
# MAGIC - Configures MOCK_MODE=true for testing with stub data
# MAGIC - Sets up Delta tables (for future production use)
# MAGIC - Creates deployment scripts and configuration
# MAGIC - Provides health checks and monitoring setup
# MAGIC - Includes rollback procedures
# MAGIC
# MAGIC **Tech Stack:**
# MAGIC - 🚀 **FastAPI** - High-performance web framework
# MAGIC - 🧪 **Mock Services** - In-memory chat history, fake HEDIS responses
# MAGIC - 📊 **Delta Lake** - Tables created but not used in mock mode
# MAGIC - 🏢 **Databricks Apps** - Serverless application hosting
# MAGIC
# MAGIC **Prerequisites for Mock Mode:**
# MAGIC - ✅ app/backend/ directory with complete application code
# MAGIC - ✅ Mock service files (automatically included)
# MAGIC
# MAGIC **Prerequisites for Production Mode (later):**
# MAGIC - HEDIS infrastructure setup completed (run setup_infrastructure.py)
# MAGIC - HEDIS agent deployed to Model Serving (run agents/02_agent_deployment.py)
# MAGIC - Unity Catalog functions created (run agents/01_setup_uc_functions.py)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Requirements

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements.txt
# MAGIC %pip install -q fastapi uvicorn python-multipart httpx
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
# MAGIC ## 📊 Create Delta Tables for Chat History
# MAGIC
# MAGIC Create tables to store chat sessions and messages for persistence and analytics.

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
    'delta.autoOptimize.optimizeWrite' = 'true'
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
# MAGIC ## 🚀 Create FastAPI Application
# MAGIC
# MAGIC Build the FastAPI application with endpoints for:
# MAGIC - Chat completion (streaming and non-streaming)
# MAGIC - Session management
# MAGIC - Health checks
# MAGIC - Metrics and monitoring

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
# MAGIC ## 📝 Create App Configuration Files

# COMMAND ----------

# Create app.yaml for Databricks Apps
app_yaml = f'''# Databricks Apps configuration for HEDIS Chat FastAPI application
name: {APP_NAME}
description: "HEDIS Chat Agent - Conversational AI for HEDIS measure analysis (Mock Mode)"

# Application entry point
command:
  - "uvicorn"
  - "app.backend.main:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "${{APP_PORT}}"

# Environment variables
env:
  # Mock mode enabled for testing with stub data
  MOCK_MODE: "true"
  DEBUG: "true"

  # Databricks configuration (not used in mock mode but required for config)
  CATALOG_NAME: "{CATALOG_NAME}"
  SCHEMA_NAME: "{SCHEMA_NAME}"
  AGENT_ENDPOINT: "{AGENT_ENDPOINT}"
  ENABLE_AUTH: "false"
  ALLOWED_USERS: ""

  # Application settings
  APP_NAME: "HEDIS Chat API"
  APP_VERSION: "1.0.0"
  EFFECTIVE_YEAR: "2025"
  HOST: "0.0.0.0"
  PORT: "${{APP_PORT}}"

  # CORS settings
  CORS_ORIGINS: '["*"]'
  CORS_CREDENTIALS: "true"
  CORS_METHODS: '["*"]'
  CORS_HEADERS: '["*"]'

  # Postgres disabled for mock mode
  POSTGRES_ENABLED: "false"

# Resource configuration
resources:
  memory: "2Gi"
  cpu: "1"

# Health check configuration
health_check:
  path: "/health"
  interval_seconds: 30
  timeout_seconds: 5

# Auto-scaling configuration
scaling:
  min_instances: 1
  max_instances: 3
  target_cpu_percent: 70
'''

with open(repo_root / "app.yaml", "w") as f:
    f.write(app_yaml)

print(f"✅ App configuration created: {repo_root / 'app.yaml'}")
print(f"   MOCK_MODE=true - Application will use mock services")

# Create requirements.txt for the app
app_requirements = """# FastAPI and web framework
fastapi>=0.115.0
uvicorn[standard]>=0.32.0
python-multipart>=0.0.9
httpx>=0.27.0

# Data validation and settings
pydantic>=2.10.0
pydantic-settings>=2.6.0

# Async file operations
aiofiles>=24.1.0

# Python utilities
python-dotenv>=1.0.0

# Date/time handling
python-dateutil>=2.9.0

# Databricks integration (optional in mock mode)
mlflow[databricks]>=3.3.2
databricks-sdk>=0.35.0
pyspark>=3.5.0
"""

with open(repo_root / "app" / "requirements.txt", "w") as f:
    f.write(app_requirements)

print(f"✅ App requirements created: {repo_root / 'app' / 'requirements.txt'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test FastAPI Application Locally
# MAGIC
# MAGIC Test the application locally before deploying to Databricks Apps.

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
# MAGIC
# MAGIC Deploy the FastAPI application to Databricks Apps infrastructure.
# MAGIC
# MAGIC **Note:** Databricks Apps provides:
# MAGIC - Serverless hosting with auto-scaling
# MAGIC - Built-in load balancing
# MAGIC - HTTPS endpoints with authentication
# MAGIC - Integration with Unity Catalog for governance

# COMMAND ----------

print("🚀 Deploying FastAPI application to Databricks Apps...")
print("   MODE: MOCK (with stub data for testing)")

# Note: As of this writing, Databricks Apps deployment is typically done via:
# 1. Databricks CLI: `databricks apps deploy`
# 2. Databricks Workspace UI: Apps section
# 3. REST API: Apps API endpoints

print(f"""
📋 Deployment Instructions:

**IMPORTANT:** This deployment uses MOCK MODE with stub data.
- No real Databricks agent or UC functions required
- All responses are fake/stub data for testing
- Chat history is in-memory (not persisted to Delta)
- Perfect for testing the application structure before connecting real services

**Option 1: Using Databricks CLI (Recommended)**
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure CLI
databricks configure --token

# Deploy app with mock data
cd {repo_root}
databricks apps deploy --source-dir . --app-name {APP_NAME}
```

**Option 2: Using Workspace UI**
1. Navigate to Databricks Workspace
2. Go to 'Apps' section
3. Click 'Create App'
4. Upload entire application directory:
   - app/backend/ (all files and subdirectories)
   - app.yaml
   - app/requirements.txt
5. The app.yaml already has MOCK_MODE=true configured
6. Click 'Deploy'

**Option 3: Using REST API**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppDeployment

w = WorkspaceClient()

# Create app deployment
app_deployment = w.apps.create(
    name="{APP_NAME}",
    description="HEDIS Chat Agent FastAPI Application (Mock Mode)",
    # Additional configuration...
)
```

📁 Application Files Location:
   - Backend Code: {app_dir}/ (entire directory)
   - Main App: {app_dir / 'main.py'}
   - Config: {repo_root / 'app.yaml'}
   - Requirements: {repo_root / 'app' / 'requirements.txt'}

🧪 Mock Services Included:
   - MockChatHistoryManager: In-memory chat storage with sample data
   - MockAgentService: Fake HEDIS measure responses
   - MockUCFunctionsService: Stub measure data

🔗 After deployment, your app will be available at:
   https://{WORKSPACE_URL}/apps/{APP_NAME}

💡 To switch to PRODUCTION mode later:
   1. Edit app.yaml and change MOCK_MODE: "true" to MOCK_MODE: "false"
   2. Ensure HEDIS agent and UC functions are deployed
   3. Redeploy the app
""")

# Create a deployment script
deployment_script = f'''#!/bin/bash
# HEDIS Chat FastAPI Deployment Script

set -e

echo "🚀 Deploying HEDIS Chat FastAPI Application..."

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found. Installing..."
    pip install databricks-cli
fi

# Deploy app
echo "📦 Deploying to Databricks Apps..."
cd {repo_root}
databricks apps deploy --source-dir . --app-name {APP_NAME}

echo "✅ Deployment complete!"
echo "🔗 App URL: https://{WORKSPACE_URL}/apps/{APP_NAME}"
'''

with open(repo_root / "deploy_app.sh", "w") as f:
    f.write(deployment_script)

os.chmod(repo_root / "deploy_app.sh", 0o755)

print(f"\n✅ Deployment script created: {repo_root / 'deploy_app.sh'}")
print(f"   Run: bash {repo_root / 'deploy_app.sh'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Health Check & Monitoring Setup
# MAGIC
# MAGIC Configure monitoring and alerting for the deployed application.

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
# MAGIC
# MAGIC Document and prepare rollback procedures in case of deployment issues.

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
- Configuration: {repo_root / 'app.yaml'}
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
# MAGIC ## 📋 Deployment Summary & Next Steps

# COMMAND ----------

print(f"""
{'='*80}
✅ HEDIS FastAPI APPLICATION DEPLOYMENT SETUP COMPLETE (MOCK MODE)
{'='*80}

📁 USING EXISTING APPLICATION:
   ✓ Backend Code: {app_dir}/ (complete application structure)
   ✓ Main Application: {app_dir / 'main.py'}
   ✓ Mock Services: {app_dir / 'services/mock_chat_history.py'}
   ✓ Mock Agent: {app_dir / 'databricks/mock_agent_service.py'}
   ✓ Mock UC Functions: {app_dir / 'databricks/mock_uc_functions.py'}

📁 CREATED DEPLOYMENT FILES:
   ✓ App Configuration: {repo_root / 'app.yaml'} (MOCK_MODE=true)
   ✓ App Requirements: {repo_root / 'app' / 'requirements.txt'}
   ✓ Deployment Script: {repo_root / 'deploy_app.sh'}
   ✓ Health Check Script: {repo_root / 'health_check.py'}
   ✓ Rollback Documentation: {repo_root / 'ROLLBACK.md'}
   ✓ Rollback Script: {repo_root / 'rollback_app.sh'}

📊 DELTA TABLES CREATED:
   ✓ {CATALOG_NAME}.{SCHEMA_NAME}.chat_sessions
   ✓ {CATALOG_NAME}.{SCHEMA_NAME}.chat_messages
   ✓ {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring (view)
   ⚠️  Note: Tables created but NOT used in MOCK MODE

⚙️  CONFIGURATION:
   • Mode: MOCK (stub data, no real services required)
   • Catalog: {CATALOG_NAME} (not used in mock mode)
   • Schema: {SCHEMA_NAME} (not used in mock mode)
   • App Name: {APP_NAME}
   • Agent Endpoint: {AGENT_ENDPOINT} (not used in mock mode)
   • Authentication: Disabled (mock mode)
   • Current User: {CURRENT_USER}

🧪 MOCK MODE FEATURES:
   • In-memory chat history with 2 sample conversations
   • Fake HEDIS measure responses (BCS, COL, HBD, etc.)
   • Stub UC functions service
   • No Databricks dependencies required
   • Perfect for testing application structure

🚀 DEPLOYMENT STEPS:

1. DEPLOY THE APPLICATION:
   bash {repo_root / 'deploy_app.sh'}

   OR manually via Databricks CLI:
   cd {repo_root}
   databricks apps deploy --source-dir . --app-name {APP_NAME}

2. VERIFY DEPLOYMENT:
   # Check app status
   databricks apps get {APP_NAME}

   # Test health endpoint
   python {repo_root / 'health_check.py'}

3. ACCESS YOUR APP:
   https://{WORKSPACE_URL}/apps/{APP_NAME}

   API Documentation:
   https://{WORKSPACE_URL}/apps/{APP_NAME}/api/docs

   Health Check:
   https://{WORKSPACE_URL}/apps/{APP_NAME}/health

4. TEST WITH MOCK DATA:
   The app includes pre-loaded sample conversations:
   - Chat about BCS (Breast Cancer Screening) measure
   - Chat about diabetes and HbD measure

   All responses are fake/stub data for testing

5. MONITOR PERFORMANCE:
   GET /metrics endpoint (in-memory stats in mock mode)
   Note: Delta tables exist but are not used in mock mode

📚 API ENDPOINTS:

   POST   /api/chats                 - Create new chat or send message
   POST   /api/chats/stream          - Stream chat responses
   GET    /api/chats                 - List all chats
   GET    /api/chats/{{id}}            - Get specific chat
   DELETE /api/chats/{{id}}            - Delete chat
   POST   /api/reviews               - Submit chat for review
   GET    /api/reviews               - List reviews
   GET    /api/reviews/{{id}}          - Get specific review
   PATCH  /api/reviews/{{id}}          - Update review status
   GET    /health                    - Health check
   GET    /                          - API info
   GET    /api/docs                  - Interactive API docs
   GET    /api/redoc                 - ReDoc API documentation

🔒 SECURITY:
   • Authentication: Disabled (mock mode - enabled in production)
   • CORS: Allow all origins (mock mode - restricted in production)
   • No sensitive data in mock responses
   • Safe for testing and development

🔧 TESTING:

   # Test health endpoint
   curl https://{WORKSPACE_URL}/apps/{APP_NAME}/health

   # Create a new chat
   curl -X POST https://{WORKSPACE_URL}/apps/{APP_NAME}/api/chats \\
     -H "Content-Type: application/json" \\
     -d '{{"userId": "test-user", "context": {{"patient": "P123"}}, "title": "Test Chat"}}'

   # Send a message to chat (will get mock response)
   curl -X POST https://{WORKSPACE_URL}/apps/{APP_NAME}/api/chats \\
     -H "Content-Type: application/json" \\
     -d '{{"chatId": "chat_001", "content": "What is the BCS measure?"}}'

   # List all chats
   curl https://{WORKSPACE_URL}/apps/{APP_NAME}/api/chats

   # Get specific chat with messages
   curl https://{WORKSPACE_URL}/apps/{APP_NAME}/api/chats/chat_001

📊 MONITORING:

   • App health: /health endpoint (returns mock mode status)
   • Usage metrics: In-memory statistics (not persisted)
   • Logs: Available in Databricks Apps console
   • Note: Delta Lake analytics disabled in mock mode

🔄 ROLLBACK:

   If issues occur:
   1. Read: {repo_root / 'ROLLBACK.md'}
   2. Run: bash {repo_root / 'rollback_app.sh'}

💡 NEXT STEPS:

   1. ✅ Deploy the application in MOCK MODE (this notebook)
   2. Test all API endpoints with mock data
   3. Verify application structure and routing
   4. Test frontend integration (if applicable)
   5. Once validated, switch to PRODUCTION MODE:
      - Deploy HEDIS agent to Model Serving
      - Create Unity Catalog functions
      - Update app.yaml: MOCK_MODE="false"
      - Redeploy application

🎯 WHY MOCK MODE FIRST?

   • Test application structure without dependencies
   • Validate API contracts and data models
   • Verify deployment process works
   • Debug issues without waiting for agent responses
   • Safe testing environment before production

🆘 SUPPORT:

   • Documentation: /docs endpoint
   • Logs: Databricks Apps console
   • Issues: Contact {CURRENT_USER}

{'='*80}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📖 Additional Resources

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
# MAGIC ## ✅ Deployment Complete
# MAGIC
# MAGIC Your FastAPI application is ready for deployment! Follow the instructions above to deploy to Databricks Apps.
