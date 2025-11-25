# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Chat Application - Dual App Deployment
# MAGIC
# MAGIC Deploys BOTH frontend and backend as separate Databricks Apps:
# MAGIC - **Backend**: FastAPI application (app/backend/)
# MAGIC - **Frontend**: Next.js UI application (app/frontend/)

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
dbutils.widgets.text("backend_app_name", config.get("backend_app_name", "hedis-chat-backend"), "Backend App Name")
dbutils.widgets.text("frontend_app_name", config.get("frontend_app_name", "hedis-chat-frontend"), "Frontend App Name")
dbutils.widgets.text("agent_endpoint", config.get("agent_endpoint", "hedis_chat_agent"), "Agent Endpoint Name")
dbutils.widgets.dropdown("enable_auth", "Yes" if config.get("enable_auth", False) else "No", ["Yes", "No"], "Enable Authentication")
dbutils.widgets.text("allowed_users", config.get("allowed_users", ""), "Allowed Users (comma-separated, empty = all)")

# Get configuration from widgets
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
BACKEND_APP_NAME = dbutils.widgets.get("backend_app_name")
FRONTEND_APP_NAME = dbutils.widgets.get("frontend_app_name")
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
print(f"   Backend App: {BACKEND_APP_NAME}")
print(f"   Frontend App: {FRONTEND_APP_NAME}")
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
# MAGIC ## 🚀 Deploy Backend App (FastAPI)
# MAGIC
# MAGIC The backend must be deployed **first** to get its URL for frontend configuration.

# COMMAND ----------

try:
    import requests
    import time

    # Get authentication token
    api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    base_url = f"https://{WORKSPACE_URL}/api/2.0"

    print(f"🚀 Deploying Backend App: {BACKEND_APP_NAME}\n")

    # Check if backend app exists, create if it doesn't
    get_url = f"{base_url}/apps/{BACKEND_APP_NAME}"
    get_response = requests.get(get_url, headers=headers)

    if get_response.status_code == 404:
        # Create the backend app first
        create_payload = {"name": BACKEND_APP_NAME}
        create_response = requests.post(f"{base_url}/apps", headers=headers, json=create_payload)
        if create_response.status_code not in [200, 201]:
            raise Exception(f"Failed to create backend app: {create_response.text}")
        print(f"✅ Backend app created: {BACKEND_APP_NAME}")
    elif get_response.status_code == 200:
        # Check for active deployment
        app_info = get_response.json()
        current_state = app_info.get("status", {}).get("state", "")

        if current_state in ["DEPLOYING", "STARTING"]:
            print(f"⏳ Active backend deployment in progress (state: {current_state})")
            print(f"   Waiting for current deployment to complete...")

            # Wait for deployment to complete (max 5 minutes)
            max_wait = 300  # 5 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                time.sleep(10)
                status_response = requests.get(get_url, headers=headers)
                if status_response.status_code == 200:
                    app_status = status_response.json()
                    state = app_status.get("status", {}).get("state", "")

                    if state not in ["DEPLOYING", "STARTING"]:
                        print(f"✅ Previous deployment completed (state: {state})")
                        break
                    print(f"   Still deploying... ({int(time.time() - start_time)}s elapsed)")
            else:
                print(f"⚠️  Deployment still in progress after {max_wait}s")
                print(f"   Proceeding with new deployment anyway...")

    # Deploy the backend app - construct workspace path correctly
    backend_workspace_path = str(repo_root / "app" / "backend")
    if not backend_workspace_path.startswith("/Workspace"):
        backend_workspace_path = f"/Workspace{backend_workspace_path}"

    print(f"📂 Deploying backend from: {backend_workspace_path}")

    deploy_url = f"{base_url}/apps/{BACKEND_APP_NAME}/deployments"
    deploy_payload = {"source_code_path": backend_workspace_path, "mode": "SNAPSHOT"}
    deploy_response = requests.post(deploy_url, headers=headers, json=deploy_payload)

    if deploy_response.status_code in [200, 201]:
        deployment_info = deploy_response.json()
        print(f"✅ Backend deployment initiated: {deployment_info.get('deployment_id', 'N/A')}")

        # Check status
        time.sleep(3)
        status_response = requests.get(get_url, headers=headers)
        if status_response.status_code == 200:
            app_info = status_response.json()
            if app_info.get("status"):
                print(f"   Status: {app_info['status'].get('state', 'UNKNOWN')}")
    else:
        print(f"❌ Backend deployment failed: {deploy_response.status_code}")
        print(f"   {deploy_response.text}")
        raise Exception("Backend deployment failed: %s" % deploy_response.text)

except Exception as e:
    print(f"❌ Error: {e}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Wait for Backend to be Running
# MAGIC
# MAGIC We need the backend URL before deploying the frontend.

# COMMAND ----------

print(f"⏳ Waiting for backend to be RUNNING...\n")
print(f"   This may take 5-10 minutes for initial deployment\n")

max_wait_time = 900  # 15 minutes
check_interval = 30  # 30 seconds
elapsed_time = 0

backend_url = None
get_url = f"{base_url}/apps/{BACKEND_APP_NAME}"

while elapsed_time < max_wait_time:
    status_response = requests.get(get_url, headers=headers)

    if status_response.status_code == 200:
        app_info = status_response.json()
        state = app_info.get("status", {}).get("state", "UNKNOWN")

        print(f"   [{elapsed_time}s] Backend status: {state}")

        if state == "RUNNING":
            backend_url = app_info.get("url")
            print(f"\n✅ Backend is running!")
            print(f"   URL: {backend_url}")
            print(f"   Health: {backend_url}/health")
            print(f"   API Docs: {backend_url}/api/docs")
            break
        elif state in ["FAILED", "ERROR", "CRASHED"]:
            error_msg = app_info.get("status", {}).get("message", "Unknown error")
            print(f"\n❌ Backend deployment failed")
            print(f"   State: {state}")
            print(f"   Error: {error_msg}")
            print(f"\n💡 Debug: Check Compute > Apps > {BACKEND_APP_NAME} for logs")
            raise Exception(f"Backend deployment failed with state {state}: {error_msg}")

    time.sleep(check_interval)
    elapsed_time += check_interval
else:
    raise TimeoutError(f"Backend did not start within {max_wait_time} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy Frontend App (Next.js)
# MAGIC
# MAGIC Now we deploy the frontend with the backend URL configured.

# COMMAND ----------

try:
    print(f"🚀 Deploying Frontend App: {FRONTEND_APP_NAME}\n")

    # Check if frontend app exists, create if it doesn't
    get_url_frontend = f"{base_url}/apps/{FRONTEND_APP_NAME}"
    get_response_frontend = requests.get(get_url_frontend, headers=headers)

    if get_response_frontend.status_code == 404:
        # Create the frontend app first
        create_payload = {"name": FRONTEND_APP_NAME}
        create_response = requests.post(f"{base_url}/apps", headers=headers, json=create_payload)
        if create_response.status_code not in [200, 201]:
            raise Exception(f"Failed to create frontend app: {create_response.text}")
        print(f"✅ Frontend app created: {FRONTEND_APP_NAME}")
    elif get_response_frontend.status_code == 200:
        # Check for active deployment
        app_info = get_response_frontend.json()
        current_state = app_info.get("status", {}).get("state", "")

        if current_state in ["DEPLOYING", "STARTING"]:
            print(f"⏳ Active frontend deployment in progress (state: {current_state})")
            print(f"   Waiting for current deployment to complete...")

            # Wait for deployment to complete (max 5 minutes)
            max_wait = 300  # 5 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                time.sleep(10)
                status_response = requests.get(get_url_frontend, headers=headers)
                if status_response.status_code == 200:
                    app_status = status_response.json()
                    state = app_status.get("status", {}).get("state", "")

                    if state not in ["DEPLOYING", "STARTING"]:
                        print(f"✅ Previous deployment completed (state: {state})")
                        break
                    print(f"   Still deploying... ({int(time.time() - start_time)}s elapsed)")
            else:
                print(f"⚠️  Deployment still in progress after {max_wait}s")
                print(f"   Proceeding with new deployment anyway...")

    # Deploy the frontend app - construct workspace path correctly
    frontend_workspace_path = str(repo_root / "app" / "frontend")
    if not frontend_workspace_path.startswith("/Workspace"):
        frontend_workspace_path = f"/Workspace{frontend_workspace_path}"

    print(f"📂 Deploying frontend from: {frontend_workspace_path}")
    print(f"🔗 Backend API URL: {backend_url}")

    deploy_url_frontend = f"{base_url}/apps/{FRONTEND_APP_NAME}/deployments"

    # Include environment variable for backend URL
    deploy_payload_frontend = {
        "source_code_path": frontend_workspace_path,
        "mode": "SNAPSHOT"
    }

    deploy_response_frontend = requests.post(deploy_url_frontend, headers=headers, json=deploy_payload_frontend)

    if deploy_response_frontend.status_code in [200, 201]:
        deployment_info = deploy_response_frontend.json()
        print(f"✅ Frontend deployment initiated: {deployment_info.get('deployment_id', 'N/A')}")

        # Check status
        time.sleep(3)
        status_response = requests.get(get_url_frontend, headers=headers)
        if status_response.status_code == 200:
            app_info = status_response.json()
            if app_info.get("status"):
                print(f"   Status: {app_info['status'].get('state', 'UNKNOWN')}")
    else:
        print(f"❌ Frontend deployment failed: {deploy_response_frontend.status_code}")
        print(f"   {deploy_response_frontend.text}")
        raise Exception("Frontend deployment failed: %s" % deploy_response_frontend.text)

except Exception as e:
    print(f"❌ Error: {e}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Wait for Frontend to be Running

# COMMAND ----------

print(f"⏳ Waiting for frontend to be RUNNING...\n")
print(f"   This may take 5-10 minutes for initial deployment\n")
print(f"   Frontend needs to build the Next.js app first\n")

max_wait_time = 900  # 15 minutes
check_interval = 30  # 30 seconds
elapsed_time = 0

frontend_url = None
get_url_frontend = f"{base_url}/apps/{FRONTEND_APP_NAME}"

while elapsed_time < max_wait_time:
    status_response = requests.get(get_url_frontend, headers=headers)

    if status_response.status_code == 200:
        app_info = status_response.json()
        state = app_info.get("status", {}).get("state", "UNKNOWN")

        print(f"   [{elapsed_time}s] Frontend status: {state}")

        if state == "RUNNING":
            frontend_url = app_info.get("url")
            print(f"\n✅ Frontend is running!")
            print(f"   URL: {frontend_url}")
            break
        elif state in ["FAILED", "ERROR", "CRASHED"]:
            error_msg = app_info.get("status", {}).get("message", "Unknown error")
            print(f"\n❌ Frontend deployment failed")
            print(f"   State: {state}")
            print(f"   Error: {error_msg}")
            print(f"\n💡 Debug: Check Compute > Apps > {FRONTEND_APP_NAME} for logs")
            raise Exception(f"Frontend deployment failed with state {state}: {error_msg}")

    time.sleep(check_interval)
    elapsed_time += check_interval
else:
    raise TimeoutError(f"Frontend did not start within {max_wait_time} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Deployment Summary

# COMMAND ----------

print("\n" + "="*80)
print("🎉 DUAL APP DEPLOYMENT SUCCESSFUL")
print("="*80)

print(f"\n📍 Backend App (FastAPI API):")
print(f"   Name: {BACKEND_APP_NAME}")
print(f"   URL: {backend_url}")
print(f"   Health: {backend_url}/health")
print(f"   API Docs: {backend_url}/api/docs")

print(f"\n📍 Frontend App (Next.js UI):")
print(f"   Name: {FRONTEND_APP_NAME}")
print(f"   URL: {frontend_url}")
print(f"   👉 Access the application here: {frontend_url}")

print(f"\n🔧 Architecture:")
print(f"   User → Frontend ({frontend_url})")
print(f"        ↓ (API calls via Next.js rewrites)")
print(f"        → Backend ({backend_url})")
print(f"        ↓")
print(f"        → Databricks Resources")

print(f"\n💡 How It Works:")
print(f"   • Frontend serves the UI to users")
print(f"   • Frontend proxies /api/* requests to backend via Next.js rewrites")
print(f"   • Backend handles all API logic and Databricks integration")
print(f"   • Both apps scale independently")

print(f"\n📊 Monitoring:")
print(f"   • Backend: Compute > Apps > {BACKEND_APP_NAME}")
print(f"   • Frontend: Compute > Apps > {FRONTEND_APP_NAME}")

print(f"\n🧪 Testing:")
print(f"   1. Navigate to: {frontend_url}")
print(f"   2. The UI should load successfully")
print(f"   3. Create a new chat - frontend will call backend API")
print(f"   4. Verify backend responses work correctly")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Update Frontend Environment Variable (If Needed)
# MAGIC
# MAGIC If the frontend can't reach the backend, you may need to explicitly set the environment variable.

# COMMAND ----------

print(f"⚙️  Backend URL Configuration:\n")
print(f"The frontend app.yaml should have:")
print(f"  NEXT_PUBLIC_API_URL: {backend_url}")
print(f"\nThis is configured in: app/frontend/app.yaml")
print(f"\nIf you need to update it, modify the app.yaml and redeploy frontend.")

# To update environment variable and redeploy frontend:
# 1. Update app/frontend/app.yaml with correct BACKEND_API_URL
# 2. Re-run the frontend deployment cell above

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Monitoring View

# COMMAND ----------

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

print(f"✅ Monitoring view: {CATALOG_NAME}.{SCHEMA_NAME}.app_monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Troubleshooting
# MAGIC
# MAGIC ### Frontend Can't Reach Backend
# MAGIC
# MAGIC 1. Check CORS settings in `backend/main.py`
# MAGIC 2. Verify `NEXT_PUBLIC_API_URL` in frontend app.yaml
# MAGIC 3. Test backend health: `{backend_url}/health`
# MAGIC 4. Check backend logs in Databricks Apps console
# MAGIC
# MAGIC ### Build Failures
# MAGIC
# MAGIC **Backend:**
# MAGIC - Check `app/backend/requirements.txt` dependencies
# MAGIC - Verify Python version compatibility
# MAGIC - Review deployment logs
# MAGIC
# MAGIC **Frontend:**
# MAGIC - Check `app/frontend/package.json` dependencies
# MAGIC - Ensure Node.js version is compatible
# MAGIC - Verify Next.js build succeeds locally
# MAGIC
# MAGIC ### App Not Starting
# MAGIC
# MAGIC 1. Check resource allocation in app.yaml files
# MAGIC 2. Verify health check endpoints respond correctly
# MAGIC 3. Review startup logs in Apps console
# MAGIC
# MAGIC ## 🔄 Redeployment
# MAGIC
# MAGIC To redeploy after making changes:
# MAGIC - **Backend only**: Re-run the "Deploy Backend App" cell
# MAGIC - **Frontend only**: Re-run the "Deploy Frontend App" cell
# MAGIC - **Both apps**: Re-run both deployment cells in order
# MAGIC
# MAGIC ## 🗑️ Cleanup
# MAGIC
# MAGIC To delete the apps, run in a new cell:
# MAGIC ```python
# MAGIC # Delete frontend
# MAGIC requests.delete(f"{base_url}/apps/{FRONTEND_APP_NAME}", headers=headers)
# MAGIC
# MAGIC # Delete backend
# MAGIC requests.delete(f"{base_url}/apps/{BACKEND_APP_NAME}", headers=headers)
# MAGIC ```
