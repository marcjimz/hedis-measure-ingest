# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Chat Agent - End-to-End Deployment
# MAGIC
# MAGIC Build and deploy a LangGraph agent for HEDIS measure analysis on Databricks.
# MAGIC
# MAGIC **Tech Stack:**
# MAGIC - 🏢 **Databricks Foundation Models** - Pay-per-token LLM endpoints
# MAGIC - 🗄️ **Lakebase (Postgres)** - Persistent conversation management
# MAGIC - 🔄 **LangGraph** - Stateful agent workflows
# MAGIC - 📊 **MLflow** - Model tracking and deployment
# MAGIC - 🔧 **Unity Catalog Functions** - Measure lookup, vector search, query expansion

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Requirements
# MAGIC
# MAGIC Install all necessary packages for agent development and deployment.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration & Setup
# MAGIC
# MAGIC Configure your Databricks environment. The widgets below allow you to customize:
# MAGIC - Catalog/schema for Unity Catalog functions
# MAGIC - Lakebase instance for conversation persistence (optional)
# MAGIC - Model serving endpoint selection
# MAGIC - Effective year (optional override - auto-detected from data if not provided)

# COMMAND ----------

import os
import sys
import pathlib
import uuid
import json
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient
import mlflow
import mlflow.pyfunc
from mlflow.models.resources import DatabricksServingEndpoint

# Add src directory to Python path
repo_root = pathlib.Path().absolute().parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

# Create configuration widgets
dbutils.widgets.text("catalog_name", "main", "Catalog")
dbutils.widgets.text("schema_name", "hedis_measurements", "Schema")
dbutils.widgets.text("endpoint_name", "databricks-claude-opus-4-1", "Model Serving Endpoint")
dbutils.widgets.text("effective_year", "", "Effective Year (optional - auto-detected if empty)")
dbutils.widgets.dropdown("enable_persistence", "No", ["Yes", "No"], "Enable Persistence")
dbutils.widgets.text("lakebase_instance", "hedis-agent-persistence", "Lakebase Instance (if persistence enabled)")
dbutils.widgets.text("db_client_id", "", "Databricks Client ID (if persistence enabled)")
dbutils.widgets.text("db_client_secret", "", "Databricks Client Secret (if persistence enabled)")

# Get configuration from widgets
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
EFFECTIVE_YEAR_STR = dbutils.widgets.get("effective_year")
EFFECTIVE_YEAR = int(EFFECTIVE_YEAR_STR) if EFFECTIVE_YEAR_STR else None
ENABLE_PERSISTENCE = dbutils.widgets.get("enable_persistence") == "Yes"
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
DATABRICKS_CLIENT_ID = dbutils.widgets.get("db_client_id")
DATABRICKS_CLIENT_SECRET = dbutils.widgets.get("db_client_secret")
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# Set environment variables for agent initialization
os.environ["ENDPOINT_NAME"] = ENDPOINT_NAME
os.environ["UC_CATALOG"] = CATALOG_NAME
os.environ["UC_SCHEMA"] = SCHEMA_NAME
if EFFECTIVE_YEAR:
    os.environ["EFFECTIVE_YEAR"] = str(EFFECTIVE_YEAR)
if ENABLE_PERSISTENCE:
    os.environ["ENABLE_PERSISTENCE"] = "true"
    os.environ["LAKEBASE_INSTANCE"] = LAKEBASE_INSTANCE
    os.environ["LAKEBASE_USER"] = DATABRICKS_CLIENT_ID
    os.environ["DATABRICKS_CLIENT_ID"] = DATABRICKS_CLIENT_ID
    os.environ["DATABRICKS_CLIENT_SECRET"] = DATABRICKS_CLIENT_SECRET
    os.environ["DATABRICKS_HOST"] = f"https://{WORKSPACE_URL}"

print(f"✅ Environment configured:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema: {SCHEMA_NAME}")
print(f"   Endpoint: {ENDPOINT_NAME}")
print(f"   Effective Year: {EFFECTIVE_YEAR or 'Auto-detect'}")
print(f"   Persistence: {ENABLE_PERSISTENCE}")
if ENABLE_PERSISTENCE:
    print(f"   Lakebase Instance: {LAKEBASE_INSTANCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Lakebase (Postgres) Setup
# MAGIC
# MAGIC Initialize the Lakebase connection if persistence is enabled. This allows the agent to maintain conversation state across sessions.

# COMMAND ----------

conn_string = None

if ENABLE_PERSISTENCE and LAKEBASE_INSTANCE and DATABRICKS_CLIENT_ID:
    from database.lakebase import LakebaseDatabase
    from langgraph.checkpoint.postgres import PostgresSaver

    lb_conn = LakebaseDatabase(host=f"https://{WORKSPACE_URL}")
    conn_string = lb_conn.initialize_connection(
        user=DATABRICKS_CLIENT_ID,
        instance_name=LAKEBASE_INSTANCE
    )

    # Setup checkpointer tables
    with PostgresSaver.from_conn_string(conn_string) as checkpointer:
        checkpointer.setup()

    print(f"✅ Lakebase connection established!")
    print(f"   Instance: {LAKEBASE_INSTANCE}")
    print(f"   Persistence: ENABLED")
elif ENABLE_PERSISTENCE:
    print("⚠️  Persistence enabled but configuration incomplete. Continuing without persistence.")
    ENABLE_PERSISTENCE = False
else:
    print("ℹ️  Persistence disabled (stateless mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Initialize the Agent
# MAGIC
# MAGIC Import and initialize the HEDIS Chat Agent. The agent will automatically:
# MAGIC - Detect the latest effective_year from your data (unless overridden)
# MAGIC - Configure Unity Catalog functions for measure lookup and search
# MAGIC - Set up conversation persistence if enabled

# COMMAND ----------

from agents.hedis_chat import HEDISChatAgentFactory

# Create the agent
agent = HEDISChatAgentFactory.create(
    endpoint_name=ENDPOINT_NAME,
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    conn_string=conn_string,
    enable_persistence=ENABLE_PERSISTENCE,
    effective_year=EFFECTIVE_YEAR
)

print("\n✅ HEDIS Chat Agent Created!")
print(f"  - Effective Year: {agent.effective_year}")
print(f"  - Tools: measures_definition_lookup, measures_document_search, measures_search_expansion")
print(f"  - Persistence: {'ENABLED' if ENABLE_PERSISTENCE else 'DISABLED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Local Agent
# MAGIC
# MAGIC Test the agent locally before deploying to ensure it's working correctly.

# COMMAND ----------

# Set the agent as the current model for this session
mlflow.models.set_model(agent)

print("✅ Using initialized HEDIS Chat Agent")
print(f"Agent type: {type(agent).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Example 1: Basic Question About a Measure
# MAGIC
# MAGIC Ask the agent about a specific HEDIS measure.

# COMMAND ----------

response = agent.predict({
    "messages": [
        {"role": "user", "content": "What is the Breast Cancer Screening (BCS) measure? Explain the denominator and numerator criteria."}
    ]
})

thread_id = response.custom_outputs.get('thread_id')
print(f"Thread ID: {thread_id}")
print("\nAgent Response:")
print(response.messages[-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💾 Example 2: Test Conversation Persistence
# MAGIC
# MAGIC Verify that the agent remembers previous messages using the thread ID (if persistence enabled).

# COMMAND ----------

if ENABLE_PERSISTENCE:
    time.sleep(2)  # Allow persistence to complete

    followup_response = agent.predict(
        messages=[
            {"role": "user", "content": "What was my previous question about?"}
        ],
        custom_inputs={"thread_id": thread_id}
    )

    print("Agent Response:")
    print(followup_response.messages[-1].content)
else:
    print("Persistence not enabled. Skipping multi-turn conversation test.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🌊 Example 3: Streaming Response
# MAGIC
# MAGIC Demonstrate streaming capabilities for real-time responses.

# COMMAND ----------

print("User: What are the exclusions for the Colorectal Cancer Screening (COL) measure?\n")
print("Agent (streaming): ", end='')

for chunk in agent.predict_stream(
    messages=[
        {"role": "user", "content": "What are the exclusions for the Colorectal Cancer Screening (COL) measure?"}
    ]
):
    if hasattr(chunk, 'delta') and chunk.delta:
        if isinstance(chunk.delta, dict):
            if 'content' in chunk.delta and chunk.delta['content']:
                print(chunk.delta['content'], end='', flush=True)

print("\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Register Agent with MLflow
# MAGIC
# MAGIC Log the agent to MLflow for tracking and deployment. The agent includes all necessary dependencies and configuration.

# COMMAND ----------

# Configure MLflow with Unity Catalog
agent_name = "hedis-chat-agent"
mlflow.set_experiment(f"/Shared/{agent_name}")
mlflow.set_registry_uri('databricks-uc')

# Define agent_path variable
agent_path = "../../src/agents/hedis_chat.py"

# Create agent configuration
agent_config = {
    "endpoint_name": ENDPOINT_NAME,
    "catalog_name": CATALOG_NAME,
    "schema_name": SCHEMA_NAME,
    "effective_year": agent.effective_year,
}

# Create resources list with DatabricksServingEndpoint
resources = [DatabricksServingEndpoint(endpoint_name=ENDPOINT_NAME)]

# Define input example
input_example = {
    "messages": [
        {"role": "user", "content": "What is the BCS measure?"}
    ]
}

# Read requirements
with open(f'{repo_root}/requirements.txt', 'r') as file:
    requirements = [x for x in file.read().splitlines() if len(x) > 0 and x[0] != '#']

# Log the agent as an MLflow model
model_info = mlflow.pyfunc.log_model(
    artifact_path=agent_name,
    python_model=agent_path,
    model_config=agent_config,
    resources=resources,
    input_example=input_example,
    pip_requirements=requirements,
    code_paths=[str(src_path)]
)

print(f"✅ Agent logged to MLflow with Unity Catalog")
print(f"   Model URI: {model_info.model_uri}")
print(f"   Artifact Path: {model_info.artifact_path}")
print(f"   Registry URI: {mlflow.get_registry_uri()}")

# Store model URI for later use
MODEL_URI = model_info.model_uri

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Model Registration
# MAGIC
# MAGIC Quick sanity check to ensure the model deserializes correctly from MLflow.

# COMMAND ----------

os.environ['MLFLOW_ARTIFACT_UPLOAD_DOWNLOAD_TIMEOUT'] = '600'
loaded_model = mlflow.pyfunc.load_model(MODEL_URI)

# Run prediction on input_example
predictions = loaded_model.predict(input_example)
print("✅ Model loaded and tested successfully!")
print("\nSample prediction:")
print(predictions['messages'][-1]['content'][:200] + "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Register to Unity Catalog
# MAGIC
# MAGIC Register the model in Unity Catalog for centralized management and versioning.

# COMMAND ----------

from mlflow import MlflowClient

# Unity Catalog location
uc_model_fqn = f"{CATALOG_NAME}.{SCHEMA_NAME}.{agent_name}".replace("-", "_")

# Register the model to Unity Catalog
uc_registered_model_info = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=uc_model_fqn
)

# Initialize the MLflow client and set alias
client = MlflowClient()
client.set_registered_model_alias(
    name=uc_model_fqn,
    alias="production",
    version=uc_registered_model_info.version
)

print(f"✅ Model registered to Unity Catalog")
print(f"   Model Name: {uc_model_fqn}")
print(f"   Version: {uc_registered_model_info.version}")
print(f"   Alias: production")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Load from Unity Catalog
# MAGIC
# MAGIC Verify we can load the model from UC using the alias.

# COMMAND ----------

model_uri_uc = f"models:/{uc_model_fqn}@production"
loaded_model_uc = mlflow.pyfunc.load_model(model_uri_uc)
test_prediction = loaded_model_uc.predict(input_example)

print(f"✅ Model loaded from Unity Catalog successfully!")
print(f"   URI: {model_uri_uc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy Agent to Production
# MAGIC
# MAGIC Deploy the agent as a Model Serving endpoint for production use. This creates a REST API and Review App interface.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔐 Configure Environment Variables
# MAGIC
# MAGIC Set all required environment variables for the deployed agent to function properly.

# COMMAND ----------

envvars = {
    "ENDPOINT_NAME": ENDPOINT_NAME,
    "UC_CATALOG": CATALOG_NAME,
    "UC_SCHEMA": SCHEMA_NAME,
}

if EFFECTIVE_YEAR:
    envvars["EFFECTIVE_YEAR"] = str(EFFECTIVE_YEAR)

if ENABLE_PERSISTENCE:
    envvars.update({
        "ENABLE_PERSISTENCE": "true",
        "LAKEBASE_INSTANCE": LAKEBASE_INSTANCE,
        "LAKEBASE_USER": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
        "DATABRICKS_CLIENT_SECRET": DATABRICKS_CLIENT_SECRET,
        "DATABRICKS_HOST": f"https://{WORKSPACE_URL}",
    })

print("Environment variables configured for deployment:")
for key in envvars:
    if "SECRET" in key or "TOKEN" in key:
        print(f"   {key}: ***")
    else:
        print(f"   {key}: {envvars[key]}")

# COMMAND ----------

from databricks import agents

# Deploy the agent
deployment_info = agents.deploy(
    model_name=uc_model_fqn,
    model_version=uc_registered_model_info.version,
    scale_to_zero=True,
    resources=resources,
    environment_vars=envvars,
)

print(f"""
✅ Deployment of {deployment_info.model_name} version {deployment_info.model_version} initiated!

This can take up to 15 minutes. The Review App & REST API will not work until deployment finishes.

View status: https://{WORKSPACE_URL}/ml/endpoints/{deployment_info.endpoint_name}
Review App: {deployment_info.review_app_url}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✨ Test Production Deployment
# MAGIC
# MAGIC Verify the deployed endpoint is working correctly by making a test prediction.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

deploy_client = get_deploy_client()

# Query the deployed agent
response = deploy_client.predict(
    endpoint=deployment_info.endpoint_name,
    inputs=input_example
)

print("✅ Production endpoint test successful!")
print(f"\nAgent Response:")
print(response.messages[-1]['content'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC **HEDIS Chat Agent Deployment Complete!**
# MAGIC
# MAGIC The agent has been:
# MAGIC 1. ✅ Configured with Unity Catalog functions
# MAGIC 2. ✅ Tested locally with conversation persistence
# MAGIC 3. ✅ Logged to MLflow experiment
# MAGIC 4. ✅ Registered to Unity Catalog
# MAGIC 5. ✅ Deployed to Model Serving endpoint
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Answer questions about HEDIS measures
# MAGIC - Semantic search over HEDIS documentation
# MAGIC - AI-powered query expansion
# MAGIC - Conversation persistence (optional)
# MAGIC - Streaming responses
# MAGIC - Auto-detection of latest effective year
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Test the Review App in the Databricks UI
# MAGIC - Integrate with your applications via REST API
# MAGIC - Monitor inference logs in auto-capture tables
# MAGIC - Collect feedback for continuous improvement
# MAGIC
# MAGIC **API Endpoint:**
# MAGIC ```
# MAGIC POST https://{WORKSPACE_URL}/serving-endpoints/{deployment_info.endpoint_name}/invocations
# MAGIC ```
