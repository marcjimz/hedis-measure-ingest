# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Chat Agent - End-to-End Deployment
# MAGIC
# MAGIC Build and deploy a LangGraph agent for HEDIS measure analysis on Databricks.
# MAGIC
# MAGIC **Tech Stack:**
# MAGIC - 🏢 **Databricks Foundation Models** - Pay-per-token LLM endpoints (Sonnet 4.5)
# MAGIC - 🗄️ **Lakebase (Postgres)** - Persistent conversation management
# MAGIC - 🔄 **LangGraph** - Stateful agent workflows
# MAGIC - 📊 **MLflow** - Model tracking and deployment
# MAGIC - 🔧 **Unity Catalog Functions** - Vector search over HEDIS chunks

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
# MAGIC Configure your Databricks environment.

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
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksLakebase, DatabricksFunction, DatabricksVectorSearchIndex

# Add src directory to Python path
repo_root = pathlib.Path().absolute().parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

import yaml

# Load configuration from config.yaml
try:
    with open("../config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    # Fallback for different execution contexts
    with open("/Workspace/Repos/hedis-measure-ingest/notebooks/config.yaml", "r") as f:
        config = yaml.safe_load(f)

# Create configuration widgets with config values as defaults
dbutils.widgets.text("catalog_name", config.get("catalog_name", "main"), "Catalog")
dbutils.widgets.text("schema_name", config.get("schema_name", "hedis_measurements"), "Schema")
dbutils.widgets.text("llm_endpoint", config.get("llm_endpoint", "databricks-claude-opus-4-1"), "Model Serving Endpoint")
dbutils.widgets.text("effective_year", config.get("effective_year", ""), "Effective Year (optional - auto-detected if empty)")
dbutils.widgets.dropdown("enable_persistence", config.get("enable_persistence", "No"), ["Yes", "No"], "Enable Persistence")
dbutils.widgets.text("lakebase_instance", config.get("lakebase_instance", "hedis-agent-pg"), "Lakebase Instance (if persistence enabled)")
dbutils.widgets.text("vector_index_name", config.get("vector_index_name", "hedis_measures_index"), "Vector Search Index Name")

# Get configuration from widgets (widgets override config if changed)
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
ENDPOINT_NAME = dbutils.widgets.get("llm_endpoint")
EFFECTIVE_YEAR_STR = dbutils.widgets.get("effective_year")
EFFECTIVE_YEAR = int(EFFECTIVE_YEAR_STR) if EFFECTIVE_YEAR_STR else None
ENABLE_PERSISTENCE = dbutils.widgets.get("enable_persistence") == "Yes"
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
VS_INDEX_NAME = dbutils.widgets.get("vector_index_name")

# Construct full vector search index path from catalog and schema
VS_INDEX = f"{CATALOG_NAME}.{SCHEMA_NAME}.{VS_INDEX_NAME}"

# Set environment variables for LOCAL agent testing (not needed for deployed agent)
os.environ["ENDPOINT_NAME"] = ENDPOINT_NAME
os.environ["UC_CATALOG"] = CATALOG_NAME
os.environ["UC_SCHEMA"] = SCHEMA_NAME
if EFFECTIVE_YEAR:
    os.environ["EFFECTIVE_YEAR"] = str(EFFECTIVE_YEAR)

print(f"✅ Environment configured:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema: {SCHEMA_NAME}")
print(f"   Endpoint: {ENDPOINT_NAME}")
print(f"   Effective Year: {EFFECTIVE_YEAR or 'Auto-detect'}")
print(f"   Persistence: {ENABLE_PERSISTENCE}")

if ENABLE_PERSISTENCE:
    print(f"   Lakebase Instance: {LAKEBASE_INSTANCE}")

mlflow.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Lakebase (Postgres) Setup
# MAGIC
# MAGIC Initialize Lakebase connection for conversation persistence (if enabled).

# COMMAND ----------

connection_pool = None

if ENABLE_PERSISTENCE and LAKEBASE_INSTANCE:
    from database.lakebase import LakebaseDatabase

    # Use passthrough authentication - credentials generated automatically via SDK
    w = WorkspaceClient()

    lb_conn = LakebaseDatabase()  # Uses default authentication (current user's token)
    lb_conn.initialize_connection(
        user=w.current_user.me().user_name,
        instance_name=LAKEBASE_INSTANCE,
        setup_checkpointer=True  # Setup tables using the connection pool
    )

    # Get the connection pool (has automatic credential refresh)
    connection_pool = lb_conn.get_connection_pool()

    print(f"✅ Lakebase connection established!")
    print(f"   Instance: {LAKEBASE_INSTANCE}")
    print(f"   User: {w.current_user.me().user_name}")
    print(f"   Persistence: ENABLED")
    print(f"   Connection pool: {connection_pool is not None}")
else:
    print("ℹ️  Persistence disabled - agent will run in stateless mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Initialize the Agent
# MAGIC
# MAGIC Import and initialize the HEDIS Chat Agent.

# COMMAND ----------

from src.agents.hedis_chat import HEDISChatAgentFactory

# Create the agent with persistence enabled (if configured)
agent = HEDISChatAgentFactory.create(
    endpoint_name=ENDPOINT_NAME,
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    connection_pool=connection_pool,
    enable_persistence=ENABLE_PERSISTENCE,
    effective_year=EFFECTIVE_YEAR
)

print("\n✅ HEDIS Chat Agent Created!")
print(f"  - Effective Year: {agent.effective_year}")
print(f"  - Tools: measures_document_search")
print(f"  - Persistence: {'ENABLED' if ENABLE_PERSISTENCE else 'DISABLED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Local Agent
# MAGIC
# MAGIC Test the agent locally before deploying to ensure it's working correctly.

# COMMAND ----------

# Enable MLflow tracing for interactive testing
mlflow.autolog()

# Set the agent as the current model for this session
mlflow.models.set_model(agent)

print("✅ Using initialized HEDIS Chat Agent")
print(f"Agent type: {type(agent).__name__}")
print("✅ MLflow tracing enabled - traces will appear as widgets below predictions")

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
    print("To enable persistence, set 'Enable Persistence' widget to 'Yes' and provide a Lakebase instance.")

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
# This config is saved with the model and read by the agent at deployment time
agent_config = {
    "endpoint_name": ENDPOINT_NAME,
    "catalog_name": CATALOG_NAME,
    "schema_name": SCHEMA_NAME,
    "effective_year": agent.effective_year,  # Used as default filter_year for measures_document_search
}

# Create resources list - includes serving endpoint and optionally Lakebase
resources = [
    DatabricksServingEndpoint(endpoint_name=ENDPOINT_NAME),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
    DatabricksFunction(function_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.measures_document_search"),
]

if ENABLE_PERSISTENCE:
    # Add Lakebase resource for agent passthrough authentication
    resources.append(DatabricksLakebase(database_instance_name=LAKEBASE_INSTANCE))
    agent_config["lakebase_instance"] = LAKEBASE_INSTANCE
    agent_config["enable_persistence"] = True
    print(f"ℹ️  Lakebase resource added: {LAKEBASE_INSTANCE}")
    print(f"   Deployed agent will use passthrough authentication")

# Define input example
input_example = {
    "messages": [
        {"role": "user", "content": "What is the BCS measure?"}
    ]
}

# Read requirements
with open(f'{repo_root}/requirements.txt', 'r') as file:
    requirements = [x for x in file.read().splitlines() if len(x) > 0 and x[0] != '#']

# Log the agent as an MLflow model (NEW way - no mlflow.start_run())
model_info = mlflow.pyfunc.log_model(
    artifact_path=agent_name,
    python_model=agent_path,
    model_config=agent_config,
    resources=resources,
    input_example=input_example,
    pip_requirements=requirements,
    code_paths=[str(src_path)]
)

print(f"\n✅ Agent logged to MLflow with Unity Catalog")
print(f"   Model URI: {model_info.model_uri}")
print(f"   Artifact Path: {model_info.artifact_path}")
print(f"   Registry URI: {mlflow.get_registry_uri()}")
print(f"   Resources: {len(resources)} resource(s) declared")

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
# predictions = loaded_model.predict(input_example)
# print("✅ Model loaded and tested successfully!")
# print("\nSample prediction:")
# print(predictions['messages'][-1]['content'][:200] + "...")

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
#test_prediction = loaded_model_uc.predict(input_example)

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
# MAGIC Set environment variables for the deployed agent.

# COMMAND ----------

envvars = {
    "ENDPOINT_NAME": ENDPOINT_NAME,
    "UC_CATALOG": CATALOG_NAME,
    "UC_SCHEMA": SCHEMA_NAME,
}

# EFFECTIVE_YEAR can be set via env var (takes precedence) or model_config
# Priority: ENV > model_config > auto-detect from database
if EFFECTIVE_YEAR:
    envvars["EFFECTIVE_YEAR"] = str(EFFECTIVE_YEAR)

print("Environment variables configured for deployment:")
for key, value in envvars.items():
    print(f"   {key}: {value}")

if agent.effective_year and "EFFECTIVE_YEAR" not in envvars:
    print(f"   effective_year will be read from model_config: {agent.effective_year}")

if ENABLE_PERSISTENCE:
    print(f"\nℹ️  Lakebase resource will be added to deployment:")
    print(f"   Instance: {LAKEBASE_INSTANCE}")

# COMMAND ----------

from databricks import agents

# Deploy the agent
deployment_info = agents.deploy(
    model_name=uc_model_fqn,
    model_version=uc_registered_model_info.version,
    scale_to_zero=False,
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
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate
import time


print("\n🧪 Testing deployed endpoint...")
deploy_client = get_deploy_client()

try:
    # Query the deployed agent
    response = deploy_client.predict(
        endpoint=deployment_info.endpoint_name,
        inputs=input_example
    )
    
    print("✅ Production endpoint test successful!")
    print(f"\nAgent Response:")
    print(response.messages[-1]['content'])
except Exception as e:
    print(f"❌ Error testing endpoint: {e}")
    raise e

# COMMAND ----------


