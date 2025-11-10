# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Chat Agent - Deployment & Interactive Demo
# MAGIC
# MAGIC This notebook demonstrates the **HEDIS Chat Agent** - a production-ready LangGraph-based agent with two modes:
# MAGIC
# MAGIC 1. **QnA Mode**: Answer questions about HEDIS measures using vector search and measure definitions
# MAGIC 2. **Compliance Mode**: Evaluate patient encounters for HEDIS measure compliance
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Automatic intent detection (QnA vs Compliance)
# MAGIC - Patient data extraction for compliance checks
# MAGIC - Integration with Unity Catalog tools (vector search, measure lookup)
# MAGIC - Optional PostgreSQL persistence for conversation history
# MAGIC - Streaming and non-streaming responses
# MAGIC - MLflow logging and Model Serving deployment
# MAGIC
# MAGIC **Demo Flow:**
# MAGIC 1. Install dependencies
# MAGIC 2. Configure agent (widgets for customization)
# MAGIC 3. Run interactive examples
# MAGIC 4. Log agent to MLflow
# MAGIC 5. Deploy to Model Serving endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configuration
# MAGIC
# MAGIC Configure the agent using Databricks widgets. Set your catalog, schema, and optional Lakebase settings.

# COMMAND ----------

# Create configuration widgets
dbutils.widgets.text("catalog_name", "main", "UC Catalog")
dbutils.widgets.text("schema_name", "hedis_measurements", "UC Schema")
dbutils.widgets.text("endpoint_name", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")
dbutils.widgets.text("effective_year", "2025", "HEDIS Year")
dbutils.widgets.dropdown("enable_persistence", "No", ["Yes", "No"], "Enable Persistence")
dbutils.widgets.text("lakebase_instance", "", "Lakebase Instance (optional)")

# Get configuration
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
endpoint_name = dbutils.widgets.get("endpoint_name")
effective_year = int(dbutils.widgets.get("effective_year"))
enable_persistence = dbutils.widgets.get("enable_persistence") == "Yes"
lakebase_instance = dbutils.widgets.get("lakebase_instance")

print("Configuration:")
print(f"  - Catalog: {catalog_name}")
print(f"  - Schema: {schema_name}")
print(f"  - Endpoint: {endpoint_name}")
print(f"  - HEDIS Year: {effective_year}")
print(f"  - Persistence: {enable_persistence}")
if lakebase_instance:
    print(f"  - Lakebase Instance: {lakebase_instance}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Import Agent and Initialize

# COMMAND ----------

import sys
sys.path.append("../../src")

from agents.hedis_chat import HEDISChatAgent, HEDISChatAgentFactory
from mlflow.types.agent import ChatAgentMessage
import json

print("HEDIS Chat Agent imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Set Up Lakebase Connection (Optional)
# MAGIC
# MAGIC If persistence is enabled, initialize the Lakebase PostgreSQL connection for conversation history.

# COMMAND ----------

conn_string = None

if enable_persistence and lakebase_instance:
    from database.lakebase import LakebaseDatabase
    from databricks.sdk import WorkspaceClient
    import os

    # Get Databricks configuration
    w = WorkspaceClient()
    host = w.config.host
    client_id = os.getenv("DATABRICKS_CLIENT_ID")

    if not client_id:
        print("WARNING: DATABRICKS_CLIENT_ID not set. Persistence disabled.")
        enable_persistence = False
    else:
        try:
            # Initialize Lakebase connection
            lakebase_db = LakebaseDatabase(host=host)

            # Initialize connection with checkpointer setup
            conn_string = lakebase_db.initialize_connection(
                user=client_id,
                instance_name=lakebase_instance,
                database="databricks_postgres",
                setup_checkpointer=True  # Creates necessary tables
            )

            print(f"Lakebase connection established!")
            print(f"  - Instance: {lakebase_instance}")
            print(f"  - Database: databricks_postgres")
            print(f"  - Persistence: ENABLED")

        except Exception as e:
            print(f"Failed to initialize Lakebase: {str(e)}")
            print("Continuing without persistence...")
            enable_persistence = False
            conn_string = None
elif enable_persistence and not lakebase_instance:
    print("WARNING: Persistence enabled but no Lakebase instance specified. Persistence disabled.")
    enable_persistence = False
else:
    print("Persistence disabled (stateless mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Agent Instance
# MAGIC
# MAGIC Initialize the HEDIS Chat Agent with Unity Catalog tools.

# COMMAND ----------

# Create the agent using the factory
agent = HEDISChatAgentFactory.create(
    endpoint_name=endpoint_name,
    catalog_name=catalog_name,
    schema_name=schema_name,
    conn_string=conn_string,
    enable_persistence=enable_persistence,
    effective_year=effective_year,
    default_mode="QNA_MODE"
)

print("HEDIS Chat Agent Created!")
print(f"  - Mode: Dual (QnA + Compliance with auto-detection)")
print(f"  - Tools: measures_definition_lookup, measures_document_search, measures_search_expansion")
print(f"  - Persistence: {'ENABLED' if enable_persistence else 'DISABLED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Interactive Examples
# MAGIC
# MAGIC Run 5-6 interactive examples showing different agent capabilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Simple QnA - Ask About a Measure
# MAGIC
# MAGIC Test the agent's ability to answer general questions about HEDIS measures.

# COMMAND ----------

# Example 1: QnA Mode - General Question
print("=" * 80)
print("EXAMPLE 1: QnA Mode - What is the BCS measure?")
print("=" * 80)

messages = [
    ChatAgentMessage(
        role="user",
        content="What is the Breast Cancer Screening (BCS) measure? Explain the initial population and numerator criteria."
    )
]

response = agent.predict(messages=messages)

print("\n--- USER ---")
print(messages[0].content)

print("\n--- AGENT RESPONSE ---")
for msg in response.messages:
    if msg.role == "assistant" and msg.content:
        print(msg.content)

print("\n--- METADATA ---")
if hasattr(response, 'custom_outputs') and response.custom_outputs:
    print(f"Mode: {response.custom_outputs.get('mode')}")
    print(f"Intent Detection: {response.custom_outputs.get('intent_data', {}).get('reasoning')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Compliance Check with Patient Data
# MAGIC
# MAGIC Test compliance mode by providing patient encounter data.

# COMMAND ----------

# Example 2: Compliance Mode - Evaluate Patient Encounter
print("=" * 80)
print("EXAMPLE 2: Compliance Mode - Evaluate BCS Compliance")
print("=" * 80)

compliance_request = """
Evaluate this patient for BCS compliance:

Patient Demographics:
- Female, DOB: January 15, 1960 (age 64)
- Enrollment: Continuously enrolled 1/1/2025 - 12/31/2025

Encounter:
- Date: March 20, 2025
- Service: Screening mammogram
- CPT Code: 77067
- Setting: Outpatient radiology

No history of mastectomy or other exclusions.

Is this patient compliant with HEDIS BCS for 2025?
"""

messages = [
    ChatAgentMessage(
        role="user",
        content=compliance_request
    )
]

response = agent.predict(messages=messages)

print("\n--- USER ---")
print(compliance_request.strip())

print("\n--- AGENT RESPONSE ---")
for msg in response.messages:
    if msg.role == "assistant" and msg.content:
        print(msg.content)

print("\n--- METADATA ---")
if hasattr(response, 'custom_outputs') and response.custom_outputs:
    print(f"Mode: {response.custom_outputs.get('mode')}")
    print(f"Intent Detection: {response.custom_outputs.get('intent_data', {}).get('reasoning')}")
    if response.custom_outputs.get('patient_data'):
        print(f"\nExtracted Patient Data:")
        print(json.dumps(response.custom_outputs['patient_data'], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Streaming Response
# MAGIC
# MAGIC Demonstrate streaming capabilities for real-time responses.

# COMMAND ----------

# Example 3: Streaming Response
print("=" * 80)
print("EXAMPLE 3: Streaming Response")
print("=" * 80)

messages = [
    ChatAgentMessage(
        role="user",
        content="What are the denominator exclusions for the Diabetes HbA1c Control (HBD) measure?"
    )
]

print("\n--- USER ---")
print(messages[0].content)

print("\n--- AGENT STREAMING RESPONSE ---")

# Stream the response
full_response = ""
for chunk in agent.predict_stream(messages=messages):
    if hasattr(chunk, 'delta') and chunk.delta:
        if isinstance(chunk.delta, dict):
            # Check for content in the delta
            if 'content' in chunk.delta and chunk.delta['content']:
                print(chunk.delta['content'], end='', flush=True)
                full_response += chunk.delta['content']
            # Check for custom_outputs (metadata at the end)
            elif 'custom_outputs' in chunk.delta:
                print("\n\n--- METADATA ---")
                print(f"Mode: {chunk.delta['custom_outputs'].get('mode')}")

print("\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Persistence Enabled - Multi-Turn Conversation
# MAGIC
# MAGIC Show conversation history with persistence (if enabled).

# COMMAND ----------

# Example 4: Persistence - Multi-Turn Conversation
print("=" * 80)
print("EXAMPLE 4: Multi-Turn Conversation with Persistence")
print("=" * 80)

if enable_persistence:
    # Turn 1
    messages_turn1 = [
        ChatAgentMessage(
            role="user",
            content="What is the AMM measure?"
        )
    ]

    response1 = agent.predict(
        messages=messages_turn1,
        custom_inputs={"thread_id": None}  # New conversation
    )

    thread_id = response1.custom_outputs.get('thread_id') if hasattr(response1, 'custom_outputs') else None

    print("--- TURN 1 ---")
    print("USER: What is the AMM measure?")
    print("\nASSISTANT:")
    for msg in response1.messages:
        if msg.role == "assistant" and msg.content:
            print(msg.content[:500] + "..." if len(msg.content) > 500 else msg.content)

    # Turn 2 - Follow-up question (uses same thread_id)
    messages_turn2 = messages_turn1 + [
        ChatAgentMessage(
            role="assistant",
            content=response1.messages[-1].content
        ),
        ChatAgentMessage(
            role="user",
            content="What are the numerator criteria for the effective continuation phase?"
        )
    ]

    response2 = agent.predict(
        messages=messages_turn2,
        custom_inputs={"thread_id": thread_id}  # Continue conversation
    )

    print("\n--- TURN 2 ---")
    print("USER: What are the numerator criteria for the effective continuation phase?")
    print("\nASSISTANT:")
    for msg in response2.messages:
        if msg.role == "assistant" and msg.content:
            print(msg.content[:500] + "..." if len(msg.content) > 500 else msg.content)

    print(f"\n--- PERSISTENCE INFO ---")
    print(f"Thread ID: {thread_id}")
    print("Conversation history saved to PostgreSQL checkpoint")
else:
    print("Persistence not enabled. Skipping multi-turn conversation demo.")
    print("To enable persistence, set 'Enable Persistence' widget to 'Yes' and provide a Lakebase instance.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5: Force Specific Mode
# MAGIC
# MAGIC Override automatic intent detection by forcing a specific mode.

# COMMAND ----------

# Example 5: Force QnA Mode (even with patient data)
print("=" * 80)
print("EXAMPLE 5: Force QnA Mode Override")
print("=" * 80)

messages = [
    ChatAgentMessage(
        role="user",
        content="Tell me about the general requirements for BCS, not a specific patient evaluation."
    )
]

response = agent.predict(
    messages=messages,
    custom_inputs={"mode": "QNA_MODE"}  # Force QnA mode
)

print("\n--- USER ---")
print(messages[0].content)

print("\n--- AGENT RESPONSE ---")
for msg in response.messages:
    if msg.role == "assistant" and msg.content:
        print(msg.content)

print("\n--- METADATA ---")
if hasattr(response, 'custom_outputs') and response.custom_outputs:
    print(f"Mode: {response.custom_outputs.get('mode')} (FORCED)")
    print(f"Intent Detection: {response.custom_outputs.get('intent_data', {}).get('reasoning')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 6: Non-Compliant Scenario
# MAGIC
# MAGIC Test the agent's ability to correctly identify non-compliance.

# COMMAND ----------

# Example 6: Non-Compliant Patient Scenario
print("=" * 80)
print("EXAMPLE 6: Non-Compliant Patient Scenario")
print("=" * 80)

non_compliant_request = """
Evaluate this patient for BCS compliance:

Patient Demographics:
- Female, DOB: June 10, 1985 (age 39)
- Enrollment: Continuously enrolled 1/1/2025 - 12/31/2025

Encounter:
- Date: August 15, 2025
- Service: Screening mammogram
- CPT Code: 77067

Is this patient compliant with HEDIS BCS for 2025?
"""

messages = [
    ChatAgentMessage(
        role="user",
        content=non_compliant_request
    )
]

response = agent.predict(messages=messages)

print("\n--- USER ---")
print(non_compliant_request.strip())

print("\n--- AGENT RESPONSE ---")
for msg in response.messages:
    if msg.role == "assistant" and msg.content:
        print(msg.content)

print("\n--- METADATA ---")
if hasattr(response, 'custom_outputs') and response.custom_outputs:
    print(f"Mode: {response.custom_outputs.get('mode')}")
    print(f"Intent: {response.custom_outputs.get('intent_data', {}).get('reasoning')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Log Agent to MLflow
# MAGIC
# MAGIC Log the agent to MLflow for versioning and tracking.

# COMMAND ----------

import mlflow
from databricks.sdk import WorkspaceClient

# Set MLflow experiment
experiment_name = f"/Users/{WorkspaceClient().current_user.me().user_name}/hedis-chat-agent"
mlflow.set_experiment(experiment_name)

print(f"MLflow Experiment: {experiment_name}")

# Log the agent
with mlflow.start_run(run_name="hedis-chat-agent-deployment") as run:
    # Log parameters
    mlflow.log_param("catalog", catalog_name)
    mlflow.log_param("schema", schema_name)
    mlflow.log_param("endpoint", endpoint_name)
    mlflow.log_param("effective_year", effective_year)
    mlflow.log_param("persistence_enabled", enable_persistence)
    mlflow.log_param("default_mode", "QNA_MODE")

    # Log agent configuration as artifact
    agent_config = {
        "catalog": catalog_name,
        "schema": schema_name,
        "endpoint": endpoint_name,
        "effective_year": effective_year,
        "persistence": enable_persistence,
        "tools": ["measures_definition_lookup", "measures_document_search", "measures_search_expansion"],
        "modes": ["QNA_MODE", "COMPLIANCE_MODE"],
        "features": [
            "Automatic intent detection",
            "Patient data extraction",
            "Vector search integration",
            "Measure lookup integration",
            "Streaming support",
            "Conversation persistence (optional)"
        ]
    }

    mlflow.log_dict(agent_config, "agent_config.json")

    # Log the agent model
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=agent,
        artifact_path="agent",
        input_example={
            "messages": [
                {
                    "role": "user",
                    "content": "What is the BCS measure?"
                }
            ]
        },
        registered_model_name=f"{catalog_name}.{schema_name}.hedis_chat_agent"
    )

    run_id = run.info.run_id
    model_uri = f"runs:/{run_id}/agent"

    print(f"\nMLflow Logging Complete!")
    print(f"  - Run ID: {run_id}")
    print(f"  - Model URI: {model_uri}")
    print(f"  - Registered Model: {catalog_name}.{schema_name}.hedis_chat_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Deploy to Model Serving Endpoint
# MAGIC
# MAGIC Deploy the logged agent to a Databricks Model Serving endpoint.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServedEntityInput,
    EndpointCoreConfigInput,
    AutoCaptureConfigInput
)

w = WorkspaceClient()

# Define serving endpoint name
serving_endpoint_name = f"hedis-chat-agent-{catalog_name}-{schema_name}".replace("_", "-")

# Get the latest model version
model_name = f"{catalog_name}.{schema_name}.hedis_chat_agent"

print(f"Deploying agent to Model Serving...")
print(f"  - Endpoint Name: {serving_endpoint_name}")
print(f"  - Model: {model_name}")

try:
    # Check if endpoint already exists
    try:
        existing_endpoint = w.serving_endpoints.get(name=serving_endpoint_name)
        print(f"\nEndpoint '{serving_endpoint_name}' already exists. Updating...")

        # Update endpoint with new model version
        w.serving_endpoints.update_config(
            name=serving_endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version=logged_agent_info.registered_model_version,
                    scale_to_zero_enabled=True,
                    workload_size="Small"
                )
            ]
        )

        print(f"Endpoint updated successfully!")

    except Exception:
        # Endpoint doesn't exist, create new one
        print(f"\nCreating new endpoint '{serving_endpoint_name}'...")

        w.serving_endpoints.create(
            name=serving_endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=model_name,
                        entity_version=logged_agent_info.registered_model_version,
                        scale_to_zero_enabled=True,
                        workload_size="Small"
                    )
                ],
                auto_capture_config=AutoCaptureConfigInput(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    enabled=True
                )
            )
        )

        print(f"Endpoint created successfully!")

    # Display endpoint information
    print(f"\nModel Serving Endpoint Details:")
    print(f"  - Name: {serving_endpoint_name}")
    print(f"  - Model: {model_name} (v{logged_agent_info.registered_model_version})")
    print(f"  - Status: Deploying (check Serving UI for progress)")
    print(f"  - Auto-capture: Enabled")
    print(f"\nTo test the endpoint:")
    print(f"  1. Go to Databricks UI > Serving")
    print(f"  2. Find endpoint: {serving_endpoint_name}")
    print(f"  3. Use the 'Query' tab to test requests")

    # Generate example request
    example_request = {
        "messages": [
            {
                "role": "user",
                "content": "What is the BCS measure?"
            }
        ]
    }

    print(f"\nExample Request:")
    print(json.dumps(example_request, indent=2))

except Exception as e:
    print(f"Error deploying endpoint: {str(e)}")
    print(f"\nYou can manually deploy using:")
    print(f"  - Model: {model_name}")
    print(f"  - Version: {logged_agent_info.registered_model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **HEDIS Chat Agent Deployment Complete!**
# MAGIC
# MAGIC The agent has been:
# MAGIC 1. Configured with Unity Catalog tools
# MAGIC 2. Tested with 6 interactive examples
# MAGIC 3. Logged to MLflow
# MAGIC 4. Deployed to Model Serving
# MAGIC
# MAGIC **Key Features Demonstrated:**
# MAGIC - Automatic intent detection (QnA vs Compliance)
# MAGIC - Patient data extraction
# MAGIC - Streaming responses
# MAGIC - Conversation persistence (optional)
# MAGIC - Mode override capabilities
# MAGIC - Compliance determination
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Test the Model Serving endpoint in the Databricks UI
# MAGIC - Integrate with your applications via REST API
# MAGIC - Monitor inference logs in auto-capture tables
# MAGIC - Collect feedback for model improvement
# MAGIC
# MAGIC **API Endpoint:**
# MAGIC ```
# MAGIC POST https://<workspace-url>/serving-endpoints/{serving_endpoint_name}/invocations
# MAGIC ```
# MAGIC
# MAGIC **Example cURL:**
# MAGIC ```bash
# MAGIC curl -X POST https://<workspace-url>/serving-endpoints/{serving_endpoint_name}/invocations \
# MAGIC   -H "Authorization: Bearer <token>" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "messages": [
# MAGIC       {"role": "user", "content": "What is the BCS measure?"}
# MAGIC     ]
# MAGIC   }'
# MAGIC ```
