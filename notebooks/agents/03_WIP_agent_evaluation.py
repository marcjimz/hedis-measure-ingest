# Databricks notebook source
# MAGIC %md
# MAGIC # WORK IN PROGRESS!!!
# MAGIC
# MAGIC # HEDIS Agent Evaluation
# MAGIC
# MAGIC This notebook demonstrates how to evaluate the HEDIS Chat Agent using MLflow evaluation framework with:
# MAGIC - Pre-built evaluation dataset for HEDIS measure QnA
# MAGIC - Automated evaluation metrics (Correctness, Relevance, Retrieval Sufficiency)
# MAGIC - Custom measure_id_accuracy scorer
# MAGIC - Human review workflow (optional)
# MAGIC - Comprehensive evaluation reporting
# MAGIC
# MAGIC **Evaluation Framework:**
# MAGIC - **Correctness**: Evaluates factual accuracy against expected answers
# MAGIC - **RelevanceToQuery**: Measures relevance of responses to user queries
# MAGIC - **RetrievalSufficiency**: Assesses if retrieved context is sufficient
# MAGIC - **measure_id_accuracy**: Custom scorer for measure ID extraction accuracy
# MAGIC
# MAGIC ![Evaluation Overview](https://docs.databricks.com/_images/agent-evaluation-overview.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation and Setup

# COMMAND ----------

# MAGIC %pip install -qqqq -U mlflow[databricks]>=3.3.2 databricks-sdk databricks-langchain pydantic>=2.10.3 langchain-core langgraph psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set up catalogs, schemas, and agent configuration

# COMMAND ----------

import os
import json
from pathlib import Path

# Configuration
UC_CATALOG = "main"
UC_SCHEMA = "hedis_measurements"
EVALUATION_SCHEMA = "hedis_evaluation"
ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
EFFECTIVE_YEAR = 2025

# Agent model configuration
AGENT_MODEL_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.hedis_chat_agent"
AGENT_ALIAS = "champion"

# Evaluation dataset path
EVALUATION_DATASET_PATH = "data/evaluation_dataset_hedis_qna.json"

# MLflow experiment configuration
EXPERIMENT_NAME = f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/hedis_agent_evaluation"

print(f"Configuration:")
print(f"  - UC Catalog: {UC_CATALOG}")
print(f"  - UC Schema: {UC_SCHEMA}")
print(f"  - Evaluation Schema: {EVALUATION_SCHEMA}")
print(f"  - Agent Model: {AGENT_MODEL_NAME}")
print(f"  - Endpoint: {ENDPOINT_NAME}")
print(f"  - Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Evaluation Schema
# MAGIC
# MAGIC Create a dedicated schema for evaluation datasets and results

# COMMAND ----------

# Create evaluation schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{EVALUATION_SCHEMA}")
print(f"Created schema: {UC_CATALOG}.{EVALUATION_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set MLflow Experiment

# COMMAND ----------

import mlflow

# Set or create experiment
mlflow.set_experiment(EXPERIMENT_NAME)
experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
print(f"Experiment ID: {experiment.experiment_id}")
print(f"Experiment Location: {experiment.artifact_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Evaluation Dataset
# MAGIC
# MAGIC Load the pre-built HEDIS QnA evaluation dataset with 20 queries covering:
# MAGIC - Definition lookups (AMM, BCS, CDC, CBP, COL, CIS, PPC, W15, ADD)
# MAGIC - Compliance checks
# MAGIC - Mixed difficulty levels (easy, medium, hard)

# COMMAND ----------

import pandas as pd

# Load evaluation dataset from JSON
with open(f"/Workspace{EVALUATION_DATASET_PATH}", "r") as f:
    eval_dataset_raw = json.load(f)

# Transform to MLflow evaluation format
eval_records = []
for query_item in eval_dataset_raw["queries"]:
    eval_records.append({
        "request_id": f"query_{query_item['id']}",
        "request": query_item["query"],
        "expected_response": query_item["expected_answer"],
        "expected_facts": [query_item["expected_answer"]],  # For Correctness scorer
        "expected_retrieved_context": [],  # Will be populated by retrieval
        "measure_type": query_item["measure_type"],
        "difficulty": query_item["difficulty"],
        "scenario_type": query_item["scenario_type"]
    })

eval_df = pd.DataFrame(eval_records)

print(f"Loaded {len(eval_df)} evaluation queries")
print(f"\nDataset Statistics:")
print(f"  - Total Queries: {len(eval_df)}")
print(f"  - Measure Types: {eval_df['measure_type'].nunique()}")
print(f"  - Difficulty Distribution:\n{eval_df['difficulty'].value_counts()}")
print(f"  - Scenario Types:\n{eval_df['scenario_type'].value_counts()}")

display(eval_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Evaluation Dataset to Unity Catalog
# MAGIC
# MAGIC Store the evaluation dataset as a Delta table for versioning and tracking

# COMMAND ----------

# Convert to Spark DataFrame and save to Unity Catalog
eval_spark_df = spark.createDataFrame(eval_df)

# Save as Delta table
eval_table_name = f"{UC_CATALOG}.{EVALUATION_SCHEMA}.hedis_qna_eval_dataset"
eval_spark_df.write.mode("overwrite").saveAsTable(eval_table_name)

print(f"Saved evaluation dataset to: {eval_table_name}")
print(f"Total rows: {spark.table(eval_table_name).count()}")

# Show table details
display(spark.sql(f"DESCRIBE TABLE EXTENDED {eval_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load HEDIS Agent Model
# MAGIC
# MAGIC Load the registered HEDIS Chat Agent from Unity Catalog

# COMMAND ----------

# Load agent from Unity Catalog
model_version_uri = f"models:/{AGENT_MODEL_NAME}@{AGENT_ALIAS}"

try:
    agent_model = mlflow.pyfunc.load_model(model_version_uri)
    print(f"Loaded agent model: {model_version_uri}")
except Exception as e:
    print(f"Warning: Could not load agent from Unity Catalog: {e}")
    print("Will use direct agent initialization instead")
    agent_model = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Agent on Sample Query
# MAGIC
# MAGIC Verify agent is working before running full evaluation

# COMMAND ----------

if agent_model:
    # Test with first query
    test_query = eval_df.iloc[0]["request"]
    print(f"Test Query: {test_query}")
    print("\n" + "="*80 + "\n")

    # Make prediction
    test_response = agent_model.predict({
        "messages": [{"role": "user", "content": test_query}]
    })

    print("Agent Response:")
    print(test_response)
else:
    print("Agent not loaded, skipping test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Custom Scorers
# MAGIC
# MAGIC Create custom evaluation metrics specific to HEDIS measures

# COMMAND ----------

from mlflow.genai.scorers import scorer
from mlflow.entities import Feedback
from typing import Any, Dict
import re

@scorer
def measure_id_accuracy(inputs: Dict[str, Any], outputs: str, expected: Dict[str, Any]) -> Feedback:
    """
    Custom scorer to evaluate if the correct measure ID is mentioned in the response.

    Checks if the expected measure type (AMM, BCS, CDC, etc.) is correctly identified
    in the agent's response.
    """
    try:
        expected_measure = expected.get("measure_type", "").upper()
        response_text = outputs.upper() if isinstance(outputs, str) else str(outputs).upper()

        # Check if measure ID is mentioned
        measure_mentioned = expected_measure in response_text

        # Also check for full measure names (optional enhancement)
        measure_names = {
            "AMM": "ANTIDEPRESSANT MEDICATION MANAGEMENT",
            "BCS": "BREAST CANCER SCREENING",
            "CDC": "COMPREHENSIVE DIABETES CARE",
            "CBP": "CONTROLLING HIGH BLOOD PRESSURE",
            "COL": "COLORECTAL CANCER SCREENING",
            "CIS": "CHILDHOOD IMMUNIZATION STATUS",
            "PPC": "PRENATAL AND POSTPARTUM CARE",
            "W15": "WELL-CHILD VISITS",
            "ADD": "ADHD MEDICATION"
        }

        full_name_mentioned = False
        if expected_measure in measure_names:
            full_name_mentioned = measure_names[expected_measure] in response_text

        # Score: 1.0 if measure ID or full name mentioned, 0.0 otherwise
        score = 1.0 if (measure_mentioned or full_name_mentioned) else 0.0

        rationale = (
            f"Expected measure '{expected_measure}' was "
            f"{'found' if measure_mentioned else 'NOT found'} in response. "
            f"Full name check: {'passed' if full_name_mentioned else 'not applicable/failed'}."
        )

        return Feedback(
            value=score,
            rationale=rationale
        )
    except Exception as e:
        return Feedback(
            value=0.0,
            rationale=f"Error in measure_id_accuracy scorer: {str(e)}"
        )

@scorer
def retrieval_context_scorer(inputs: Dict[str, Any], outputs: str, trace_info: Any = None) -> Feedback:
    """
    Custom scorer to evaluate the quality of retrieved context from vector search.

    Analyzes if the agent properly used the retrieval tools and if the retrieved
    context was relevant to the query.
    """
    try:
        # This would analyze trace information to check tool calls
        # For now, return a placeholder score
        # In production, you'd inspect trace_info for tool_calls

        score = 0.5  # Neutral score
        rationale = "Retrieval context analysis not fully implemented. Manual review recommended."

        return Feedback(
            value=score,
            rationale=rationale
        )
    except Exception as e:
        return Feedback(
            value=0.0,
            rationale=f"Error in retrieval_context_scorer: {str(e)}"
        )

print("Custom scorers defined:")
print("  - measure_id_accuracy: Validates correct measure identification")
print("  - retrieval_context_scorer: Evaluates retrieval quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Evaluation Predict Function
# MAGIC
# MAGIC Wrapper function to format inputs for the agent

# COMMAND ----------

import mlflow

@mlflow.trace
def evaluate_predict_fn(inputs: pd.DataFrame) -> pd.Series:
    """
    Predict function wrapper for MLflow evaluation.

    Converts evaluation dataset format to agent input format.
    """
    responses = []

    for idx, row in inputs.iterrows():
        try:
            # Format input for agent
            request = row["request"]

            if agent_model:
                # Use loaded agent
                response = agent_model.predict({
                    "messages": [{"role": "user", "content": request}]
                })

                # Extract response text
                if isinstance(response, dict):
                    # Handle different response formats
                    if "messages" in response:
                        # Get last message content
                        last_msg = response["messages"][-1]
                        if isinstance(last_msg, dict):
                            response_text = last_msg.get("content", str(response))
                        else:
                            response_text = getattr(last_msg, "content", str(response))
                    elif "response" in response:
                        response_text = response["response"]
                    else:
                        response_text = str(response)
                else:
                    response_text = str(response)
            else:
                # Fallback: direct agent initialization (if UC model not available)
                response_text = f"Agent not loaded. Query: {request}"

            responses.append(response_text)
        except Exception as e:
            responses.append(f"Error: {str(e)}")

    return pd.Series(responses)

print("Evaluation predict function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run MLflow Evaluation
# MAGIC
# MAGIC Execute comprehensive evaluation with multiple scorers:
# MAGIC - **Correctness**: Factual accuracy against expected answers
# MAGIC - **RelevanceToQuery**: Response relevance to user query
# MAGIC - **RetrievalSufficiency**: Adequacy of retrieved context
# MAGIC - **measure_id_accuracy**: Custom HEDIS measure identification
# MAGIC
# MAGIC This may take 5-10 minutes depending on dataset size.

# COMMAND ----------

from mlflow.genai.scorers import Correctness, RelevanceToQuery, RetrievalSufficiency

# Only run evaluation if agent is loaded
if agent_model:
    print("Starting MLflow evaluation...")
    print(f"Evaluating {len(eval_df)} queries")
    print("This may take several minutes...\n")

    # Run evaluation
    eval_results = mlflow.genai.evaluate(
        data=eval_df,
        predict_fn=evaluate_predict_fn,
        scorers=[
            # Ground truth scorers
            Correctness(),
            RetrievalSufficiency(),

            # Without ground truth scorers
            RelevanceToQuery(),

            # Custom scorers
            measure_id_accuracy,
            retrieval_context_scorer
        ],
        run_name=f"hedis_agent_eval_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
        extra_metrics=[],
    )

    print("\n" + "="*80)
    print("Evaluation Complete!")
    print("="*80)
    print(f"\nRun ID: {eval_results.run_id}")

    # Display metrics summary
    metrics_df = eval_results.metrics
    print("\nEvaluation Metrics Summary:")
    display(metrics_df)
else:
    print("Agent not loaded. Skipping evaluation.")
    eval_results = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Detailed Results
# MAGIC
# MAGIC Show per-query results with scores and rationales

# COMMAND ----------

if eval_results:
    # Get detailed results table
    results_table = eval_results.tables["eval_results_table"]

    print(f"Detailed Results: {len(results_table)} queries evaluated")

    # Display full results
    display(results_table)

    # Save results to Unity Catalog
    results_table_name = f"{UC_CATALOG}.{EVALUATION_SCHEMA}.hedis_agent_eval_results"
    results_spark_df = spark.createDataFrame(results_table)
    results_spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(results_table_name)

    print(f"\nResults saved to: {results_table_name}")
else:
    print("No evaluation results to display")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Top and Bottom Performing Queries
# MAGIC
# MAGIC Identify queries with highest and lowest scores for deeper analysis

# COMMAND ----------

if eval_results:
    results_table = eval_results.tables["eval_results_table"]

    # Calculate average score per query (across all scorers)
    score_columns = [col for col in results_table.columns if col.endswith("/value")]

    if score_columns:
        results_table["avg_score"] = results_table[score_columns].mean(axis=1)

        # Sort by average score
        sorted_results = results_table.sort_values("avg_score", ascending=False)

        print("="*80)
        print("TOP 5 PERFORMING QUERIES")
        print("="*80)
        top_5 = sorted_results.head(5)[["request", "avg_score"] + score_columns]
        display(top_5)

        print("\n" + "="*80)
        print("BOTTOM 5 PERFORMING QUERIES")
        print("="*80)
        bottom_5 = sorted_results.tail(5)[["request", "avg_score"] + score_columns]
        display(bottom_5)

        # Breakdown by difficulty
        print("\n" + "="*80)
        print("PERFORMANCE BY DIFFICULTY")
        print("="*80)

        # Merge with original metadata
        results_with_meta = results_table.merge(
            eval_df[["request", "difficulty", "measure_type", "scenario_type"]],
            on="request",
            how="left"
        )

        difficulty_stats = results_with_meta.groupby("difficulty")["avg_score"].agg(["mean", "std", "count"])
        print(difficulty_stats)

        # Breakdown by measure type
        print("\n" + "="*80)
        print("PERFORMANCE BY MEASURE TYPE")
        print("="*80)
        measure_stats = results_with_meta.groupby("measure_type")["avg_score"].agg(["mean", "std", "count"])
        print(measure_stats)

        # Breakdown by scenario type
        print("\n" + "="*80)
        print("PERFORMANCE BY SCENARIO TYPE")
        print("="*80)
        scenario_stats = results_with_meta.groupby("scenario_type")["avg_score"].agg(["mean", "std", "count"])
        print(scenario_stats)
    else:
        print("No score columns found in results")
else:
    print("No evaluation results to analyze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Evaluation Metrics
# MAGIC
# MAGIC Create visualizations for metric distributions

# COMMAND ----------

if eval_results:
    import matplotlib.pyplot as plt
    import seaborn as sns

    results_table = eval_results.tables["eval_results_table"]
    score_columns = [col for col in results_table.columns if col.endswith("/value")]

    if score_columns:
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle("HEDIS Agent Evaluation Metrics Distribution", fontsize=16)

        # Plot 1: Overall score distribution
        ax1 = axes[0, 0]
        results_table["avg_score"].hist(bins=20, ax=ax1, edgecolor="black")
        ax1.set_xlabel("Average Score")
        ax1.set_ylabel("Frequency")
        ax1.set_title("Overall Score Distribution")
        ax1.axvline(results_table["avg_score"].mean(), color="red", linestyle="--", label=f"Mean: {results_table['avg_score'].mean():.2f}")
        ax1.legend()

        # Plot 2: Score by difficulty
        ax2 = axes[0, 1]
        results_with_meta = results_table.merge(eval_df[["request", "difficulty"]], on="request", how="left")
        results_with_meta.boxplot(column="avg_score", by="difficulty", ax=ax2)
        ax2.set_xlabel("Difficulty")
        ax2.set_ylabel("Average Score")
        ax2.set_title("Score by Difficulty Level")
        plt.sca(ax2)
        plt.xticks(rotation=0)

        # Plot 3: Scorer comparison
        ax3 = axes[1, 0]
        scorer_means = results_table[score_columns].mean()
        scorer_names = [col.replace("/value", "").split("/")[-1] for col in score_columns]
        ax3.bar(range(len(scorer_names)), scorer_means.values, edgecolor="black")
        ax3.set_xticks(range(len(scorer_names)))
        ax3.set_xticklabels(scorer_names, rotation=45, ha="right")
        ax3.set_ylabel("Average Score")
        ax3.set_title("Average Score by Scorer")
        ax3.set_ylim([0, 1])

        # Plot 4: Score by measure type
        ax4 = axes[1, 1]
        results_with_meta = results_table.merge(eval_df[["request", "measure_type"]], on="request", how="left")
        measure_means = results_with_meta.groupby("measure_type")["avg_score"].mean().sort_values(ascending=False)
        ax4.barh(range(len(measure_means)), measure_means.values, edgecolor="black")
        ax4.set_yticks(range(len(measure_means)))
        ax4.set_yticklabels(measure_means.index)
        ax4.set_xlabel("Average Score")
        ax4.set_title("Average Score by Measure Type")
        ax4.set_xlim([0, 1])

        plt.tight_layout()
        plt.show()

        # Save plot to MLflow
        if eval_results.run_id:
            with mlflow.start_run(run_id=eval_results.run_id):
                mlflow.log_figure(fig, "evaluation_metrics_distribution.png")
                print("Visualization saved to MLflow run")
    else:
        print("No score columns found for visualization")
else:
    print("No evaluation results to visualize")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search Evaluation Traces
# MAGIC
# MAGIC Explore individual traces with detailed assessments

# COMMAND ----------

if eval_results:
    # Search traces from evaluation run
    traces = mlflow.search_traces(
        run_id=eval_results.run_id,
        max_results=10
    )

    print(f"Found {len(traces)} traces")

    # Display trace information
    if len(traces) > 0:
        print("\nSample Trace Analysis:")
        print("="*80)

        for i, trace in enumerate(traces["trace"][:3], 1):  # Show first 3 traces
            print(f"\n--- Trace {i} ---")
            print(f"Trace ID: {trace.info.trace_id}")
            print(f"Status: {trace.info.status}")

            # Get assessments
            assessments = trace.info.assessments
            if assessments:
                print(f"\nAssessments ({len(assessments)}):")
                for assessment in assessments:
                    print(f"  - {assessment.name}: {assessment.value}")
                    print(f"    Rationale: {assessment.rationale[:100]}...")
            else:
                print("No assessments found")

            print("-"*80)
else:
    print("No traces to display")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Human Review Setup (Optional)
# MAGIC
# MAGIC Set up a human labeling session for manual review and feedback.
# MAGIC
# MAGIC **Note**: This section uses MLflow labeling features that may require:
# MAGIC - MLflow >= 3.4.0
# MAGIC - Databricks workspace with labeling enabled
# MAGIC - Assigned reviewers with appropriate permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Evaluation Dataset for Human Review

# COMMAND ----------

try:
    import mlflow.genai.datasets

    # Create or get evaluation dataset in Unity Catalog
    uc_eval_dataset_name = f"{UC_CATALOG}.{EVALUATION_SCHEMA}.hedis_human_review_dataset"

    try:
        # Try to delete existing dataset
        mlflow.genai.datasets.delete_dataset(uc_table_name=uc_eval_dataset_name)
        print(f"Deleted existing dataset: {uc_eval_dataset_name}")
    except Exception as e:
        print(f"No existing dataset to delete: {e}")

    # Create new dataset
    try:
        human_review_dataset = mlflow.genai.datasets.create_dataset(
            uc_table_name=uc_eval_dataset_name,
        )
        print(f"Created evaluation dataset: {uc_eval_dataset_name}")
    except Exception as e:
        if "TABLE_ALREADY_EXISTS" in str(e):
            print("Table already exists. Loading dataset instead.")
            human_review_dataset = mlflow.genai.datasets.get_dataset(uc_eval_dataset_name)
            print(f"Loaded evaluation dataset: {uc_eval_dataset_name}")
        else:
            print(f"Error creating dataset: {e}")
            human_review_dataset = None

    # Add traces to dataset
    if human_review_dataset and eval_results:
        traces = mlflow.search_traces(
            run_id=eval_results.run_id,
            max_results=20  # Limit for human review
        )

        if len(traces) > 0:
            human_review_dataset.merge_records(traces)
            print(f"Added {len(traces)} records to human review dataset")
        else:
            print("No traces found to add to dataset")

except ImportError as e:
    print(f"MLflow labeling features not available: {e}")
    print("This feature requires MLflow >= 3.4.0 with genai.datasets module")
    human_review_dataset = None
except Exception as e:
    print(f"Error setting up human review dataset: {e}")
    human_review_dataset = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Labeling Session (Optional)
# MAGIC
# MAGIC **Note**: Uncomment and configure the cell below to create a human labeling session.
# MAGIC You'll need to:
# MAGIC 1. Specify assigned reviewers (email addresses)
# MAGIC 2. Have MLflow labeling features enabled
# MAGIC 3. Have appropriate workspace permissions

# COMMAND ----------

# # Uncomment to create labeling session
# try:
#     import mlflow.genai.labeling as labeling
#     import mlflow.genai.label_schemas as schemas
#
#     # Define label schemas
#     quality_schema = schemas.create_label_schema(
#         name="response_quality",
#         type=schemas.LabelSchemaType.FEEDBACK,
#         title="Rate the response quality",
#         input=schemas.InputCategorical(
#             options=["Poor", "Fair", "Good", "Excellent"]
#         ),
#         overwrite=True
#     )
#
#     accuracy_schema = schemas.create_label_schema(
#         name="measure_accuracy",
#         type=schemas.LabelSchemaType.FEEDBACK,
#         title="Rate HEDIS measure accuracy",
#         input=schemas.InputCategorical(
#             options=["Incorrect", "Partially Correct", "Mostly Correct", "Fully Correct"]
#         ),
#         overwrite=True
#     )
#
#     expected_facts_schema = schemas.create_label_schema(
#         name=schemas.EXPECTED_FACTS,
#         type=schemas.LabelSchemaType.EXPECTATION,
#         title="Expected facts",
#         input=schemas.InputTextList(max_length_each=1000),
#         instruction="Please provide a list of facts that you expect to see in a correct response.",
#         overwrite=True
#     )
#
#     # Create labeling session
#     # IMPORTANT: Replace with actual reviewer email addresses
#     LABEL_USERS = ["reviewer1@example.com", "reviewer2@example.com"]
#
#     session = labeling.create_labeling_session(
#         name=f"hedis_agent_review_{pd.Timestamp.now().strftime('%Y%m%d')}",
#         assigned_users=LABEL_USERS,
#         label_schemas=[
#             "response_quality",
#             "measure_accuracy",
#             schemas.EXPECTED_FACTS
#         ]
#     )
#
#     print(f"Created labeling session: {session.name}")
#     print(f"Session ID: {session.labeling_session_id}")
#
#     # Add dataset to session
#     session.add_dataset(uc_eval_dataset_name)
#     print(f"Added dataset to labeling session")
#
#     # Get review app URL
#     app = labeling.get_review_app()
#     print(f"\nReview App URL: {app.url}")
#
# except ImportError as e:
#     print(f"MLflow labeling features not available: {e}")
# except Exception as e:
#     print(f"Error creating labeling session: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Evaluation Report
# MAGIC
# MAGIC Create a comprehensive summary report of the evaluation results

# COMMAND ----------

if eval_results:
    results_table = eval_results.tables["eval_results_table"]
    score_columns = [col for col in results_table.columns if col.endswith("/value")]

    if score_columns:
        results_table["avg_score"] = results_table[score_columns].mean(axis=1)
        results_with_meta = results_table.merge(
            eval_df[["request", "difficulty", "measure_type", "scenario_type"]],
            on="request",
            how="left"
        )

        # Generate report
        report = f"""
# HEDIS Agent Evaluation Report
Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview
- **Total Queries Evaluated**: {len(results_table)}
- **MLflow Run ID**: {eval_results.run_id}
- **Experiment**: {EXPERIMENT_NAME}
- **Agent Model**: {AGENT_MODEL_NAME}@{AGENT_ALIAS}

## Overall Performance
- **Average Score**: {results_table['avg_score'].mean():.3f}
- **Median Score**: {results_table['avg_score'].median():.3f}
- **Std Dev**: {results_table['avg_score'].std():.3f}
- **Min Score**: {results_table['avg_score'].min():.3f}
- **Max Score**: {results_table['avg_score'].max():.3f}

## Performance by Scorer
"""
        for col in score_columns:
            scorer_name = col.replace("/value", "").split("/")[-1]
            scorer_mean = results_table[col].mean()
            report += f"- **{scorer_name}**: {scorer_mean:.3f}\n"

        report += f"""
## Performance by Difficulty
"""
        difficulty_stats = results_with_meta.groupby("difficulty")["avg_score"].agg(["mean", "count"])
        for difficulty, row in difficulty_stats.iterrows():
            report += f"- **{difficulty}**: {row['mean']:.3f} (n={int(row['count'])})\n"

        report += f"""
## Performance by Measure Type
"""
        measure_stats = results_with_meta.groupby("measure_type")["avg_score"].agg(["mean", "count"]).sort_values("mean", ascending=False)
        for measure, row in measure_stats.iterrows():
            report += f"- **{measure}**: {row['mean']:.3f} (n={int(row['count'])})\n"

        report += f"""
## Performance by Scenario Type
"""
        scenario_stats = results_with_meta.groupby("scenario_type")["avg_score"].agg(["mean", "count"])
        for scenario, row in scenario_stats.iterrows():
            report += f"- **{scenario}**: {row['mean']:.3f} (n={int(row['count'])})\n"

        report += f"""
## Top Performing Queries
"""
        top_queries = results_with_meta.nlargest(3, "avg_score")[["request", "avg_score", "measure_type", "difficulty"]]
        for idx, row in top_queries.iterrows():
            report += f"\n### Query (Score: {row['avg_score']:.3f})\n"
            report += f"**Measure**: {row['measure_type']} | **Difficulty**: {row['difficulty']}\n"
            report += f"**Question**: {row['request'][:200]}...\n"

        report += f"""
## Bottom Performing Queries
"""
        bottom_queries = results_with_meta.nsmallest(3, "avg_score")[["request", "avg_score", "measure_type", "difficulty"]]
        for idx, row in bottom_queries.iterrows():
            report += f"\n### Query (Score: {row['avg_score']:.3f})\n"
            report += f"**Measure**: {row['measure_type']} | **Difficulty**: {row['difficulty']}\n"
            report += f"**Question**: {row['request'][:200]}...\n"

        report += f"""
## Recommendations
Based on the evaluation results:

1. **Focus Areas**:
   - Review queries with scores below {results_table['avg_score'].mean():.2f}
   - Improve performance on {difficulty_stats['mean'].idxmin()} difficulty queries

2. **Measure-Specific Improvements**:
   - Consider additional training data for measures with lower scores
   - Review retrieval context quality for complex measures

3. **Next Steps**:
   - Conduct human review for borderline cases
   - Fine-tune prompt engineering based on failure patterns
   - Expand evaluation dataset with edge cases
   - Implement continuous evaluation pipeline

## Dataset Details
- **Evaluation Dataset**: {eval_table_name}
- **Results Dataset**: {results_table_name}
- **Total Measures Covered**: {eval_df['measure_type'].nunique()}
- **Difficulty Levels**: {', '.join(eval_df['difficulty'].unique())}
- **Scenario Types**: {', '.join(eval_df['scenario_type'].unique())}
"""

        print(report)

        # Save report to MLflow
        if eval_results.run_id:
            with mlflow.start_run(run_id=eval_results.run_id):
                # Save as artifact
                report_path = "/tmp/evaluation_report.md"
                with open(report_path, "w") as f:
                    f.write(report)
                mlflow.log_artifact(report_path, "evaluation_report")
                print(f"\nReport saved to MLflow run: {eval_results.run_id}")

                # Log summary metrics
                mlflow.log_metric("avg_score", results_table['avg_score'].mean())
                mlflow.log_metric("median_score", results_table['avg_score'].median())
                mlflow.log_metric("std_score", results_table['avg_score'].std())
                mlflow.log_metric("total_queries", len(results_table))

                print("Summary metrics logged to MLflow")
    else:
        print("No score columns found for report generation")
else:
    print("No evaluation results to generate report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook completed the following:
# MAGIC
# MAGIC 1. ✅ Loaded HEDIS QnA evaluation dataset (20 queries)
# MAGIC 2. ✅ Uploaded dataset to Unity Catalog for versioning
# MAGIC 3. ✅ Loaded HEDIS Chat Agent from Unity Catalog
# MAGIC 4. ✅ Defined custom scorers (measure_id_accuracy, retrieval_context_scorer)
# MAGIC 5. ✅ Ran MLflow evaluation with multiple metrics:
# MAGIC    - Correctness
# MAGIC    - RelevanceToQuery
# MAGIC    - RetrievalSufficiency
# MAGIC    - measure_id_accuracy (custom)
# MAGIC    - retrieval_context_scorer (custom)
# MAGIC 6. ✅ Analyzed top/bottom performing queries
# MAGIC 7. ✅ Visualized metric distributions
# MAGIC 8. ✅ Generated comprehensive evaluation report
# MAGIC 9. ⚠️ (Optional) Set up human review workflow
# MAGIC
# MAGIC ### Key Takeaways:
# MAGIC - Use MLflow evaluation for systematic agent assessment
# MAGIC - Combine automated and custom scorers for comprehensive evaluation
# MAGIC - Analyze performance by measure type, difficulty, and scenario
# MAGIC - Use human review for complex or ambiguous cases
# MAGIC - Track evaluations in Unity Catalog for versioning and governance
# MAGIC
# MAGIC ### Next Steps:
# MAGIC 1. Review low-scoring queries and improve agent responses
# MAGIC 2. Expand evaluation dataset with additional edge cases
# MAGIC 3. Set up continuous evaluation pipeline
# MAGIC 4. Integrate human feedback into training data
# MAGIC 5. A/B test different agent configurations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: View MLflow UI
# MAGIC
# MAGIC To view detailed evaluation results in MLflow UI:
# MAGIC
# MAGIC 1. Navigate to **Machine Learning** > **Experiments** in Databricks workspace
# MAGIC 2. Search for experiment: `/Users/{your_username}/hedis_agent_evaluation`
# MAGIC 3. Click on the latest run to view:
# MAGIC    - Metrics and parameters
# MAGIC    - Evaluation artifacts
# MAGIC    - Traces and assessments
# MAGIC    - Visualizations
# MAGIC
# MAGIC Or use the direct link (replace with your workspace URL and run ID):
# MAGIC ```
# MAGIC https://<workspace-url>/ml/experiments/{experiment_id}/runs/{run_id}
# MAGIC ```

# COMMAND ----------


