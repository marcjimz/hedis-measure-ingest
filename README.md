# HEDIS Measure Ingestion Pipeline

## Overview
Data pipeline for ingesting and processing NCQA HEDIS measures with agent-based QnA and compliance evaluation.

## Setup
1. Run `notebooks/setup_infrastructure.py` to create UC catalog, schema, volume, vector search endpoint, and optionally Lakebase
2. Upload your HEDIS measures document to the volume ahead of notebook executions.
3. Run extraction notebooks to process HEDIS PDFs
4. Run agent notebooks to deploy and evaluate

## Features
- **Dual-mode agent**: QnA and Compliance evaluation
- **Automatic intent detection**: Routes to appropriate mode
- **Optional persistence**: Lakebase PostgreSQL checkpointing
- **Evaluation framework**: MLflow-based with 20 test queries

## Requirements
See `requirements.txt`
