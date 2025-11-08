# HEDIS Measure Ingestion Pipeline

## Overview
Data pipeline for ingesting and processing NCQA HEDIS measures with agent-based QnA and compliance evaluation.

## Structure
```
├── src/
│   ├── agents/
│   │   ├── hedis_chat.py          # Main chat agent
│   │   └── prompts/                # Agent prompts
│   ├── database/
│   │   └── lakebase.py             # PostgreSQL persistence
│   └── extraction/                 # PDF processing
│       ├── ai_pdf_processor.py     # Databricks ai_parse_document
│       ├── llm_extractor.py        # Measure extraction
│       └── chunker.py              # Document chunking
├── notebooks/
│   ├── setup_infrastructure.py     # Infrastructure setup
│   ├── extraction/                 # Data extraction pipeline
│   │   ├── 01_bronze_metadata_ingestion.py
│   │   ├── 02_silver_definitions_extraction.py
│   │   └── 03_silver_chunks_processing.py
│   └── agents/                     # Agent deployment
│       ├── 01_setup_uc_functions.py
│       ├── 02_agent_deployment.py
│       └── 03_agent_evaluation.py
└── data/
    └── evaluation_dataset_hedis_qna.json
```

## Setup
1. Run `notebooks/setup_infrastructure.py` to create UC catalog, schema, volume, vector search endpoint, and optionally Lakebase
2. Run extraction notebooks to process HEDIS PDFs
3. Run agent notebooks to deploy and evaluate

## Features
- **Dual-mode agent**: QnA and Compliance evaluation
- **Automatic intent detection**: Routes to appropriate mode
- **Optional persistence**: Lakebase PostgreSQL checkpointing
- **Evaluation framework**: MLflow-based with 20 test queries

## Requirements
See `requirements.txt`
