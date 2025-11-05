# HEDIS Measure Ingestion Pipeline

A modular data ingestion pipeline for HEDIS (Healthcare Effectiveness Data and Information Set) measure specifications, built for Databricks Lakeflow with medallion architecture (Bronze → Silver).

## Overview

This pipeline extracts HEDIS measure specifications from PDF documents into structured Delta tables optimized for analytics and vector search. The pipeline implements three modules that can be orchestrated as separate Databricks jobs or combined into a workflow.

### Key Features

- **AI-Powered PDF Processing**: Uses Databricks `ai_parse_document` SQL function for superior OCR-free extraction
- **Modular Design**: Three independent notebooks for bronze, silver definitions, and silver chunks
- **LLM-Powered Extraction**: Structured measure extraction using Databricks Foundation Models
- **Vector Search Ready**: Chunking with header preservation and semantic coherence
- **Dual Extraction Methods**: AI processor (production) + PyMuPDF (fallback/development)
- **Parameterized**: Environment-specific configurations via Databricks Asset Bundles
- **Idempotent**: Safe to re-run without duplicating data
- **Scalable**: Designed for annual HEDIS updates and multi-year processing

## Architecture

```
┌─────────────────────────────────────┐
│  Unity Catalog Volume               │
│  /Volumes/{catalog}/{schema}/hedis/ │
│                                     │
│  - HEDIS MY 2025 Volume 2.pdf      │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  📒 Bronze Notebook                  │
│  01_bronze_metadata_ingestion.py    │
│                                     │
│  ✓ Scan volume for PDFs            │
│  ✓ Extract metadata                 │
│  ✓ Deduplicate via checksums        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  📊 Bronze Table                     │
│  hedis_file_metadata                │
│                                     │
│  file_id | file_name | page_count  │
└─────┬────────────────────┬──────────┘
      │                    │
      ▼                    ▼
┌──────────────┐    ┌──────────────┐
│ 📒 Silver 1  │    │ 📒 Silver 2  │
│ Definitions  │    │ Chunks       │
└──────┬───────┘    └──────┬───────┘
       │                   │
       ▼                   ▼
┌─────────────┐    ┌──────────────┐
│ 📊 Silver 1 │    │ 📊 Silver 2  │
│ Definitions │    │ Chunks       │
└─────────────┘    └──────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
                  │ Vector Search │
                  └───────────────┘
```

## Project Structure

```
hedis-measure-ingest/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── databricks.yml               # Databricks Asset Bundle config
│
├── agents/                      # Agent prompts and configurations
│   └── prompts/
│       ├── __init__.py
│       └── hedis_prompts.py     # LLM extraction prompts and schemas
│
├── src/                         # Core Python modules
│   ├── __init__.py
│   └── extraction/              # Extraction utilities
│       ├── __init__.py
│       ├── ai_pdf_processor.py  # ⭐ AI-powered extraction (Databricks ai_parse_document)
│       ├── pdf_processor.py     # Legacy PyMuPDF processor (fallback)
│       ├── llm_extractor.py     # LLM-based measure extraction
│       └── chunker.py           # Document chunking with headers
│
├── docs/                        # Documentation
│   └── AI_VS_PYMUPDF_COMPARISON.md  # Comparison guide
│
├── notebooks/                   # Databricks notebooks (.py format)
│   ├── 00_setup_infrastructure.py   # ⭐ Run this first to create infrastructure
│   ├── 01_bronze_metadata_ingestion.py
│   ├── 02_silver_definitions_extraction.py
│   └── 03_silver_chunks_processing.py
│
├── data/                        # Sample data (gitignored)
│   └── HEDIS MY 2025 Volume 2 Technical Update 2025-03-31.pdf
│
└── examples/                    # Reference examples
    ├── lakebase-interrupt-agent/ # Style reference
    └── measurement/              # PDF processing reference
```

## Quick Start

### 1. Prerequisites

- Databricks workspace with Unity Catalog enabled
- **Databricks Runtime 17.1+** (for `ai_parse_document` function)
- **US or EU region** (or cross-geography routing enabled)
- LLM model endpoint (e.g., `databricks-meta-llama-3-3-70b-instruct`)
- Unity Catalog volume for storing HEDIS PDFs
- Permissions: `CREATE TABLE`, `CREATE VOLUME`, `EXECUTE ON FUNCTION`

### 2. Setup

```bash
# Clone repository
git clone <repo-url>
cd hedis-measure-ingest

# Upload HEDIS PDF to Databricks volume
databricks fs cp data/HEDIS*.pdf dbfs:/Volumes/main/hedis_pipeline/hedis/
```

### 3. Create Infrastructure

**Run the setup notebook FIRST** to create all necessary resources:

```python
# 0. Setup Infrastructure (run this first!)
# Open notebooks/00_setup_infrastructure.py in Databricks
# Set widgets:
#   - catalog_name: "main"
#   - schema_name: "hedis_pipeline"
#   - volume_name: "hedis"
#   - vector_search_endpoint: "hedis_vector_endpoint"
#   - Create Catalog?: "no" (or "yes" if you have permissions)
#   - Create Schema?: "yes"
#   - Create Volume?: "yes"
#   - Create Vector Search Endpoint?: "yes"
#   - Create Delta Tables?: "yes"
# Run all cells
```

This will create:
- ✅ Unity Catalog schema
- ✅ Volume for HEDIS PDFs
- ✅ Vector Search endpoint
- ✅ All three Delta tables (bronze + 2 silver)

### 4. Run Pipeline

**Option A: Run Individual Notebooks**

```python
# 1. Bronze Ingestion
# Open notebooks/01_bronze_metadata_ingestion.py in Databricks
# Set widgets:
#   - catalog_name: "main"
#   - schema_name: "hedis_pipeline"
#   - volume_name: "hedis"
# Run all cells

# 2. Silver Definitions
# Open notebooks/02_silver_definitions_extraction.py
# Set widgets:
#   - model_endpoint: "databricks-meta-llama-3-3-70b-instruct"
#   - effective_year: "2025"
# Run all cells

# 3. Silver Chunks
# Open notebooks/03_silver_chunks_processing.py
# Set widgets:
#   - chunk_size: "1536"
#   - overlap_percent: "0.15"
# Run all cells
```

**Option B: Deploy as Databricks Workflow**

```bash
# Deploy using Databricks Asset Bundles
databricks bundle deploy -t dev

# Run workflow
databricks bundle run hedis_ingestion_pipeline -t dev
```
