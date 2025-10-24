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

## Data Schemas

### Bronze Table: `hedis_file_metadata`

| Column | Type | Description |
|--------|------|-------------|
| `file_id` | STRING | Unique file identifier (UUID) |
| `file_name` | STRING | Original filename |
| `file_path` | STRING | Full volume path |
| `page_count` | INT | Total pages in PDF |
| `effective_year` | INT | HEDIS year (e.g., 2025) |
| `processing_status` | STRING | pending/processing/completed/failed |
| `checksum` | STRING | SHA256 hash for deduplication |

### Silver Table 1: `hedis_measures_definitions`

| Column | Type | Description |
|--------|------|-------------|
| `measure_id` | STRING | Unique measure identifier |
| `specifications` | STRING | Official NCQA description |
| `measure` | STRING | Measure name with acronym |
| `initial_pop` | STRING | Initial population criteria |
| `denominator` | ARRAY<STRING> | Denominator components |
| `numerator` | ARRAY<STRING> | Numerator compliance criteria |
| `exclusion` | ARRAY<STRING> | Exclusion conditions |
| `effective_year` | INT | Measure year |
| `page_start`, `page_end` | INT | Page range in source PDF |

### Silver Table 2: `hedis_measures_chunks`

| Column | Type | Description |
|--------|------|-------------|
| `chunk_id` | STRING | Unique chunk identifier |
| `file_id` | STRING | Foreign key to bronze table |
| `chunk_text` | STRING | Chunk content with markdown headers |
| `token_count` | INT | Approximate tokens |
| `headers` | ARRAY<STRING> | Header hierarchy (H1 > H2 > H3) |
| `page_start`, `page_end` | INT | Page range |
| `measure_name` | STRING | Associated measure (if available) |
| `metadata` | STRING | JSON metadata for vector search |

## Module Details

### Module 0: Infrastructure Setup (00_setup_infrastructure.py)

**Purpose**: Create all necessary infrastructure before running the pipeline

**Key Functions**:
- Create Unity Catalog and Schema
- Create Volume for HEDIS PDFs
- Create Vector Search endpoint (takes 5-10 minutes)
- Create all three Delta tables with proper schemas
- Verify all resources are accessible

**Features**:
- Widget-based configuration
- Conditional creation (skip if resources exist)
- Progress tracking during endpoint provisioning
- Comprehensive verification checks
- Helpful next steps and cleanup commands

**Important Notes**:
- Creating a catalog requires metastore admin permissions
- Vector Search endpoint creation takes 5-10 minutes
- All operations are idempotent (safe to re-run)

### Module 1: Bronze Metadata Ingestion

**Purpose**: Scan Unity Catalog volume and catalog all HEDIS PDF files

**Key Functions**:
- Discover PDFs matching pattern (e.g., `HEDIS*.pdf`)
- Extract file metadata (size, pages, publish date)
- Calculate checksums for duplicate detection
- Create/update bronze Delta table

**Idempotency**: Checks checksums before inserting new records

### Module 2: Silver Definitions Extraction

**Purpose**: Extract structured HEDIS measure definitions using LLM

**Key Functions**:
- Parse PDF table of contents to identify measures
- Extract measure pages as text
- Use LLM with JSON schema enforcement to structure data
- Write to silver definitions table

**LLM Configuration**:
- Model: Llama 3.3 70B Instruct (or configurable)
- Temperature: 0.0 (deterministic)
- Max tokens: 4096
- Response format: JSON schema with validation

**Extraction Schema** (from INSTRUCTIONS.md):
```json
{
  "Specifications": "Official measure description from NCQA",
  "measure": "Measure name with acronym",
  "Initial_Pop": "Initial population criteria",
  "denominator": ["List of components"],
  "numerator": ["List of compliance criteria"],
  "exclusion": ["List of exclusions"],
  "effective_year": 2025
}
```

### Module 3: Silver Chunks Processing

**Purpose**: Chunk documents for vector search with semantic preservation

**Key Functions**:
- Parse table of contents for measure context
- Chunk with header-aware algorithm
- Preserve header hierarchy (H1 > H2 > H3)
- Add measure metadata to each chunk

**Chunking Configuration**:
- Chunk size: 1536 tokens (optimized for HEDIS complexity)
- Overlap: 15% (prevents criteria fragmentation)
- Header preservation: Markdown headers included in chunks
- Measure tagging: Chunks tagged with parent measure

**Algorithm**:
1. Extract structured text with PyMuPDF
2. Identify headers by font size/boldness
3. Chunk with token-based boundaries
4. Maintain 15% overlap between chunks
5. Track header hierarchy throughout

## Configuration

### Environment Variables

Create a `.env` file (gitignored):

```bash
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
```

### Widget Parameters

Each notebook accepts parameters via Databricks widgets:

**Bronze Notebook**:
- `catalog_name`: Unity Catalog catalog (default: "main")
- `schema_name`: Unity Catalog schema (default: "hedis_pipeline")
- `volume_name`: Volume name (default: "hedis")
- `file_pattern`: File pattern to match (default: "HEDIS*.pdf")

**Silver Definitions Notebook**:
- `model_endpoint`: LLM endpoint (default: "databricks-meta-llama-3-3-70b-instruct")
- `effective_year`: HEDIS year to process (default: "2025")
- `batch_size`: Files to process per run (default: "10")

**Silver Chunks Notebook**:
- `chunk_size`: Tokens per chunk (default: "1536")
- `overlap_percent`: Overlap fraction (default: "0.15")

## Vector Search Integration

The chunks table is designed for delta sync with Databricks Vector Search:

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create delta sync index
vsc.create_delta_sync_index(
    endpoint_name="hedis_vector_endpoint",
    index_name="hedis_chunks_index",
    source_table_name="main.hedis_pipeline.hedis_measures_chunks",
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint_name="databricks-bge-large-en",
    pipeline_type="TRIGGERED"
)
```

**Search Example**:
```python
results = vsc.get_index("hedis_chunks_index").similarity_search(
    query_text="What are the exclusion criteria for diabetes screening?",
    columns=["chunk_id", "chunk_text", "measure_name", "headers"],
    num_results=5
)
```

## Monitoring & Operations

### Pipeline Monitoring

```sql
-- Check bronze processing status
SELECT processing_status, COUNT(*) as count
FROM main.hedis_pipeline.hedis_file_metadata
GROUP BY processing_status;

-- Check silver extraction progress
SELECT effective_year, COUNT(*) as measure_count
FROM main.hedis_pipeline.hedis_measures_definitions
GROUP BY effective_year;

-- Check chunk statistics
SELECT
    effective_year,
    COUNT(*) as chunk_count,
    AVG(token_count) as avg_tokens
FROM main.hedis_pipeline.hedis_measures_chunks
GROUP BY effective_year;
```

### Error Handling

- **Bronze failures**: Check file permissions and volume paths
- **LLM extraction failures**: Retry logic included (3 attempts with exponential backoff)
- **Chunking failures**: Validate PDF structure and TOC parsing

### Performance

Expected processing times (HEDIS 2025, 700+ pages):
- Bronze ingestion: ~5 minutes
- Silver definitions: ~30-60 minutes (LLM-dependent)
- Silver chunks: ~10-15 minutes
- **Total pipeline**: ~60-90 minutes

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -r requirements.txt
pip install pytest pytest-mock

# Run tests
pytest tests/
```

### Code Style

This project follows the style and structure of `examples/lakebase-interrupt-agent/`:
- Type hints throughout
- Google-style docstrings
- Logging with context
- Error handling with try-except
- Databricks-native patterns

## Roadmap

### Phase 1: Core Pipeline (Current)
- [x] Bronze metadata ingestion
- [x] Silver definitions extraction
- [x] Silver chunks processing
- [x] Databricks notebook format (.py with magic commands)

### Phase 2: Enhancements
- [ ] TOC-aware measure boundary detection
- [ ] Multi-year comparison and change tracking
- [ ] Validation metrics and quality scores
- [ ] Automated testing suite

### Phase 3: Production
- [ ] CI/CD with Databricks Asset Bundles
- [ ] Monitoring dashboards
- [ ] SLA tracking and alerting
- [ ] Data lineage visualization

## Contributing

1. Follow the existing code style (see `examples/lakebase-interrupt-agent/`)
2. Add type hints and docstrings
3. Test changes with sample HEDIS PDFs
4. Update README with new features

## License

Proprietary - HEDIS specifications are copyrighted by NCQA. This pipeline is for internal use only.

## Support

For issues or questions:
- Check existing documentation
- Review example notebooks in `examples/`
- Open an issue in the repository

## Acknowledgments

- Built with guidance from the Hive Mind Collective Intelligence System
- Based on architectural patterns from `lakebase-interrupt-agent`
- Chunking algorithm adapted from `examples/measurement/pdf_chunker.py`
- HEDIS specifications copyright NCQA

---

**Generated with Claude Code** | Last updated: 2025-10-23
