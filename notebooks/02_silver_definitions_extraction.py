# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Definitions Extraction
# MAGIC
# MAGIC This notebook extracts structured HEDIS measure definitions from PDFs using Databricks `ai_parse_document` and LLM-based extraction.
# MAGIC
# MAGIC **Module**: Silver Definitions (Step 2 of 3)
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Bronze table: `{catalog}.{schema}.hedis_file_metadata`
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Silver table: `{catalog}.{schema}.hedis_measures_definitions`
# MAGIC
# MAGIC **Features**:
# MAGIC - AI-powered PDF parsing with `ai_parse_document` SQL function
# MAGIC - Table of Contents parsing for measure boundaries
# MAGIC - LLM-based structured extraction (Llama 3.3 70B)
# MAGIC - Validation and error handling

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "hedis_measurements", "Schema Name")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4-5", "LLM Model Endpoint")
dbutils.widgets.text("batch_size", "10", "Batch Size")
dbutils.widgets.dropdown("processing_mode", "incremental", ["incremental", "full_refresh"], "Processing Mode")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_endpoint = dbutils.widgets.get("model_endpoint")
batch_size = int(dbutils.widgets.get("batch_size"))
processing_mode = dbutils.widgets.get("processing_mode")

# Table names
bronze_table = f"{catalog_name}.{schema_name}.hedis_file_metadata"
silver_table = f"{catalog_name}.{schema_name}.hedis_measures_definitions"

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   LLM Endpoint: {model_endpoint}")
print(f"   Processing Mode: {processing_mode}")
print(f"   Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../src")

from pyspark.sql.types import *
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
import uuid
from datetime import datetime
from tqdm import tqdm

# Initialize
w = WorkspaceClient()

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Import modules
from extraction.ai_pdf_processor import AIPDFProcessor
from extraction.llm_extractor import LLMExtractor

# Initialize processors
pdf_processor = AIPDFProcessor(spark=spark, workspace_client=w)
llm_extractor = LLMExtractor(model_endpoint=model_endpoint, temperature=0.0)

print("✅ Environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI-Powered PDF Parsing with `ai_parse_document`
# MAGIC
# MAGIC This pipeline uses Databricks' native `ai_parse_document` SQL function (Runtime 17.1+) for PDF text extraction.
# MAGIC The function returns structured JSON with classified elements (text, table, header, figure) and handles complex layouts.
# MAGIC
# MAGIC **Basic SQL usage:**
# MAGIC ```sql
# MAGIC SELECT ai_parse_document(content, map('version', '2.0')) as parsed_doc
# MAGIC FROM read_files('/Volumes/catalog/schema/volume/file.pdf', format => 'binaryFile')
# MAGIC ```
# MAGIC
# MAGIC The demo below shows the function in action with a sample HEDIS file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: Extract Structure from Sample PDF

# COMMAND ----------

sample_path

# COMMAND ----------

sample_file[0]

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ai_parse_document(
# MAGIC   content,
# MAGIC   map('version', '2.0')
# MAGIC ) AS parsed
# MAGIC FROM READ_FILES('/Volumes/marcin_demo/hedis_measurements/hedis/*.{pdf}', format => 'binaryFile');

# COMMAND ----------

sample_directory

# COMMAND ----------

import os

# Get a sample file path from bronze table
sample_file = spark.sql(f"""
    SELECT file_path, file_name
    FROM {bronze_table}
    LIMIT 1
""").collect()

if sample_file:
    sample_path = sample_file[0].file_path
    sample_directory = os.path.dirname(sample_path)
    sample_name = sample_file[0].file_name

    print(f"📄 Demonstrating ai_parse_document with: {sample_name}")
    print(f"   Path: {sample_path}")

    # Use ai_parse_document SQL function directly
    parsed_result = spark.sql(f"""
        SELECT
            path,
            ai_parse_document(content, map('version', '2.0')) as parsed_doc
        FROM READ_FILES('{sample_directory}', format => 'binaryFile');
    """).first()

    parsed_doc = parsed_result.parsed_doc
    document = parsed_doc.get('document', {})

    # Display structure
    print(f"\n📊 Parsed Document Structure:")
    print(f"   Pages: {len(document.get('pages', []))}")
    print(f"   Elements: {len(document.get('elements', []))}")

    # Show element type breakdown
    elements = document.get('elements', [])
    if elements:
        element_types = {}
        for elem in elements[:100]:  # Sample first 100 elements
            elem_type = elem.get('type', 'unknown')
            element_types[elem_type] = element_types.get(elem_type, 0) + 1

        print(f"\n   Element Types (first 100):")
        for elem_type, count in sorted(element_types.items(), key=lambda x: x[1], reverse=True):
            print(f"     • {elem_type}: {count}")

        # Show a sample table element if exists
        tables = [e for e in elements if e.get('type') == 'table']
        if tables:
            print(f"\n   Sample Table (HTML format):")
            print(f"     {tables[0].get('content', '')[:150]}...")
else:
    print("⚠️  No files in bronze table yet - run bronze ingestion first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        measure_id STRING NOT NULL,
        file_id STRING NOT NULL,
        specifications STRING NOT NULL,
        measure STRING NOT NULL,
        initial_pop STRING,
        denominator ARRAY<STRING>,
        numerator ARRAY<STRING>,
        exclusion ARRAY<STRING>,
        effective_year INT NOT NULL,
        page_start INT,
        page_end INT,
        extraction_timestamp TIMESTAMP,
        extraction_confidence DOUBLE,
        source_text STRING
    )
    USING DELTA
    COMMENT 'Silver layer: HEDIS measure definitions'
    PARTITIONED BY (effective_year)
""")

print(f"✅ Silver table created/verified: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Files to Process (Idempotent)
# MAGIC
# MAGIC Two processing modes:
# MAGIC - **Incremental**: Uses Change Data Feed to process only new/changed files
# MAGIC - **Full Refresh**: Reprocesses all files (uses MERGE for idempotency)

# COMMAND ----------

if processing_mode == "incremental":
    print(f"📊 Incremental mode: Using Change Data Feed to detect new files")

    # Get last processed version from silver table (or 0 if first run)
    try:
        last_version = spark.sql(f"""
            SELECT MAX(_commit_version) as max_version
            FROM table_changes('{bronze_table}', 0)
            WHERE file_id IN (SELECT DISTINCT file_id FROM {silver_table})
        """).first()["max_version"]

        if last_version is None:
            last_version = 0
            print(f"   First run detected, starting from version 0")
        else:
            print(f"   Last processed version: {last_version}")
    except:
        last_version = 0
        print(f"   No prior processing detected, starting from version 0")

    # Get new files from CDF
    files_to_process = spark.sql(f"""
        SELECT DISTINCT b.*
        FROM table_changes('{bronze_table}', {last_version}) cdf
        INNER JOIN {bronze_table} b ON cdf.file_id = b.file_id
        WHERE cdf._change_type IN ('insert', 'update_postimage')
        ORDER BY b.ingestion_timestamp DESC
        LIMIT {batch_size}
    """).collect()

    print(f"   📁 Found {len(files_to_process)} new/updated files since version {last_version}")

else:  # full_refresh
    print(f"🔄 Full refresh mode: Processing all files (idempotent with MERGE)")

    # Get files not yet processed (compare bronze vs silver)
    files_to_process = spark.sql(f"""
        SELECT b.*
        FROM {bronze_table} b
        LEFT JOIN (
            SELECT DISTINCT file_id
            FROM {silver_table}
        ) s ON b.file_id = s.file_id
        WHERE s.file_id IS NULL
        ORDER BY b.ingestion_timestamp DESC
        LIMIT {batch_size}
    """).collect()

    print(f"   📁 Found {len(files_to_process)} unprocessed files")

# Display files
if files_to_process:
    for f in files_to_process:
        print(f"   - {f.file_name} ({f.page_count} pages)")
else:
    print("   ✅ No new files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Measures from Each File

# COMMAND ----------

all_measures = []

for file_row in tqdm(files_to_process, desc="Processing files"):
    try:
        print(f"\n📄 Processing: {file_row.file_name}")

        # Parse table of contents using ai_parse_document
        toc_entries = pdf_processor.extract_table_of_contents(file_row.file_path)
        print(f"   Found {len(toc_entries)} measures in TOC")

        # Extract each measure
        for entry in tqdm(toc_entries[:5], desc="Extracting measures", leave=False):  # Limit to 5 for demo
            try:
                measure_acronym = entry['measure_acronym']
                start_page = entry['start_page']
                end_page = entry.get('end_page', start_page + 10)

                print(f"   Extracting {measure_acronym} (pages {start_page}-{end_page})...")

                # Extract text from measure pages using ai_parse_document
                pages = pdf_processor.extract_text_from_pages(
                    file_row.file_path,
                    start_page=start_page,
                    end_page=end_page
                )

                # Combine page text
                measure_text = "\n\n".join([p.text for p in pages])

                # Extract with LLM (effective_year from file metadata)
                measure_dict = llm_extractor.extract_with_retry(
                    text=measure_text,
                    effective_year=file_row.effective_year,
                    max_retries=3
                )

                # Add metadata
                measure_record = {
                    "measure_id": str(uuid.uuid4()),
                    "file_id": file_row.file_id,
                    "specifications": measure_dict.get("Specifications", ""),
                    "measure": measure_dict.get("measure", ""),
                    "initial_pop": measure_dict.get("Initial_Pop"),
                    "denominator": measure_dict.get("denominator", []),
                    "numerator": measure_dict.get("numerator", []),
                    "exclusion": measure_dict.get("exclusion", []),
                    "effective_year": measure_dict.get("effective_year", file_row.effective_year),
                    "page_start": start_page,
                    "page_end": end_page,
                    "extraction_timestamp": datetime.now(),
                    "extraction_confidence": 0.95,  # Placeholder
                    "source_text": measure_text[:5000]  # Store excerpt
                }

                all_measures.append(measure_record)
                print(f"      ✅ Extracted: {measure_dict.get('measure', 'Unknown')}")

            except Exception as e:
                print(f"      ❌ Failed to extract {entry.get('measure_name', 'Unknown')}: {str(e)}")

        print(f"✅ Completed: {file_row.file_name}")

    except Exception as e:
        print(f"❌ Failed to process file: {str(e)}")

print(f"\n📊 Extracted {len(all_measures)} measures total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table (Idempotent MERGE)

# COMMAND ----------

if all_measures:
    # Create DataFrame
    measures_df = spark.createDataFrame(all_measures)

    # Create temp view for merge
    measures_df.createOrReplaceTempView("new_measures")

    # MERGE to make idempotent - upsert based on file_id + measure name
    spark.sql(f"""
        MERGE INTO {silver_table} target
        USING new_measures source
        ON target.file_id = source.file_id
           AND target.measure = source.measure
           AND target.page_start = source.page_start
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").first()["cnt"]
    print(f"✅ Wrote {len(all_measures)} measures to silver table (MERGE)")
    print(f"   Total measures in table: {result_count}")

    # Display sample
    display(spark.table(silver_table).orderBy(F.desc("extraction_timestamp")).limit(10))
else:
    print("⚠️  No measures extracted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        effective_year,
        COUNT(*) as measure_count,
        COUNT(DISTINCT file_id) as file_count
    FROM {silver_table}
    GROUP BY effective_year
    ORDER BY effective_year DESC
""")

print("📊 Silver Table Summary:")
display(summary)
