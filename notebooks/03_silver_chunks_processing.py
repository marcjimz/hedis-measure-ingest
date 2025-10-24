# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Chunks Processing
# MAGIC
# MAGIC This notebook chunks HEDIS documents for vector search with semantic preservation.
# MAGIC
# MAGIC **Module**: Silver Chunks (Step 3 of 3)
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Bronze table: `{catalog}.{schema}.hedis_file_metadata` (status='completed')
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Silver table: `{catalog}.{schema}.hedis_measures_chunks`
# MAGIC
# MAGIC **Features**:
# MAGIC - Header-aware chunking with overlap
# MAGIC - Measure context preservation
# MAGIC - Ready for vector search delta sync

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
dbutils.widgets.text("schema_name", "hedis_pipeline", "Schema Name")
dbutils.widgets.text("chunk_size", "1536", "Chunk Size (tokens)")
dbutils.widgets.text("overlap_percent", "0.15", "Overlap Percent")
dbutils.widgets.text("effective_year", "2025", "Effective Year")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
chunk_size = int(dbutils.widgets.get("chunk_size"))
overlap_percent = float(dbutils.widgets.get("overlap_percent"))
effective_year = int(dbutils.widgets.get("effective_year"))

# Table names
bronze_table = f"{catalog_name}.{schema_name}.hedis_file_metadata"
silver_table = f"{catalog_name}.{schema_name}.hedis_measures_chunks"

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   Chunk Size: {chunk_size} tokens")
print(f"   Overlap: {overlap_percent * 100}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../src")

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
import uuid
from datetime import datetime
from tqdm import tqdm
import json

# Initialize
spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Import modules
from extraction.ai_pdf_processor import AIPDFProcessor
from extraction.chunker import HEDISChunker

# Initialize processors
pdf_processor = AIPDFProcessor(spark=spark, workspace_client=w)
chunker = HEDISChunker(chunk_size=chunk_size, overlap_percent=overlap_percent)

print("✅ Environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Chunks Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        chunk_id STRING NOT NULL,
        file_id STRING NOT NULL,
        measure_name STRING,
        chunk_text STRING NOT NULL,
        chunk_sequence INT NOT NULL,
        token_count INT,
        page_start INT,
        page_end INT,
        headers ARRAY<STRING>,
        char_start LONG,
        char_end LONG,
        effective_year INT,
        chunk_timestamp TIMESTAMP,
        metadata STRING
    )
    USING DELTA
    COMMENT 'Silver layer: HEDIS measure chunks for vector search'
    PARTITIONED BY (effective_year)
""")

print(f"✅ Silver chunks table created/verified: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Completed Files from Bronze

# COMMAND ----------

completed_files = spark.sql(f"""
    SELECT *
    FROM {bronze_table}
    WHERE processing_status = 'completed'
    AND effective_year = {effective_year}
    ORDER BY ingestion_timestamp DESC
""").collect()

print(f"📁 Found {len(completed_files)} completed files to chunk")
for f in completed_files:
    print(f"   - {f.file_name} ({f.page_count} pages)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunk Each File

# COMMAND ----------

all_chunks = []

for file_row in tqdm(completed_files, desc="Chunking files"):
    try:
        print(f"\n📄 Chunking: {file_row.file_name}")

        # Read PDF
        pdf_bytes = pdf_processor.read_pdf_from_volume(file_row.file_path)

        # Parse TOC for measure context
        toc_entries = pdf_processor.extract_table_of_contents(pdf_bytes)

        # Create measure name lookup by page
        page_to_measure = {}
        for entry in toc_entries:
            start = entry['start_page']
            end = entry.get('end_page', start + 10)
            for page in range(start, end + 1):
                page_to_measure[page] = entry['measure_name']

        # Chunk entire document
        chunks = chunker.chunk_document(pdf_bytes)

        print(f"   Generated {len(chunks)} chunks")

        # Convert to records
        for idx, chunk in enumerate(chunks):
            # Determine measure name from page
            measure_name = page_to_measure.get(chunk.start_page, None)

            # Create metadata JSON
            metadata = {
                "file_name": file_row.file_name,
                "effective_year": file_row.effective_year,
                "measure_name": measure_name,
                "chunk_strategy": "header_aware_overlap"
            }

            chunk_record = {
                "chunk_id": str(uuid.uuid4()),
                "file_id": file_row.file_id,
                "measure_name": measure_name,
                "chunk_text": chunk.text,
                "chunk_sequence": idx,
                "token_count": chunk.token_count,
                "page_start": chunk.start_page,
                "page_end": chunk.end_page,
                "headers": chunk.headers,
                "char_start": chunk.char_start,
                "char_end": chunk.char_end,
                "effective_year": file_row.effective_year,
                "chunk_timestamp": datetime.now(),
                "metadata": json.dumps(metadata)
            }

            all_chunks.append(chunk_record)

        print(f"✅ Chunked: {file_row.file_name} → {len(chunks)} chunks")

    except Exception as e:
        print(f"❌ Failed to chunk file: {str(e)}")

print(f"\n📊 Generated {len(all_chunks)} total chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Chunks Table

# COMMAND ----------

if all_chunks:
    # Create DataFrame
    chunks_df = spark.createDataFrame(all_chunks)

    # Write to silver table (append)
    chunks_df.write.mode("append").saveAsTable(silver_table)

    print(f"✅ Wrote {len(all_chunks)} chunks to silver table")

    # Display sample
    display(spark.table(silver_table).orderBy(F.desc("chunk_timestamp")).limit(10))
else:
    print("⚠️  No chunks generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Vector Search Preparation

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        effective_year,
        COUNT(*) as chunk_count,
        COUNT(DISTINCT file_id) as file_count,
        COUNT(DISTINCT measure_name) as measure_count,
        AVG(token_count) as avg_tokens,
        SUM(token_count) as total_tokens
    FROM {silver_table}
    GROUP BY effective_year
    ORDER BY effective_year DESC
""")

print("📊 Silver Chunks Summary:")
display(summary)

print("\n✅ Chunks are ready for vector search delta sync!")
print(f"   Next step: Configure Databricks Vector Search to sync from {silver_table}")
