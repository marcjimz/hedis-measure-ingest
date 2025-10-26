# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: HEDIS File Metadata Ingestion
# MAGIC
# MAGIC This notebook scans a Unity Catalog volume for HEDIS PDF files and creates bronze metadata entries.
# MAGIC
# MAGIC **Module**: Bronze Ingestion (Step 1 of 3)
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Volume path containing HEDIS PDFs
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Bronze table: `{catalog}.{schema}.hedis_file_metadata`
# MAGIC
# MAGIC **Features**:
# MAGIC - PDF metadata extraction (filename, size, pages, publish date)
# MAGIC - Duplicate detection via checksums
# MAGIC - Idempotent processing

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for parameterization
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "hedis_measurements", "Schema Name")
dbutils.widgets.text("volume_name", "hedis", "Volume Name")
dbutils.widgets.text("file_pattern", "HEDIS*.pdf", "File Pattern")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
file_pattern = dbutils.widgets.get("file_pattern")

# Construct paths
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
bronze_table = f"{catalog_name}.{schema_name}.hedis_file_metadata"

print(f"📋 Configuration:")
print(f"   Catalog: {catalog_name}")
print(f"   Schema: {schema_name}")
print(f"   Volume: {volume_path}")
print(f"   Bronze Table: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../src")
from databricks.sdk import WorkspaceClient
import uuid
from datetime import datetime
import re

# Initialize clients
w = WorkspaceClient()

# Import custom modules
from extraction.pdf_processor import PDFProcessor

# Initialize PDF processor
pdf_processor = PDFProcessor(workspace_client=w)

print("✅ Environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Table Schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    LongType
)

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Define bronze table schema
bronze_schema = StructType([
    StructField("file_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("volume_ingestion_date", TimestampType(), True),
    StructField("published_date", StringType(), True),
    StructField("effective_year", IntegerType(), True),
    StructField("file_size_bytes", LongType(), True),
    StructField("page_count", IntegerType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("last_modified", TimestampType(), True),
    StructField("checksum", StringType(), True)
])

# Create table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        file_id STRING NOT NULL,
        file_name STRING NOT NULL,
        file_path STRING NOT NULL,
        volume_ingestion_date TIMESTAMP,
        published_date STRING,
        effective_year INT,
        file_size_bytes LONG,
        page_count INT,
        ingestion_timestamp TIMESTAMP,
        last_modified TIMESTAMP,
        checksum STRING
    )
    USING DELTA
    COMMENT 'Bronze layer: HEDIS file metadata'
""")

print(f"✅ Bronze table created/verified: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Volume for PDFs

# COMMAND ----------

import os

try:
    files_response = w.files.list_directory_contents(directory_path=volume_path)
    files = list(files_response)
    
    # Filter for PDFs matching pattern
    pdf_files = [f for f in files if f.name.endswith('.pdf') and re.match(file_pattern.replace('*', '.*'), f.name)]
    
    if len(pdf_files) == 0:
        raise Exception("No files found, nothing to process. Please upload files to the volume to proceed with processing.")
    
    print(f"📁 Found {len(pdf_files)} PDF files matching pattern '{file_pattern}':")
    for f in pdf_files:
        file_size = os.path.getsize(f.path)
        print(f"   - {f.name} ({file_size:,} bytes)")
        
except Exception as e:
    raise Exception(f"Error accessing volume: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Metadata from Each PDF

# COMMAND ----------

from tqdm import tqdm

metadata_records = []
failed_files = []

for file_info in tqdm(pdf_files, desc="Processing PDFs"):
    try:
        file_path = file_info.path
        file_name = file_info.name
        
        # Get file size using os.path.getsize since DirectoryEntry doesn't have size attribute
        file_size = os.path.getsize(file_path)
        
        # Get modification time
        modification_time = os.path.getmtime(file_path)

        # Read PDF
        pdf_bytes = pdf_processor.read_pdf_from_volume(file_path)

        # Extract metadata
        page_count = pdf_processor.get_page_count(pdf_bytes)
        checksum = pdf_processor.calculate_checksum(pdf_bytes)

        # Extract effective year from filename (e.g., "HEDIS MY 2025...")
        year_match = re.search(r'20\d{2}', file_name)
        effective_year = int(year_match.group(0)) if year_match else None

        # Extract publication date from filename (e.g., "...2025-03-31.pdf")
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
        published_date = date_match.group(1) if date_match else None

        # Create metadata record
        metadata_records.append({
            "file_id": str(uuid.uuid4()),
            "file_name": file_name,
            "file_path": file_path,
            "volume_ingestion_date": datetime.fromtimestamp(modification_time),
            "published_date": published_date,
            "effective_year": effective_year,
            "file_size_bytes": file_size,
            "page_count": page_count,
            "ingestion_timestamp": datetime.now(),
            "last_modified": datetime.now(),
            "checksum": checksum
        })

        print(f"✅ Extracted metadata: {file_name} ({page_count} pages)")

    except Exception as e:
        failed_files.append(file_info.name)
        print(f"❌ Failed to process {file_info.name}: {str(e)}")

# Check if all files were processed successfully
if failed_files:
    raise Exception(
        f"Failed to process {len(failed_files)} out of {len(pdf_files)} files. "
        f"Failed files: {', '.join(failed_files)}"
    )

print(f"\n📊 Successfully extracted metadata from {len(metadata_records)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table

# COMMAND ----------

from pyspark.sql import functions as F

if metadata_records:
    # Create DataFrame
    metadata_df = spark.createDataFrame(metadata_records, schema=bronze_schema)

    # Read existing records
    existing_df = spark.table(bronze_table)

    # Merge - only insert if checksum doesn't exist
    metadata_df.createOrReplaceTempView("new_metadata")

    spark.sql(f"""
        MERGE INTO {bronze_table} target
        USING new_metadata source
        ON target.checksum = source.checksum
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    # Show results
    result_count = spark.table(bronze_table).count()
    print(f"\n✅ Bronze ingestion complete!")
    print(f"   Total records in bronze table: {result_count}")

    # Display sample
    display(spark.table(bronze_table).orderBy(F.desc("ingestion_timestamp")).limit(10))
else:
    print("⚠️  No new files to ingest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
summary = spark.sql(f"""
    SELECT
        file_name,
        COUNT(*) as count,
        SUM(page_count) as total_pages,
        SUM(file_size_bytes) / 1024 / 1024 as total_size_mb
    FROM {bronze_table} 
    GROUP BY file_name
""")

print("📊 Bronze Table Summary:")
display(summary)
