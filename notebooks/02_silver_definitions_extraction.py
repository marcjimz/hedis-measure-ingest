# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Definitions Extraction
# MAGIC
# MAGIC This notebook extracts structured HEDIS measure definitions from PDFs using Databricks `ai_parse_document` and `ai_query` SQL functions.
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
# MAGIC - SQL-based structured extraction with `ai_query`
# MAGIC - Idempotent writes with MERGE
# MAGIC
# MAGIC **Future Enhancement**: Consider using Delta Change Data Feed (CDF) to process only new/updated files incrementally. Enable CDF on bronze table with `TBLPROPERTIES (delta.enableChangeDataFeed = true)` and use `table_changes()` function to track changes.

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
dbutils.widgets.text("volume_name", "hedis", "Volume Name")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4-5", "LLM Model Endpoint")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
model_endpoint = dbutils.widgets.get("model_endpoint")

# Table names
bronze_table = f"{catalog_name}.{schema_name}.hedis_file_metadata"
silver_table = f"{catalog_name}.{schema_name}.hedis_measures_definitions"
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   Volume Path: {volume_path}")
print(f"   LLM Endpoint: {model_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

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
# MAGIC WITH parsed_documents AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     ai_parse_document(
# MAGIC       content,
# MAGIC       map(
# MAGIC         'imageOutputPath', '/Volumes/catalog/schema/volume/parsed_images/',
# MAGIC         'descriptionElementTypes', '*'
# MAGIC       )
# MAGIC     ) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/catalog/schema/volume/*.pdf', format => 'binaryFile')
# MAGIC )
# MAGIC SELECT * FROM parsed_documents WHERE try_cast(parsed:error_status AS STRING) IS NULL;
# MAGIC ```
# MAGIC
# MAGIC The demo below shows the function in action with a sample HEDIS file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: Extract Structure from Sample PDF

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: if using serverless, make sure you are environment version `4 - Python 3.12, Scala 2.13`

# COMMAND ----------

sample_path

# COMMAND ----------

# DBTITLE 1,Document Extraction - This cell may take some time
import os

# Get a sample file path from bronze table; this does one, in future we will scale this to many files.
sample_file = spark.sql(f"""
    SELECT file_path, file_name
    FROM {bronze_table}
    LIMIT 1
""").collect()

if sample_file:
    sample_path = sample_file[0].file_path
    directory = os.path.dirname(sample_path)
    sample_name = sample_file[0].file_name

    print(f"📄 Demonstrating ai_parse_document with: {sample_name}")
    print(f"   Path: {sample_path}")

    sql = f'''
        with parsed_documents AS (
        SELECT
            path,
            ai_parse_document(content
            ,
            map(
            'version', '2.0',
            'imageOutputPath', '{directory}/parsed_images/',
            'descriptionElementTypes', '*'
            )
        ) as parsed
        FROM
            read_files('{sample_path}', format => 'binaryFile')
        )
        select * from parsed_documents
        '''

    parsed_results = [row.parsed for row in spark.sql(sql).collect()]
else:
    print("⚠️  No files in bronze table yet - run bronze ingestion first")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactive Document Viewer
# MAGIC
# MAGIC Use the controls below to navigate through pages. The viewer provides:
# MAGIC - **Previous/Next buttons** for sequential navigation
# MAGIC - **Slider** for quick page selection
# MAGIC - **Dropdown** for precise page selection
# MAGIC - **Hover tooltips** over bounding boxes to see element content

# COMMAND ----------

from src.extraction.document_renderer import render_ai_parse_output_interactive

# Launch interactive viewer with page navigation
render_ai_parse_output_interactive(parsed_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        measure_id STRING NOT NULL,
        file_id STRING NOT NULL,
        file_name STRING,
        measure_acronym STRING,
        measure_name STRING NOT NULL,
        page_start INT,
        page_end INT,
        specifications STRING,
        denominator STRING,
        numerator STRING,
        exclusions STRING,
        effective_year INT NOT NULL,
        extracted_json STRING,
        extraction_timestamp TIMESTAMP,
        source_text STRING
    )
    USING DELTA
    COMMENT 'Silver layer: HEDIS measure definitions extracted with ai_query'
    PARTITIONED BY (effective_year)
""")

print(f"✅ Silver table created/verified: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL-Based Extraction Pipeline
# MAGIC
# MAGIC This section uses SQL CTEs to:
# MAGIC 1. Parse PDFs with `ai_parse_document`
# MAGIC 2. Extract text from document elements
# MAGIC 3. Identify Table of Contents entries (page boundaries)
# MAGIC 4. Extract measure text by page range
# MAGIC 5. Use `ai_query` to extract structured definitions
# MAGIC 6. Write results to silver table with MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Parse Documents and Extract TOC

# COMMAND ----------

# Get files that haven't been processed yet
files_to_process = spark.sql(f"""
    SELECT b.file_id, b.file_path, b.file_name, b.effective_year
    FROM {bronze_table} b
    LEFT JOIN (
        SELECT DISTINCT file_id
        FROM {silver_table}
    ) s ON b.file_id = s.file_id
    WHERE s.file_id IS NULL
    ORDER BY b.ingestion_timestamp DESC
""")

file_count = files_to_process.count()
print(f"📁 Found {file_count} files to process")

if file_count > 0:
    display(files_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Extract TOC and Measure Text with SQL
# MAGIC
# MAGIC We want to find the table of contents so we understand where each and every measure lives.

# COMMAND ----------

# Create a SQL query that processes all files at once
# This uses ai_parse_document to extract text, finds TOC entries, and segments by page ranges

if file_count > 0:
    # Create temp view of files to process
    files_to_process.createOrReplaceTempView("files_to_process")

    # SQL extraction pipeline
    extraction_sql = f"""
    WITH parsed_documents AS (
        -- Parse all PDFs with ai_parse_document
        SELECT
            f.file_id,
            f.file_name,
            f.effective_year,
            f.file_path as path,
            ai_parse_document(
                rf.content,
                map(
                    'version', '2.0',
                    'imageOutputPath', '{volume_path}/parsed_images/',
                    'descriptionElementTypes', '*'
                )
            ) AS parsed
        FROM files_to_process f
        CROSS JOIN LATERAL read_files(f.file_path, format => 'binaryFile') rf
        WHERE try_cast(parsed:error_status AS STRING) IS NULL
    ),
    extracted_text AS (
        -- Extract all text content from elements with page information
        SELECT
            file_id,
            file_name,
            effective_year,
            path,
            posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) as (element_idx, element)
        FROM parsed_documents
    ),
    element_details AS (
        -- Get element content and page_id
        SELECT
            file_id,
            file_name,
            effective_year,
            element_idx,
            try_cast(element:page_id AS INT) as page_id,
            try_cast(element:type AS STRING) as element_type,
            try_cast(element:content AS STRING) as content
        FROM extracted_text
    ),
    toc_candidates AS (
        -- Find TOC entries in first 30 pages using regex pattern
        -- Pattern: "AMM - Antidepressant Medication Management ... 45"
        SELECT
            file_id,
            file_name,
            effective_year,
            page_id,
            content,
            regexp_extract(content, '([A-Z]{{2,5}})\\\\s*[-–—]\\\\s*(.+?)\\\\s+\\\\.{{2,}}\\\\s+(\\\\d+)', 1) as measure_acronym,
            regexp_extract(content, '([A-Z]{{2,5}})\\\\s*[-–—]\\\\s*(.+?)\\\\s+\\\\.{{2,}}\\\\s+(\\\\d+)', 2) as measure_title,
            regexp_extract(content, '([A-Z]{{2,5}})\\\\s*[-–—]\\\\s*(.+?)\\\\s+\\\\.{{2,}}\\\\s+(\\\\d+)', 3) as start_page
        FROM element_details
        WHERE page_id < 30
          AND content IS NOT NULL
          AND content RLIKE '([A-Z]{{2,5}})\\\\s*[-–—]\\\\s*.+?\\\\s+\\\\.{{2,}}\\\\s+\\\\d+'
    ),
    toc_entries AS (
        -- Clean TOC entries and assign end pages
        SELECT
            file_id,
            file_name,
            effective_year,
            measure_acronym,
            trim(measure_title) as measure_title,
            concat(measure_acronym, ' - ', trim(measure_title)) as measure_name,
            cast(start_page as int) as start_page,
            LEAD(cast(start_page as int), 1, cast(start_page as int) + 15) OVER (
                PARTITION BY file_id
                ORDER BY cast(start_page as int)
            ) - 1 as end_page
        FROM toc_candidates
        WHERE measure_acronym != ''
          AND start_page != ''
    )
    SELECT * FROM toc_entries
    ORDER BY file_id, start_page
    """

    # Execute TOC extraction
    toc_df = spark.sql(extraction_sql)
    toc_count = toc_df.count()

    print(f"📊 Extracted {toc_count} TOC entries")

    if toc_count > 0:
        # Create temp view for next step
        toc_df.createOrReplaceTempView("toc_entries")
        display(toc_df.limit(10))
else:
    print("⚠️  No files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Extract Measure Text and Use ai_query for Structured Extraction

# COMMAND ----------

if file_count > 0 and toc_count > 0:
    # SQL query to extract measure text and use ai_query
    measure_extraction_sql = f"""
    WITH parsed_documents AS (
        -- Re-parse documents for full text extraction
        SELECT
            f.file_id,
            f.file_name,
            f.effective_year,
            ai_parse_document(
                rf.content,
                map('version', '2.0')
            ) AS parsed
        FROM files_to_process f
        CROSS JOIN LATERAL read_files(f.file_path, format => 'binaryFile') rf
    ),
    all_elements AS (
        -- Get all elements with page info
        SELECT
            file_id,
            file_name,
            effective_year,
            try_cast(element:page_id AS INT) as page_id,
            try_cast(element:content AS STRING) as content
        FROM parsed_documents
        LATERAL VIEW posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) elem_table as elem_idx, element
    ),
    measure_text AS (
        -- Join TOC entries with elements to get measure-specific text
        SELECT
            toc.file_id,
            toc.file_name,
            toc.effective_year,
            toc.measure_acronym,
            toc.measure_name,
            toc.start_page,
            toc.end_page,
            concat_ws('\\n\\n',
                collect_list(elem.content)
            ) as full_text
        FROM toc_entries toc
        INNER JOIN all_elements elem
            ON toc.file_id = elem.file_id
            AND elem.page_id >= toc.start_page
            AND elem.page_id <= toc.end_page
        WHERE elem.content IS NOT NULL
        GROUP BY
            toc.file_id,
            toc.file_name,
            toc.effective_year,
            toc.measure_acronym,
            toc.measure_name,
            toc.start_page,
            toc.end_page
    ),
    extracted_measures AS (
        -- Use ai_query to extract structured definitions
        SELECT
            uuid() as measure_id,
            file_id,
            file_name,
            measure_acronym,
            measure_name,
            start_page as page_start,
            end_page as page_end,
            effective_year,
            current_timestamp() as extraction_timestamp,
            substring(full_text, 1, 5000) as source_text,
            ai_query(
                '{model_endpoint}',
                concat(
                    'Extract HEDIS measure definition from this document. ',
                    'Return a JSON object with these exact keys: ',
                    'specifications (string), denominator (string), numerator (string), exclusions (string). ',
                    'If a field is not found, use empty string. ',
                    'Document text:\\n\\n',
                    full_text
                ),
                returnType => 'STRING'
            ) AS extracted_json
        FROM measure_text
        WHERE length(full_text) > 100
    )
    SELECT
        measure_id,
        file_id,
        file_name,
        measure_acronym,
        measure_name,
        page_start,
        page_end,
        effective_year,
        extracted_json,
        -- Parse JSON fields
        get_json_object(extracted_json, '$.specifications') as specifications,
        get_json_object(extracted_json, '$.denominator') as denominator,
        get_json_object(extracted_json, '$.numerator') as numerator,
        get_json_object(extracted_json, '$.exclusions') as exclusions,
        extraction_timestamp,
        source_text
    FROM extracted_measures
    """

    # Execute measure extraction
    print("🤖 Running AI extraction with ai_query...")
    measures_df = spark.sql(measure_extraction_sql)

    # Show sample
    measure_count = measures_df.count()
    print(f"✅ Extracted {measure_count} measures")

    if measure_count > 0:
        display(measures_df.limit(10))

        # Create temp view for merge
        measures_df.createOrReplaceTempView("extracted_measures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table (Idempotent MERGE)

# COMMAND ----------

if file_count > 0 and toc_count > 0 and measure_count > 0:
    # MERGE to make idempotent - upsert based on file_id + measure_acronym + page_start
    spark.sql(f"""
        MERGE INTO {silver_table} target
        USING extracted_measures source
        ON target.file_id = source.file_id
           AND target.measure_acronym = source.measure_acronym
           AND target.page_start = source.page_start
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").first()["cnt"]
    print(f"✅ Wrote {measure_count} measures to silver table (MERGE)")
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
        COUNT(DISTINCT file_id) as file_count,
        COUNT(DISTINCT measure_acronym) as unique_measures
    FROM {silver_table}
    GROUP BY effective_year
    ORDER BY effective_year DESC
""")

print("📊 Silver Table Summary:")
display(summary)
