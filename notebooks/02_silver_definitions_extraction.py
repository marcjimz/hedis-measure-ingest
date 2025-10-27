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
IMAGE_OUTPUT_PATH = f"{volume_path}/images"

# TOC extraction configuration
TOC_PAGE_LIMIT = 15  # Number of pages to use for TOC extraction

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   Volume Path: {volume_path}")
print(f"   LLM Endpoint: {model_endpoint}")
print(f"   TOC Page Limit: {TOC_PAGE_LIMIT} pages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../src")

from pyspark.sql import functions as F
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Initialize
w = WorkspaceClient()

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Import AI PDF processor
from extraction.ai_pdf_processor import AIPDFProcessor

# Initialize processor
pdf_processor = AIPDFProcessor(spark=spark, workspace_client=w)

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
            'imageOutputPath', '{IMAGE_OUTPUT_PATH}',
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
# MAGIC ## Extraction Pipeline
# MAGIC
# MAGIC This section processes files individually to extract HEDIS measures:
# MAGIC 1. **Parse PDFs** - Use `ai_parse_document` SQL function to extract document structure
# MAGIC 2. **Extract TOC** - Use Python API to identify Table of Contents entries (measure boundaries)
# MAGIC 3. **Extract measure text** - Join TOC with parsed elements by page range
# MAGIC 4. **Use `ai_query`** - Extract structured definitions (denominator, numerator, exclusions) with LLM
# MAGIC 5. **Write to silver** - Idempotent MERGE to silver table
# MAGIC
# MAGIC **Note**: Files are processed individually to avoid `ARGUMENT_NOT_CONSTANT` error from `read_files()` with dynamic paths.

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

if file_count > 0:
    print(f"🔍 Parsing {file_count} document(s) with ai_parse_document...")
    
    # Collect file list (small operation)
    files_list = files_to_process.select("file_id", "file_name", "file_path", "effective_year").collect()
    
    # Parse all documents
    all_parsed_docs = []
    
    from tqdm import tqdm
    for file_row in tqdm(files_list, desc="Parsing documents"):
        try:
            print(f"\n📄 Parsing: {file_row.file_name}")
            
            # Parse the document
            parsed_result = spark.sql(f"""
                SELECT
                    '{file_row.file_id}' AS file_id,
                    '{file_row.file_name}' AS file_name,
                    {file_row.effective_year} AS effective_year,
                    ai_parse_document(
                        content,
                        map(
                            'imageOutputPath', '{IMAGE_OUTPUT_PATH}',
                            'descriptionElementTypes', '*'
                        )
                    ) AS parsed
                FROM READ_FILES(
                    '{file_row.file_path}',
                    format => 'binaryFile'
                )
            """).collect()
            
            if parsed_result:
                result = parsed_result[0]
                all_parsed_docs.append({
                    'file_id': result.file_id,
                    'file_name': result.file_name,
                    'effective_year': result.effective_year,
                    'parsed': result.parsed
                })
                print(f"   ✅ Parsed successfully")
            
        except Exception as e:
            print(f"   ❌ Failed to parse: {str(e)}")
            continue
    
    print(f"\n📊 Successfully parsed {len(all_parsed_docs)} document(s)")
    
    # Create DataFrame from parsed documents
    if all_parsed_docs:
        from pyspark.sql.functions import expr
        
        # Create DataFrame with parsed content
        parsed_docs_df = spark.createDataFrame(all_parsed_docs)
        
        # Extract full text and error status
        parsed_docs_df = parsed_docs_df.withColumn(
            "full_text",
            expr("""
                concat_ws(
                    '\n\n',
                    transform(
                        try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                        element -> try_cast(element:content AS STRING)
                    )
                )
            """)
        ).withColumn(
            "error_status",
            expr("try_cast(parsed:error_status AS STRING)")
        )
        
        # Register as temp view for SQL access
        parsed_docs_df.createOrReplaceTempView("parsed_documents")

        print(f"✅ Created 'parsed_documents' temp view with {parsed_docs_df.count()} document(s)")
        print(f"   Available columns: file_id, file_name, effective_year, parsed, full_text, error_status")

        # Display summary
        display(parsed_docs_df.select("file_id", "file_name", "effective_year", "error_status"))

        # Extract elements with page numbers
        print("\n🔍 Extracting elements with page numbers...")

        elements_df = spark.sql("""
            WITH elements AS (
                SELECT
                    file_id,
                    file_name,
                    effective_year,
                    el,
                    try_cast(el:type AS STRING) AS element_type,
                    try_cast(el:content AS STRING) AS element_content,
                    /* Prefer top-level page_id if present, else fallback to bbox page_id */
                    coalesce(
                        try_cast(el:page_id AS INT),
                        try_cast(el:bbox[0]:page_id AS INT)
                    ) AS page_index_0_based
                FROM parsed_documents
                LATERAL VIEW explode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) e AS el
            )
            SELECT
                file_id,
                file_name,
                effective_year,
                element_type,
                element_content,
                page_index_0_based + 1 AS page_number
            FROM elements
        """)

        # Register as temp view
        elements_df.createOrReplaceTempView("elements")
        element_count = elements_df.count()

        print(f"✅ Created 'elements' temp view with {element_count:,} elements")
        print(f"   Available columns: file_id, file_name, effective_year, element_type, element_content, page_number")

        # Display element summary
        display(elements_df.groupBy("file_name", "element_type").count().orderBy("file_name", "element_type"))
    else:
        print("⚠️  No documents successfully parsed")
else:
    print("⚠️  No files to process")

# COMMAND ----------

# Verify elements temp view exists
if spark.catalog.tableExists("elements"):

    print("🔍 Extracting Table of Contents...")

    # Extract elements from first N pages for TOC
    toc_input_df = spark.sql(f"""
        SELECT
            file_id,
            file_name,
            effective_year,
            concat_ws(
                '\n\n',
                collect_list(element_content)
            ) AS toc_text,
            count(*) AS num_elements_used,
            {TOC_PAGE_LIMIT} AS page_limit
        FROM elements
        WHERE page_number <= {TOC_PAGE_LIMIT}
            AND element_content IS NOT NULL
        GROUP BY file_id, file_name, effective_year
    """)

    toc_input_list = toc_input_df.collect()
    total_docs = len(toc_input_list)

    print(f"📊 Found {total_docs} document(s) to process")
    print(f"   Using first {TOC_PAGE_LIMIT} pages per document")
    print(f"   Processing sequentially (one document at a time)...\n")
    
    # Process each document sequentially
    all_extractions = []
    
    from tqdm import tqdm
    for idx, doc_row in enumerate(tqdm(toc_input_list, desc="Processing documents"), 1):
        print(f"\n[{idx}/{total_docs}] 📋 {doc_row.file_name}")
        print(f"         Pages used: {doc_row.page_limit}")
        print(f"         Elements extracted: {doc_row.num_elements_used:,}")

        if not doc_row.toc_text or doc_row.toc_text.strip() == "":
            raise ValueError(f"No text content for {doc_row.file_name}")

        print(f"         Text length: {len(doc_row.toc_text):,} characters")
        print(f"         Calling ai_query for TOC extraction...")
        
        # Extract TOC using ai_query - only extract measure_acronym, measure_name, start_page
        toc_result = spark.sql(f"""
            SELECT
                ai_query(
                    '{model_endpoint}',
                    concat(
                        'Extract ALL measures from the Table of Contents in this HEDIS document. ',
                        'Find the "Table of Contents" section and extract EVERY SINGLE measure listed. You know these are measures as they will have a acroynym in parantheses next to both the title and page number. It is very important we map the measure to the correct start page number, these should be unique to the measure. Do not add, remove, or edit text, simply extract. Accuracy is VERY important, wrong start_page assignments can negatively impact care.',
                        'For each measure entry on a single line of the TOC, extract: ',
                        '1. measure_acronym: The acronym in parentheses (e.g., "BCS-E", "ADD-E", "WCC") ',
                        '2. measure_name: The full name of the measure ',
                        '3. start_page: The number at the end of the TOC line (the number after the dots) on the SAME line. Example: "Glycemic Status Assessment for Patients With Diabetes (GSD) ..................................................... 134" would return 134 for the start_page\n',
                        'CRITICAL: Extract ALL measures from the entire TOC, not just the first few. The table of contents may span many pages.',
                        'The start_page is always the rightmost number on each TOC line. ',
                        'Return a complete list of ALL measures found in the TOC. ',
                        'Please use tree-of-thought reasoning to complete this task. Be confident with the extractions.',
                        '\n\nDocument:\n',
                        '{doc_row.toc_text.replace("'", "''")}'
                    ),
                    responseFormat => '{{
                        "type": "json_schema",
                        "json_schema": {{
                            "name": "hedis_toc",
                            "strict": true,
                            "schema": {{
                                "type": "object",
                                "properties": {{
                                    "measures": {{
                                        "type": "array",
                                        "items": {{
                                            "type": "object",
                                            "properties": {{
                                                "measure_acronym": {{"type": "string"}},
                                                "measure_name": {{"type": "string"}},
                                                "start_page": {{"type": "integer"}}
                                            }},
                                            "required": ["measure_acronym", "measure_name", "start_page"],
                                            "additionalProperties": false
                                        }}
                                    }}
                                }},
                                "required": ["measures"],
                                "additionalProperties": false
                            }}
                        }}
                    }}'
                ) AS toc_response
        """).collect()
        
        if not toc_result or not toc_result[0].toc_response:
            raise ValueError(f"Empty response from ai_query for {doc_row.file_name}")
        
        toc_response = toc_result[0].toc_response
        
        # Parse the JSON response
        import json
        toc_data = json.loads(toc_response) if isinstance(toc_response, str) else toc_response
        toc_measures = toc_data.get('measures', []) if isinstance(toc_data, dict) else toc_data
        
        print(f"         ✅ Extracted {len(toc_measures)} measures")
        
        all_extractions.append({
            'file_id': doc_row.file_id,
            'file_name': doc_row.file_name,
            'effective_year': doc_row.effective_year,
            'toc_array': toc_measures
        })
    
    # Summary
    print(f"\n{'='*80}")
    print(f"📊 TOC Extraction Summary:")
    print(f"   Total documents: {total_docs}")
    print(f"   Successful: {len(all_extractions)}")
    print(f"{'='*80}\n")
    
    # Create DataFrame from TOC extractions
    if all_extractions:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from pyspark.sql.functions import lead, col
        from pyspark.sql.window import Window
        
        # Flatten the array of measures into individual rows
        all_toc_rows = []
        for extraction in all_extractions:
            for measure in extraction['toc_array']:
                all_toc_rows.append({
                    'file_id': extraction['file_id'],
                    'file_name': extraction['file_name'],
                    'effective_year': extraction['effective_year'],
                    'measure_acronym': measure.get('measure_acronym') if isinstance(measure, dict) else measure.measure_acronym,
                    'measure_name': measure.get('measure_name') if isinstance(measure, dict) else measure.measure_name,
                    'start_page': measure.get('start_page') if isinstance(measure, dict) else measure.start_page
                })
        
        toc_schema = StructType([
            StructField("file_id", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("effective_year", IntegerType(), False),
            StructField("measure_acronym", StringType(), False),
            StructField("measure_name", StringType(), False),
            StructField("start_page", IntegerType(), True)
        ])
        
        toc_df = spark.createDataFrame(all_toc_rows, schema=toc_schema)
        
        # Calculate end_page as (next measure's start_page - 1), null for last measure
        window_spec = Window.partitionBy("file_id", "file_name").orderBy("start_page")
        toc_df = toc_df.withColumn(
            "end_page",
            lead(col("start_page"), 1).over(window_spec) - 1
        )
        
        toc_df.createOrReplaceTempView("toc_entries")
        toc_count = toc_df.count()
        
        print(f"✅ Created 'toc_entries' table with {toc_count} measure entries")
        print(f"   Calculated end_page as (next start_page - 1)")
        
        if toc_count > 0:
            display(toc_df)
        else:
            raise ValueError("No TOC entries extracted")
    else:
        raise ValueError("No TOC entries extracted from any document")
else:
    raise ValueError("'elements' temp view not found. Please run previous cells first.")

# COMMAND ----------

sql = f'''describe table parsed_documents'''

# Execute query and display results
df = spark.sql(sql)
display(df)

# COMMAND ----------

sql = f'''WITH elements AS (
  SELECT
    file_name AS path,
    el,
    try_cast(el:type AS STRING) AS element_type,
    try_cast(el:content AS STRING) AS element_content,
    /* Prefer top-level page_id if present, else fallback to bbox page_id */
    coalesce(try_cast(el:page_id AS INT),
             try_cast(el:bbox[0]:page_id AS INT)) AS page_index_0_based
  FROM parsed_documents
  LATERAL VIEW explode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) e AS el
)
SELECT
  path,
  element_type,
  element_content,
  page_index_0_based + 1 AS page_number
FROM elements'''

# Execute query and display results
df = spark.sql(sql)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### done up to here
# MAGIC
# MAGIC #### next steps:
# MAGIC
# MAGIC 1. For each measure, go to the page, extract the elements, extract text and define schema
# MAGIC 2. Create table with this

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Extract Measure Text and Use ai_query for Structured Extraction

# COMMAND ----------

if file_count > 0 and toc_count > 0:
    print("🤖 Running AI extraction with ai_query...")
    print(f"   Processing {file_count} files with {toc_count} measures")

    # Collect all extracted measures
    all_measures = []

    # Process each file individually
    for file_row in tqdm(files_list, desc="Extracting measures"):
        try:
            print(f"\n📄 Processing: {file_row.file_name}")

            # Parse document with ai_parse_document (using constant path for this file)
            file_path = file_row.file_path

            parse_sql = f"""
            WITH parsed_doc AS (
                SELECT
                    ai_parse_document(
                        content,
                        map('version', '2.0')
                    ) AS parsed
                FROM read_files('{file_path}', format => 'binaryFile')
            ),
            all_elements AS (
                -- Extract all elements with page info
                SELECT
                    try_cast(element:page_id AS INT) as page_id,
                    try_cast(element:content AS STRING) as content
                FROM parsed_doc
                LATERAL VIEW posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) elem_table as elem_idx, element
                WHERE try_cast(element:content AS STRING) IS NOT NULL
            )
            SELECT page_id, content
            FROM all_elements
            ORDER BY page_id
            """

            # Execute parsing
            elements_df = spark.sql(parse_sql)

            # Create temp view for this file's elements
            elements_df.createOrReplaceTempView("current_file_elements")

            # Get TOC entries for this file
            file_toc_sql = f"""
            SELECT
                file_id,
                file_name,
                effective_year,
                measure_acronym,
                measure_name,
                start_page,
                end_page
            FROM toc_entries
            WHERE file_id = '{file_row.file_id}'
            """

            file_toc_df = spark.sql(file_toc_sql)
            file_toc_count = file_toc_df.count()

            if file_toc_count == 0:
                print(f"   No TOC entries found for this file, skipping...")
                continue

            print(f"   Found {file_toc_count} measures in TOC")

            # Extract measure text and use ai_query
            measure_sql = f"""
            WITH file_toc AS (
                SELECT * FROM ({file_toc_sql}) AS toc
            ),
            measure_text AS (
                -- Join TOC with elements to get measure-specific text
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
                FROM file_toc toc
                INNER JOIN current_file_elements elem
                    ON elem.page_id >= toc.start_page
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

            # Execute measure extraction for this file
            file_measures_df = spark.sql(measure_sql)
            file_measure_count = file_measures_df.count()

            print(f"   Extracted {file_measure_count} measures with ai_query")

            # Collect measures from this file
            if file_measure_count > 0:
                file_measures = file_measures_df.collect()
                all_measures.extend(file_measures)

        except Exception as e:
            print(f"❌ Failed to extract measures from {file_row.file_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue

    # Create DataFrame from all measures
    if all_measures:
        from pyspark.sql import Row

        # Convert Row objects to DataFrame
        measures_df = spark.createDataFrame(all_measures)
        measure_count = measures_df.count()

        print(f"\n✅ Extracted {measure_count} total measures from {file_count} files")

        # Create temp view for merge
        measures_df.createOrReplaceTempView("extracted_measures")

        # Display sample
        display(measures_df.limit(10))
    else:
        measure_count = 0
        print("\n⚠️  No measures extracted")
else:
    measure_count = 0
    if file_count == 0:
        print("⚠️  No files to process")
    elif toc_count == 0:
        print("⚠️  No TOC entries found")

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
