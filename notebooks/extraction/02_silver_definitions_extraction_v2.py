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
# MAGIC **PERFORMANCE OPTIMIZATION**: This version eliminates DataFrame → collect() → DataFrame pattern that caused driver OOM crashes
# MAGIC
# MAGIC **Future Enhancement**: Consider using Delta Change Data Feed (CDF) to process only new/updated files incrementally. Enable CDF on bronze table with `TBLPROPERTIES (delta.enableChangeDataFeed = true)` and use `table_changes()` function to track changes.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

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
dbutils.widgets.text("model_endpoint", "databricks-claude-opus-4-1", "LLM Model Endpoint")

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
PAGE_MARGIN = 10     # Pages before and after TOC boundaries to include (handles TOC inaccuracies)

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   Volume Path: {volume_path}")
print(f"   LLM Endpoint: {model_endpoint}")
print(f"   TOC Page Limit: {TOC_PAGE_LIMIT} pages")
print(f"   Page Margin: ±{PAGE_MARGIN} pages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../../src")

from pyspark.sql import functions as F
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Initialize
w = WorkspaceClient()

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

from src.sql.functions.document_renderer import render_ai_parse_output_interactive

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
        specifications STRING COMMENT 'Official measure description from NCQA',
        measure STRING NOT NULL COMMENT 'Official measure name with standard acronym',
        initial_pop STRING COMMENT 'Initial identification of measure population - raw text',
        denominator ARRAY<STRING> COMMENT 'Denominator definition - array of raw text from elements',
        numerator ARRAY<STRING> COMMENT 'Numerator definition - array of raw text from elements',
        exclusion ARRAY<STRING> COMMENT 'Exclusion criteria - array of raw text from elements',
        effective_year INT NOT NULL,
        page_start INT COMMENT 'TOC start page',
        page_end INT COMMENT 'TOC end page',
        page_start_actual INT COMMENT 'Actual start page with margin',
        page_end_actual INT COMMENT 'Actual end page with margin',
        extraction_timestamp TIMESTAMP,
        extracted_json STRING COMMENT 'Raw JSON response from ai_query',
        source_text_preview STRING COMMENT 'Preview of source text (first 5000 chars)'
    )
    USING DELTA
    COMMENT 'Silver layer: HEDIS measure definitions extracted with ai_query - text from elements as-is'
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
# MAGIC
# MAGIC **OPTIMIZATION APPLIED**: This section has been rewritten to eliminate DataFrame → collect() → DataFrame pattern
# MAGIC - Parsing stays distributed via SQL
# MAGIC - Elements extraction uses explode() in SQL (distributed operation)
# MAGIC - Results written to temp tables, not collected to driver

# COMMAND ----------

if file_count > 0:
    print(f"🔍 Parsing {file_count} document(s) with ai_parse_document...")
    print(f"⚡ OPTIMIZED: Processing stays fully distributed - no collect() operations\n")
    
    # ✅ OPTIMIZATION 1: Use SQL with UNION ALL instead of Python loop + collect()
    # Build a single SQL statement that processes all files at once
    files_list = files_to_process.select("file_id", "file_name", "file_path", "effective_year").collect()
    
    # Create UNION ALL query for all files - processes distributedly
    union_queries = []
    for file_row in files_list:
        file_id_escaped = file_row.file_id.replace("'", "''")
        file_name_escaped = file_row.file_name.replace("'", "''")
        file_path_escaped = file_row.file_path.replace("'", "''")
        
        union_queries.append(f"""
            SELECT
                '{file_id_escaped}' AS file_id,
                '{file_name_escaped}' AS file_name,
                CAST({file_row.effective_year} AS INT) AS effective_year,
                ai_parse_document(
                    content,
                    map(
                        'imageOutputPath', '{IMAGE_OUTPUT_PATH}',
                        'descriptionElementTypes', '*'
                    )
                ) AS parsed
            FROM READ_FILES(
                '{file_path_escaped}',
                format => 'binaryFile'
            )
        """)
    
    # Combine all queries with UNION ALL for distributed processing
    combined_sql = " UNION ALL ".join(union_queries)
    
    print(f"   Processing {len(union_queries)} file(s) in parallel...")
    
    # ✅ Execute as single distributed operation - NO collect()
    parsed_docs_df = spark.sql(combined_sql)
    
    # Extract full text and error status - still distributed
    parsed_docs_df = parsed_docs_df.withColumn(
        "full_text",
        F.expr("""
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
        F.expr("try_cast(parsed:error_status AS STRING)")
    )
    
    # ✅ Write to temp table instead of collecting - stays distributed
    parsed_docs_df.createOrReplaceTempView("parsed_documents")
    
    doc_count = parsed_docs_df.count()
    print(f"   ✅ Parsed {doc_count} document(s) successfully")
    print(f"   ✅ Created 'parsed_documents' temp view")
    print(f"   Available columns: file_id, file_name, effective_year, parsed, full_text, error_status")

    # Display summary
    display(parsed_docs_df.select("file_id", "file_name", "effective_year", "error_status"))

    # ✅ OPTIMIZATION 2: Extract elements with explode() in SQL - fully distributed
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
            WHERE try_cast(el:content AS STRING) IS NOT NULL
        )
        SELECT
            file_id,
            file_name,
            effective_year,
            element_type,
            element_content,
            page_index_0_based + 1 AS page_number,
            -- Flag for page headers and footers for special handling
            CASE
                WHEN element_type IN ('page_header', 'page_footer') THEN true
                ELSE false
            END AS is_page_metadata
        FROM elements
    """)

    # ✅ Write to temp table - stays distributed
    elements_df.createOrReplaceTempView("elements")
    element_count = elements_df.count()

    print(f"   ✅ Created 'elements' temp view with {element_count:,} elements")
    print(f"   Available columns: file_id, file_name, effective_year, element_type, element_content, page_number")

    # Display element summary
    display(elements_df.groupBy("file_name", "element_type").count().orderBy("file_name", "element_type"))
    
else:
    print("⚠️  No files to process")

# COMMAND ----------

# Verify elements temp view exists
if file_count > 0:
    if spark.catalog.tableExists("elements"):

        print("🔍 Extracting Table of Contents...")
        print("⚡ OPTIMIZED: TOC extraction fully distributed via SQL\n")

        # ✅ OPTIMIZATION 3: Extract TOC in SQL, then process with ai_query distributedly
        # First aggregate TOC text per document - distributed operation
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

        total_docs = toc_input_df.count()
        print(f"📊 Found {total_docs} document(s) to process")
        print(f"   Using first {TOC_PAGE_LIMIT} pages per document\n")
        
        # ✅ OPTIMIZATION 4: Process TOC extraction with ai_query in DISTRIBUTED manner
        # Use DataFrame operations with ai_query instead of Python loop with collect()
        
        print("   Calling ai_query for TOC extraction in parallel...")
        
        toc_extraction_df = toc_input_df.selectExpr(
            "file_id",
            "file_name", 
            "effective_year",
            "toc_text",
            f"""
            ai_query(
                '{model_endpoint}',
                concat(
                    'Extract ALL measures from the Table of Contents in this HEDIS document. ',
                    'Find the "Table of Contents" section and extract EVERY SINGLE measure listed. You know these are measures as they will have a acroynym in parantheses next to both the title and page number. It is very important we map the measure to the correct start page number, these should be unique to the measure. Do not add, remove, or edit text, simply extract. Accuracy is VERY important, wrong start_page assignments can negatively impact care.',
                    'For each measure entry on a single line of the TOC, extract: ',
                    '1. measure_acronym: The acronym in parentheses (e.g., "BCS-E", "ADD-E", "WCC") ',
                    '2. measure_name: The full name of the measure ',
                    '3. start_page: The number at the end of the TOC line (the number after the dots) on the SAME line. Example: "Glycemic Status Assessment for Patients With Diabetes (GSD) ..................................................... 134" would return 134 for the start_page\\n',
                    'CRITICAL: Extract ALL measures from the entire TOC, not just the first few. The table of contents may span many pages.',
                    'The start_page is always the rightmost number on each TOC line. ',
                    'Return a complete list of ALL measures found in the TOC. ',
                    'Please use tree-of-thought reasoning to complete this task. Be confident with the extractions.',
                    '\\n\\nDocument:\\n',
                    toc_text
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
            """
        )
        
        # ✅ Parse JSON and explode measures - all distributed operations
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
        from pyspark.sql.functions import from_json, explode, lead, col
        from pyspark.sql.window import Window
        
        # Define schema for JSON parsing
        measures_schema = ArrayType(StructType([
            StructField("measure_acronym", StringType(), True),
            StructField("measure_name", StringType(), True),
            StructField("start_page", IntegerType(), True)
        ]))
        
        # Parse JSON and explode to rows - distributed
        toc_df = toc_extraction_df.withColumn(
            "parsed_json",
            from_json(F.expr("toc_response"), StructType([
                StructField("measures", measures_schema, True)
            ]))
        ).withColumn(
            "measure",
            explode("parsed_json.measures")
        ).select(
            "file_id",
            "file_name",
            "effective_year",
            col("measure.measure_acronym").alias("measure_acronym"),
            col("measure.measure_name").alias("measure_name"),
            col("measure.start_page").alias("start_page")
        )
        
        # Calculate end_page - distributed window function
        window_spec = Window.partitionBy("file_id", "file_name").orderBy("start_page")
        toc_df = toc_df.withColumn(
            "end_page",
            lead(col("start_page"), 1).over(window_spec) - 1
        )
        
        # ✅ Write to temp table - stays distributed
        toc_df.createOrReplaceTempView("toc_entries")
        toc_count = toc_df.count()
        
        print(f"   ✅ Extracted {toc_count} measure entries from {total_docs} document(s)")
        print(f"   ✅ Created 'toc_entries' temp view")
        print(f"   Calculated end_page as (next start_page - 1)\n")
        
        if toc_count > 0:
            display(toc_df)
        else:
            raise ValueError("No TOC entries extracted")
    else:
        raise ValueError("'elements' temp view not found. Please run previous cells first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Extract Measure Definitions as Raw Text
# MAGIC
# MAGIC This step extracts HEDIS measure definitions as raw text from elements:
# MAGIC - measure: Official measure name with standard acronym
# MAGIC - initial_pop: Initial measure population identification (raw text)
# MAGIC - denominator: Denominator definition (raw text from elements)
# MAGIC - numerator: Numerator definition (raw text from elements)
# MAGIC - exclusion: Exclusion criteria (raw text from elements)
# MAGIC - effective_year: Year measure is effective
# MAGIC
# MAGIC **Page Margin**: Extracts elements from ±10 pages around TOC boundaries to handle TOC inaccuracies
# MAGIC
# MAGIC **Element Handling**: Includes page headers and footers which provide measure context
# MAGIC
# MAGIC **OPTIMIZATION APPLIED**: This entire section now runs as distributed SQL - no collect() operations

# COMMAND ----------

if file_count > 0 and toc_count > 0:
    print("🤖 Running comprehensive AI extraction with ai_query...")
    print(f"⚡ OPTIMIZED: Processing {toc_count} measures fully distributed via SQL")
    print(f"   Using ±{PAGE_MARGIN} page margin for TOC inaccuracies\n")

    # ✅ OPTIMIZATION 5: Entire extraction pipeline in SQL - fully distributed
    # No collect(), no Python loops - pure distributed Spark operations
    
    print(f"🚀 Processing {toc_count} measures with parallel AI extraction...")
    
    from pyspark.sql.functions import expr, col, from_json
    from pyspark.sql.types import ArrayType, TimestampType
    
    # Single massive SQL query that does everything distributedly
    extraction_sql = f"""
    WITH measure_elements AS (
        -- Join TOC entries with elements by page range (distributed join)
        SELECT
            t.file_id,
            t.file_name,
            t.measure_acronym,
            t.measure_name,
            t.start_page AS page_start,
            t.end_page AS page_end,
            -- Calculate actual page range with margin
            GREATEST(1, t.start_page - {PAGE_MARGIN}) AS page_start_actual,
            COALESCE(t.end_page, t.start_page + 15) + {PAGE_MARGIN} AS page_end_actual,
            t.effective_year,
            e.element_type,
            e.element_content,
            e.page_number,
            e.is_page_metadata
        FROM toc_entries t
        INNER JOIN elements e
            ON t.file_id = e.file_id
            AND e.page_number >= GREATEST(1, t.start_page - {PAGE_MARGIN})
            AND e.page_number <= COALESCE(t.end_page, t.start_page + 15) + {PAGE_MARGIN}
            AND e.element_content IS NOT NULL
    ),
    structured_content AS (
        -- Aggregate elements per measure (distributed aggregation)
        SELECT
            file_id,
            file_name,
            measure_acronym,
            measure_name,
            page_start,
            page_end,
            page_start_actual,
            page_end_actual,
            effective_year,
            concat_ws('\\n\\n',
                concat('=== PAGE HEADERS AND FOOTERS ===\\n',
                    concat_ws('\\n',
                        collect_list(
                            CASE WHEN is_page_metadata THEN
                                concat('[', element_type, ' - Page ', page_number, '] ', element_content)
                            END
                        )
                    )
                ),
                '\\n\\n=== MEASURE CONTENT ===\\n',
                concat_ws('\\n\\n',
                    collect_list(
                        CASE WHEN NOT is_page_metadata THEN
                            concat('[Page ', page_number, '] ', element_content)
                        END
                    )
                )
            ) AS full_text,
            count(*) AS element_count,
            count(CASE WHEN is_page_metadata THEN 1 END) AS metadata_count
        FROM measure_elements
        GROUP BY file_id, file_name, measure_acronym, measure_name,
                 page_start, page_end, page_start_actual, page_end_actual, effective_year
    )
    -- Extract definitions with ai_query (distributed across all measures)
    SELECT
        uuid() as measure_id,
        file_id,
        file_name,
        measure_acronym,
        measure_name AS measure_name_from_toc,
        page_start,
        page_end,
        page_start_actual,
        page_end_actual,
        effective_year,
        current_timestamp() as extraction_timestamp,
        full_text,
        element_count,
        metadata_count,
        substring(full_text, 1, 5000) as source_text_preview,
        ai_query(
            '{model_endpoint}',
            concat(
                'You are extracting HEDIS measure definitions from official NCQA documentation. ',
                'Extract the complete raw text content exactly as it appears in the document. ',
                'Page headers and footers contain important context about the measure name and acronym.\\n\\n',
                'Extract the following fields:\\n',
                '1. Specifications: The official measure description/specification text from NCQA\\n',
                '2. measure: The official measure name with standard acronym (e.g., "LSC - Lead Screening in Children")\\n',
                '3. Initial_Pop: Extract the complete initial population definition text exactly as written\\n',
                '4. denominator: Extract ALL denominator text - if there are multiple components or bullet points, include them ALL as separate array items\\n',
                '5. numerator: Extract ALL numerator text - if there are multiple requirements or bullet points, include them ALL as separate array items\\n',
                '6. exclusion: Extract ALL exclusion criteria - if there are multiple conditions, include them ALL as separate array items\\n',
                '7. effective_year: The year this measure is effective (extract from document or use ', effective_year, ')\\n\\n',
                'CRITICAL INSTRUCTIONS:\\n',
                '- Extract the COMPLETE, EXACT text from the policy document - do NOT summarize\\n',
                '- For denominator, numerator, and exclusion: if the document has bullet points or multiple items, return them as separate array entries\\n',
                '- If a section like "Administrative Specification" or "Hybrid Specification" has denominator/numerator details, extract ALL of that text\\n',
                '- Maintain accuracy - wrong information impacts patient care\\n',
                '- Use page headers/footers to verify you are extracting the correct measure\\n',
                '- If a section is not found, return empty string or empty array\\n\\n',
                'Document content:\\n\\n',
                full_text
            ),
            responseFormat => '{{
                "type": "json_schema",
                "json_schema": {{
                    "name": "hedis_measure_definition",
                    "strict": true,
                    "schema": {{
                        "type": "object",
                        "properties": {{
                            "Specifications": {{"type": "string"}},
                            "measure": {{"type": "string"}},
                            "Initial_Pop": {{"type": "string"}},
                            "denominator": {{
                                "type": "array",
                                "items": {{"type": "string"}}
                            }},
                            "numerator": {{
                                "type": "array",
                                "items": {{"type": "string"}}
                            }},
                            "exclusion": {{
                                "type": "array",
                                "items": {{"type": "string"}}
                            }},
                            "effective_year": {{"type": "integer"}}
                        }},
                        "required": ["Specifications", "measure", "Initial_Pop", "denominator", "numerator", "exclusion", "effective_year"],
                        "additionalProperties": false
                    }}
                }}
            }}'
        ) AS extracted_json
    FROM structured_content
    WHERE length(full_text) > 100
    """
    
    # ✅ Execute entire extraction pipeline as distributed SQL
    measures_with_extraction_df = spark.sql(extraction_sql)
    
    # ✅ Parse JSON and extract fields - distributed operations
    json_schema = StructType([
        StructField("Specifications", StringType(), True),
        StructField("measure", StringType(), True),
        StructField("Initial_Pop", StringType(), True),
        StructField("denominator", ArrayType(StringType()), True),
        StructField("numerator", ArrayType(StringType()), True),
        StructField("exclusion", ArrayType(StringType()), True),
        StructField("effective_year", IntegerType(), True)
    ])
    
    final_df = measures_with_extraction_df.withColumn("parsed", from_json(col("extracted_json"), json_schema)) \
        .select(
            "measure_id",
            "file_id",
            "file_name",
            "measure_acronym",
            col("parsed.Specifications").alias("specifications"),
            col("parsed.measure").alias("measure"),
            col("parsed.Initial_Pop").alias("initial_pop"),
            col("parsed.denominator").alias("denominator"),
            col("parsed.numerator").alias("numerator"),
            col("parsed.exclusion").alias("exclusion"),
            col("parsed.effective_year").alias("effective_year"),
            "page_start",
            "page_end",
            "page_start_actual",
            "page_end_actual",
            "extraction_timestamp",
            "extracted_json",
            "source_text_preview"
        )
    
    measure_count = final_df.count()
    
    print(f"\n{'='*80}")
    print(f"✅ Extraction Summary:")
    print(f"   Total measures extracted: {measure_count}/{toc_count}")
    print(f"   Files processed: {file_count}")
    print(f"   ⚡ OPTIMIZED: Zero collect() operations - fully distributed processing")
    print(f"{'='*80}\n")
    
    # ✅ Create temp view - stays distributed
    final_df.createOrReplaceTempView("extracted_measures")
    
    # Display extracted measures
    print("Extracted measures:")
    display(final_df)
    
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
    print("💾 Writing measures to silver table...")

    spark.sql(f"""
        MERGE INTO {silver_table} target
        USING extracted_measures source
        ON target.file_id = source.file_id
           AND target.measure_acronym = source.measure_acronym
           AND target.page_start = source.page_start
        WHEN MATCHED THEN
            UPDATE SET
                target.measure_id = source.measure_id,
                target.file_name = source.file_name,
                target.specifications = source.specifications,
                target.measure = source.measure,
                target.initial_pop = source.initial_pop,
                target.denominator = source.denominator,
                target.numerator = source.numerator,
                target.exclusion = source.exclusion,
                target.effective_year = source.effective_year,
                target.page_end = source.page_end,
                target.page_start_actual = source.page_start_actual,
                target.page_end_actual = source.page_end_actual,
                target.extraction_timestamp = source.extraction_timestamp,
                target.extracted_json = source.extracted_json,
                target.source_text_preview = source.source_text_preview
        WHEN NOT MATCHED THEN
            INSERT (
                measure_id,
                file_id,
                file_name,
                measure_acronym,
                specifications,
                measure,
                initial_pop,
                denominator,
                numerator,
                exclusion,
                effective_year,
                page_start,
                page_end,
                page_start_actual,
                page_end_actual,
                extraction_timestamp,
                extracted_json,
                source_text_preview
            )
            VALUES (
                source.measure_id,
                source.file_id,
                source.file_name,
                source.measure_acronym,
                source.specifications,
                source.measure,
                source.initial_pop,
                source.denominator,
                source.numerator,
                source.exclusion,
                source.effective_year,
                source.page_start,
                source.page_end,
                source.page_start_actual,
                source.page_end_actual,
                source.extraction_timestamp,
                source.extracted_json,
                source.source_text_preview
            )
    """)

    result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").first()["cnt"]
    print(f"✅ Wrote {measure_count} measures to silver table (MERGE)")
    print(f"   Total measures in table: {result_count}")

    # Display sample
    print("\nSample measures:")
    display(spark.table(silver_table)
        .limit(10))

    # Display detailed view of one measure with arrays
    print("\nDetailed view of first measure:")
    display(spark.table(silver_table)
        .select(
            "measure_acronym",
            "measure",
            "specifications",
            "initial_pop",
            "denominator",
            "numerator",
            "exclusion"
        )
        .orderBy(F.desc("extraction_timestamp"))
        .limit(1))
else:
    print("⚠️  No measures extracted")