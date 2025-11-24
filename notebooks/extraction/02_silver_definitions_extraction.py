# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Definitions Extraction
# MAGIC
# MAGIC This notebook extracts structured HEDIS measure definitions from PDFs using PyMuPDF and `ai_query` SQL functions.
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
# MAGIC - PDF parsing with PyMuPDF (fitz) for text extraction
# MAGIC - Table of Contents parsing for measure boundaries
# MAGIC - SQL-based structured extraction with `ai_query`
# MAGIC - Idempotent writes with MERGE
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

import yaml

# Load configuration from config.yaml
with open("../config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Create widgets with config values as defaults
dbutils.widgets.text("catalog_name", config.get("catalog_name", "main"), "Catalog Name")
dbutils.widgets.text("schema_name", config.get("schema_name", "hedis_measurements"), "Schema Name")
dbutils.widgets.text("volume_name", config.get("volume_name", "hedis"), "Volume Name")
dbutils.widgets.text("llm_endpoint", config.get("llm_endpoint", "databricks-claude-opus-4-1"), "LLM Model Endpoint")

# Get parameters (widgets override config if changed)
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
model_endpoint = dbutils.widgets.get("llm_endpoint")

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
# MAGIC ## PDF Parsing with PyMuPDF
# MAGIC
# MAGIC This pipeline uses PyMuPDF (fitz) for PDF text extraction. The library extracts text blocks with positional
# MAGIC information, allowing classification of elements as headers, footers, or body text based on their position on the page.
# MAGIC
# MAGIC **Element Classification:**
# MAGIC - **page_header**: Text in the top 8% of the page
# MAGIC - **page_footer**: Text in the bottom 8% of the page
# MAGIC - **text**: Body text content
# MAGIC
# MAGIC The parser is implemented in `src/extraction/pdfparser.py` and follows patterns from the dbx-hls-vector-search example.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize PDF Parser

# COMMAND ----------

from src.extraction.pdfparser import PDFParser

# Initialize the PDF parser
pdfparser = PDFParser(
    header_threshold_pct=0.08,  # Top 8% of page is header zone
    footer_threshold_pct=0.92,  # Bottom 8% of page is footer zone
    line_grouping_threshold=5.0  # Pixel threshold for grouping words into lines
)

print("✅ PDFParser initialized from src.extraction.pdfparser")

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
    -- WHERE s.file_id IS NULL
    ORDER BY b.ingestion_timestamp DESC
""")

file_count = files_to_process.count()
print(f"📁 Found {file_count} files to process")

if file_count > 0:
    display(files_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Parse Documents with PyMuPDF and Extract Elements
# MAGIC
# MAGIC Parse all PDF documents using PyMuPDF to extract text elements with positional classification.

# COMMAND ----------

if file_count > 0:
    print(f"🔍 Parsing {file_count} document(s) with PyMuPDF...")

    # Collect file list (small operation)
    files_list = files_to_process.select("file_id", "file_name", "file_path", "effective_year").collect()

    # Parse all documents using pdfparser.document_parser()
    all_elements = []

    from tqdm import tqdm
    for file_row in tqdm(files_list, desc="Parsing documents"):
        try:
            print(f"\n📄 Parsing: {file_row.file_name}")

            # Parse PDF with pdfparser.document_parser()
            elements = pdfparser.document_parser(
                file_path=file_row.file_path,
                file_id=file_row.file_id,
                file_name=file_row.file_name,
                effective_year=file_row.effective_year
            )

            all_elements.extend(elements)
            print(f"   ✅ Extracted {len(elements)} elements")

        except Exception as e:
            print(f"   ❌ Failed to parse: {str(e)}")
            raise e

    print(f"\n📊 Successfully parsed {len(files_list)} document(s)")
    print(f"   Total elements extracted: {len(all_elements):,}")

    # Create DataFrame from elements
    if all_elements:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

        elements_schema = StructType([
            StructField("file_id", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("effective_year", IntegerType(), False),
            StructField("element_type", StringType(), False),
            StructField("element_content", StringType(), True),
            StructField("page_number", IntegerType(), False),
            StructField("is_page_metadata", BooleanType(), False)
        ])

        elements_df = spark.createDataFrame(all_elements, schema=elements_schema)

        # Register as temp view
        elements_df.createOrReplaceTempView("elements")
        element_count = elements_df.count()

        print(f"✅ Created 'elements' temp view with {element_count:,} elements")
        print(f"   Available columns: file_id, file_name, effective_year, element_type, element_content, page_number, is_page_metadata")

        # Display element summary
        display(elements_df.groupBy("file_name", "element_type").count().orderBy("file_name", "element_type"))
    else:
        print("⚠️  No elements extracted from documents")
else:
    print("⚠️  No files to process")

# COMMAND ----------

# Verify elements temp view exists
if file_count > 0:
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

# COMMAND ----------

if file_count > 0 and toc_count > 0:
    print("🤖 Running comprehensive AI extraction with ai_query...")
    print(f"   Processing {toc_count} measures across {file_count} files")
    print(f"   Using ±{PAGE_MARGIN} page margin for TOC inaccuracies\n")

    toc_entries_list = spark.sql("SELECT * FROM toc_entries ORDER BY file_id, start_page").collect()
    
    # Create a list to store all measure queries
    all_measure_queries = []
    
    for idx, toc_entry in enumerate(toc_entries_list, 1):
        # Calculate actual page range with margin
        page_start_actual = max(1, toc_entry.start_page - PAGE_MARGIN)
        page_end_actual = (toc_entry.end_page + PAGE_MARGIN) if toc_entry.end_page else (toc_entry.start_page + 15 + PAGE_MARGIN)
        
        # Escape single quotes for SQL
        file_id_escaped = toc_entry.file_id.replace("'", "''")
        file_name_escaped = toc_entry.file_name.replace("'", "''")
        measure_acronym_escaped = toc_entry.measure_acronym.replace("'", "''")
        measure_name_escaped = toc_entry.measure_name.replace("'", "''")
        
        measure_info = {
            'measure_id': f"uuid()",
            'file_id': file_id_escaped,
            'file_name': file_name_escaped,
            'measure_acronym': measure_acronym_escaped,
            'measure_name_from_toc': measure_name_escaped,
            'page_start': toc_entry.start_page,
            'page_end': toc_entry.end_page if toc_entry.end_page else None,
            'page_start_actual': page_start_actual,
            'page_end_actual': page_end_actual,
            'effective_year': toc_entry.effective_year
        }
        
        all_measure_queries.append(measure_info)
    
    # Create temporary table with measure metadata
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    measures_metadata_schema = StructType([
        StructField("file_id", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("measure_acronym", StringType(), True),
        StructField("measure_name_from_toc", StringType(), True),
        StructField("page_start", IntegerType(), True),
        StructField("page_end", IntegerType(), True),
        StructField("page_start_actual", IntegerType(), False),
        StructField("page_end_actual", IntegerType(), False),
        StructField("effective_year", IntegerType(), False)
    ])
    
    # Create list of tuples for the dataframe
    measures_metadata_data = [
        (m['file_id'], m['file_name'], m['measure_acronym'], m['measure_name_from_toc'],
         m['page_start'], m['page_end'], m['page_start_actual'], m['page_end_actual'], m['effective_year'])
        for m in all_measure_queries
    ]
    
    measures_metadata_df = spark.createDataFrame(measures_metadata_data, schema=measures_metadata_schema)
    measures_metadata_df.createOrReplaceTempView("measures_metadata")
    
    print(f"🚀 Processing {len(all_measure_queries)} measures with parallel AI extraction...")
    
    # Join with elements and perform extraction in one operation
    from pyspark.sql.functions import expr, col, from_json
    from pyspark.sql.types import ArrayType, TimestampType
    
    extraction_sql = f"""
    WITH measure_elements AS (
        SELECT
            m.file_id,
            m.file_name,
            m.measure_acronym,
            m.measure_name_from_toc,
            m.page_start,
            m.page_end,
            m.page_start_actual,
            m.page_end_actual,
            m.effective_year,
            e.element_type,
            e.element_content,
            e.page_number,
            e.is_page_metadata
        FROM measures_metadata m
        INNER JOIN elements e
            ON m.file_id = e.file_id
            AND e.page_number >= m.page_start_actual
            AND e.page_number <= m.page_end_actual
            AND e.element_content IS NOT NULL
    ),
    structured_content AS (
        SELECT
            file_id,
            file_name,
            measure_acronym,
            measure_name_from_toc,
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
        GROUP BY file_id, file_name, measure_acronym, measure_name_from_toc,
                 page_start, page_end, page_start_actual, page_end_actual, effective_year
    )
    SELECT
        uuid() as measure_id,
        file_id,
        file_name,
        measure_acronym,
        measure_name_from_toc,
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
    
    measures_with_extraction_df = spark.sql(extraction_sql)
    
    # Parse JSON and extract fields
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
            col("parsed.denominator").alias("denominator"),      # Changed - no to_json()
            col("parsed.numerator").alias("numerator"),          # Changed - no to_json()
            col("parsed.exclusion").alias("exclusion"),          # Changed - no to_json()
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
    print(f"{'='*80}\n")
    
    # Create temp view
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
