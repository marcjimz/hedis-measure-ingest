# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Chunks Processing
# MAGIC
# MAGIC This notebook chunks HEDIS documents for vector search using `ai_parse_document` for structure-aware chunking.
# MAGIC
# MAGIC **Module**: Silver Chunks for Search (Step 3 of 3)
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Bronze table: `{catalog}.{schema}.hedis_file_metadata` (status='completed')
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Silver table: `{catalog}.{schema}.hedis_measures_chunks`
# MAGIC
# MAGIC **Features**:
# MAGIC - AI-powered PDF parsing with `ai_parse_document` SQL function
# MAGIC - Header-aware chunking with configurable overlap
# MAGIC - Page headers and footers preserved in each chunk
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
dbutils.widgets.text("schema_name", "hedis_measurements", "Schema Name")
dbutils.widgets.text("volume_name", "hedis", "Volume Name")
dbutils.widgets.text("chunk_size", "1024", "Chunk Size (tokens)")
dbutils.widgets.text("overlap_percent", "0.15", "Overlap Percent")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
chunk_size = int(dbutils.widgets.get("chunk_size"))
overlap_percent = float(dbutils.widgets.get("overlap_percent"))

# Table names
bronze_table = f"{catalog_name}.{schema_name}.hedis_file_metadata"
silver_table = f"{catalog_name}.{schema_name}.hedis_measures_chunks"
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
IMAGE_OUTPUT_PATH = f"{volume_path}/images"

print(f"📋 Configuration:")
print(f"   Bronze Table: {bronze_table}")
print(f"   Silver Table: {silver_table}")
print(f"   Volume Path: {volume_path}")
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

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print("✅ Environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI-Powered Chunking with `ai_parse_document`
# MAGIC
# MAGIC Chunking leverages `ai_parse_document` to preserve document structure. The function identifies headers and sections,
# MAGIC which enables semantic chunking that respects HEDIS measure boundaries.
# MAGIC
# MAGIC **How it helps chunking:**
# MAGIC - Element classification preserves header hierarchy (H1 > H2 > H3)
# MAGIC - Bounding boxes help identify page breaks and column layouts
# MAGIC - Table detection ensures code value sets aren't split across chunks
# MAGIC - Page headers and footers provide measure context for each chunk
# MAGIC
# MAGIC This notebook uses SQL-based processing similar to notebook 2 for consistency and performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Chunks Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        chunk_id STRING NOT NULL,
        file_id STRING NOT NULL,
        measure_name STRING,
        header STRING COMMENT 'Page header text for context',
        footer STRING COMMENT 'Page footer text for context',
        page_content STRING COMMENT 'Main page content',
        chunk_content STRING NOT NULL COMMENT 'Combined header + footer + page_content for embedding',
        chunk_sequence INT NOT NULL,
        token_count INT,
        page_start INT,
        page_end INT,
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
# MAGIC ## Select Files to Process

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
# MAGIC ## Parse Documents with AI
# MAGIC
# MAGIC Using `ai_parse_document` SQL function to extract structured elements from PDFs.
# MAGIC This step uses the same proven approach from notebook 02.

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

            # Parse the document - SAME SYNTAX AS NOTEBOOK 02
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

            # Process ALL results from the parse (defensive - typically 1 per file)
            if parsed_result:
                for result in parsed_result:
                    all_parsed_docs.append({
                        'file_id': result.file_id,
                        'file_name': result.file_name,
                        'effective_year': result.effective_year,
                        'parsed': result.parsed
                    })
                print(f"   ✅ Parsed successfully ({len(parsed_result)} result(s))")

        except Exception as e:
            print(f"   ❌ Failed to parse: {str(e)}")
            continue

    print(f"\n📊 Successfully parsed {len(all_parsed_docs)} document(s)")

    # Create DataFrame from parsed documents - SAME AS NOTEBOOK 02
    if all_parsed_docs:
        # Create DataFrame with parsed content
        parsed_docs_df = spark.createDataFrame(all_parsed_docs)

        # Register as temp view for SQL access
        parsed_docs_df.createOrReplaceTempView("parsed_documents")

        print(f"✅ Created 'parsed_documents' temp view with {parsed_docs_df.count()} document(s)")
        print(f"   Available columns: file_id, file_name, effective_year, parsed")

        # Display summary
        display(parsed_docs_df.select("file_id", "file_name", "effective_year"))
    else:
        print("⚠️  No documents successfully parsed")
else:
    print("⚠️  No files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Elements with Page Numbers
# MAGIC
# MAGIC Extract all elements from parsed documents with proper page numbering and metadata flags.
# MAGIC This uses the SAME LOGIC as notebook 02.

# COMMAND ----------

if file_count > 0 and len(all_parsed_docs) > 0:
    print("🔍 Extracting elements with page numbers...")

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

    # Register as temp view
    elements_df.createOrReplaceTempView("elements")
    element_count = elements_df.count()

    print(f"✅ Created 'elements' temp view with {element_count:,} elements")
    print(f"   Available columns: file_id, file_name, effective_year, element_type, element_content, page_number, is_page_metadata")

    # Display element summary
    display(elements_df.groupBy("file_name", "element_type").count().orderBy("file_name", "element_type"))
else:
    print("⚠️  No documents to extract elements from")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Page-Level Content with Headers and Footers
# MAGIC
# MAGIC Group elements by page and separate headers, footers, and content.

# COMMAND ----------

if file_count > 0 and element_count > 0:
    print("📄 Creating page-level content with headers and footers...")

    page_content_df = spark.sql("""
        SELECT
            file_id,
            file_name,
            effective_year,
            page_number,
            concat_ws('\\n',
                collect_list(
                    CASE WHEN is_page_metadata AND element_type = 'page_header'
                    THEN element_content END
                )
            ) AS header,
            concat_ws('\\n',
                collect_list(
                    CASE WHEN is_page_metadata AND element_type = 'page_footer'
                    THEN element_content END
                )
            ) AS footer,
            concat_ws('\\n\\n',
                collect_list(
                    CASE WHEN NOT is_page_metadata
                    THEN element_content END
                )
            ) AS page_content
        FROM elements
        GROUP BY file_id, file_name, effective_year, page_number
        ORDER BY file_id, page_number
    """)

    page_content_df.createOrReplaceTempView("page_content")

    page_count = page_content_df.count()
    print(f"✅ Created page-level content for {page_count:,} pages")
    print(f"   Sample:")
    display(page_content_df.limit(5))
else:
    print("⚠️  No elements to create page content from")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunk Content with Overlap
# MAGIC
# MAGIC Create overlapping chunks with configurable size and overlap percentage.
# MAGIC Each chunk includes header, footer, and page content combined.

# COMMAND ----------

if file_count > 0 and page_count > 0:
    from pyspark.sql.window import Window

    # Calculate chunk boundaries with overlap
    # Token approximation: 1 token ≈ 4 characters
    chars_per_chunk = chunk_size * 4
    overlap_chars = int(chars_per_chunk * overlap_percent)

    print(f"📦 Chunking with:")
    print(f"   Target tokens per chunk: {chunk_size}")
    print(f"   Target chars per chunk: {chars_per_chunk}")
    print(f"   Overlap: {overlap_percent * 100}% ({overlap_chars} chars)")

    # Generate chunks using SQL
    chunks_sql = f"""
        WITH page_text AS (
            SELECT
                file_id,
                file_name,
                effective_year,
                page_number,
                header,
                footer,
                page_content,
                concat_ws('\\n\\n',
                    CASE WHEN length(header) > 0 THEN concat('=== HEADER ===\\n', header) END,
                    CASE WHEN length(page_content) > 0 THEN page_content END,
                    CASE WHEN length(footer) > 0 THEN concat('=== FOOTER ===\\n', footer) END
                ) AS combined_content,
                length(concat_ws('\\n\\n', header, page_content, footer)) AS content_length
            FROM page_content
            WHERE length(page_content) > 50
        ),
        page_positions AS (
            SELECT
                *,
                sum(content_length) OVER (
                    PARTITION BY file_id
                    ORDER BY page_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_length
            FROM page_text
        ),
        chunk_boundaries AS (
            SELECT
                file_id,
                file_name,
                effective_year,
                page_number,
                header,
                footer,
                page_content,
                combined_content,
                content_length,
                cumulative_length,
                floor((cumulative_length - content_length) / ({chars_per_chunk} - {overlap_chars})) AS chunk_id
            FROM page_positions
        ),
        grouped_chunks AS (
            SELECT
                file_id,
                file_name,
                effective_year,
                chunk_id,
                min(page_number) AS page_start,
                max(page_number) AS page_end,
                concat_ws('\\n', collect_list(DISTINCT header)) AS header,
                concat_ws('\\n', collect_list(DISTINCT footer)) AS footer,
                concat_ws('\\n\\n', collect_list(page_content)) AS page_content,
                concat_ws('\\n\\n', collect_list(combined_content)) AS chunk_content,
                sum(content_length) AS total_chars,
                cast(sum(content_length) / 4 AS INT) AS token_count
            FROM chunk_boundaries
            GROUP BY file_id, file_name, effective_year, chunk_id
            HAVING sum(content_length) > 100
        )
        SELECT
            concat(file_id, '_', cast(row_number() OVER (PARTITION BY file_id ORDER BY chunk_id) AS INT)) AS chunk_id,
            file_id,
            file_name,
            effective_year,
            cast(row_number() OVER (PARTITION BY file_id ORDER BY chunk_id) AS INT) AS chunk_sequence,
            page_start,
            page_end,
            header,
            footer,
            page_content,
            chunk_content,
            total_chars,
            token_count
        FROM grouped_chunks
        ORDER BY file_id, chunk_sequence
    """

    chunks_df = spark.sql(chunks_sql)

    chunk_count = chunks_df.count()
    print(f"\n✅ Generated {chunk_count:,} chunks")

    if chunk_count > 0:
        stats = chunks_df.select(
            F.avg("token_count").alias("avg_tokens"),
            F.min("token_count").alias("min_tokens"),
            F.max("token_count").alias("max_tokens")
        ).first()

        print(f"   Average tokens: {stats.avg_tokens:.0f}")
        print(f"   Min tokens: {stats.min_tokens}")
        print(f"   Max tokens: {stats.max_tokens}")

        # Display sample chunks
        print(f"\n📋 Sample chunks:")
        display(chunks_df.limit(5))
    else:
        print("⚠️  No chunks generated")
else:
    chunk_count = 0
    print("⚠️  No pages to chunk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Chunks Table (Idempotent)

# COMMAND ----------

if chunk_count > 0:
    print("💾 Writing chunks to silver table...")

    # Add timestamp and metadata
    final_chunks_df = chunks_df.withColumn(
        "chunk_timestamp", F.current_timestamp()
    ).withColumn(
        "metadata",
        F.to_json(F.struct(
            F.col("file_name"),
            F.col("effective_year"),
            F.lit("header_aware_overlap").alias("chunk_strategy")
        ))
    ).withColumn(
        "measure_name", F.lit(None).cast("string")  # Placeholder - can be populated from TOC if needed
    )

    # Get unique file_ids being processed
    file_ids_processed = [row.file_id for row in chunks_df.select("file_id").distinct().collect()]

    # DELETE existing chunks for these files (idempotent reprocessing)
    if file_ids_processed:
        file_ids_str = "', '".join(file_ids_processed)
        delete_count = spark.sql(f"""
            DELETE FROM {silver_table}
            WHERE file_id IN ('{file_ids_str}')
        """)
        print(f"   🗑️  Removed existing chunks for {len(file_ids_processed)} files")

    # INSERT new chunks
    final_chunks_df.write.mode("append").saveAsTable(silver_table)

    result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").first()["cnt"]
    print(f"✅ Wrote {chunk_count:,} chunks to silver table (DELETE+INSERT)")
    print(f"   Total chunks in table: {result_count:,}")

    # Display sample
    print(f"\n📋 Sample chunks from table:")
    display(spark.table(silver_table).orderBy(F.desc("chunk_timestamp")).limit(10))
else:
    print("⚠️  No chunks generated - skipping write")

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
        SUM(token_count) as total_tokens,
        MIN(token_count) as min_tokens,
        MAX(token_count) as max_tokens
    FROM {silver_table}
    GROUP BY effective_year
    ORDER BY effective_year DESC
""")

print("📊 Silver Chunks Summary:")
display(summary)

print(f"\n✅ Chunks are ready for vector search delta sync!")
print(f"   Next step: Configure Databricks Vector Search to sync from {silver_table}")
print(f"   Column to embed: chunk_content (contains header + footer + page_content)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search Setup Instructions
# MAGIC
# MAGIC To create a vector search endpoint and index:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC
# MAGIC vsc = VectorSearchClient()
# MAGIC
# MAGIC # Create endpoint (if not exists)
# MAGIC vsc.create_endpoint(
# MAGIC     name="hedis_vector_search_endpoint",
# MAGIC     endpoint_type="STANDARD"
# MAGIC )
# MAGIC
# MAGIC # Create delta sync index
# MAGIC vsc.create_delta_sync_index(
# MAGIC     endpoint_name="hedis_vector_search_endpoint",
# MAGIC     source_table_name="{catalog_name}.{schema_name}.hedis_measures_chunks",
# MAGIC     index_name="{catalog_name}.{schema_name}.hedis_chunks_index",
# MAGIC     pipeline_type="TRIGGERED",
# MAGIC     primary_key="chunk_id",
# MAGIC     embedding_source_column="chunk_content",  # This column contains header + footer + page_content
# MAGIC     embedding_model_endpoint_name="databricks-gte-large-en"  # Or your preferred model
# MAGIC )
# MAGIC ```
