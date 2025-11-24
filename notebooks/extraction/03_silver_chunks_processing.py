# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: HEDIS Measures Chunks Processing
# MAGIC
# MAGIC This notebook chunks HEDIS documents for vector search using PyMuPDF for structure-aware chunking.
# MAGIC
# MAGIC **Module**: Silver Chunks for Search (Step 3 of 3)
# MAGIC
# MAGIC **Inputs**:
# MAGIC - Silver table: `{catalog}.{schema}.hedis_definitions` (from notebook 02)
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Silver table: `{catalog}.{schema}.hedis_measures_chunks`
# MAGIC
# MAGIC **Features**:
# MAGIC - Uses pre-parsed elements from hedis_definitions table
# MAGIC - Header-aware chunking with configurable overlap
# MAGIC - Page headers and footers preserved in each chunk
# MAGIC - Ready for vector search delta sync

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
try:
    with open("../config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    # Fallback for different execution contexts
    with open("/Workspace/Repos/hedis-measure-ingest/notebooks/config.yaml", "r") as f:
        config = yaml.safe_load(f)

# Create widgets with config values as defaults
dbutils.widgets.text("catalog_name", config.get("catalog_name", "main"), "Catalog Name")
dbutils.widgets.text("schema_name", config.get("schema_name", "hedis_measurements"), "Schema Name")
dbutils.widgets.text("volume_name", config.get("volume_name", "hedis"), "Volume Name")
dbutils.widgets.text("chunk_size", config.get("chunk_size", "1024"), "Chunk Size (tokens)")
dbutils.widgets.text("overlap_percent", config.get("overlap_percent", "0.15"), "Overlap Percent")
dbutils.widgets.text("vector_search_endpoint", config.get("vector_search_endpoint", "hedis_vector_endpoint"), "Vector Search Endpoint")
dbutils.widgets.text("embedding_model", config.get("embedding_model", "databricks-bge-large-en"), "Embedding Model")
dbutils.widgets.text("vector_index_name", config.get("vector_index_name", "hedis_measures_index"), "Vector Search Index Name")

# Get parameters (widgets override config if changed)
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
chunk_size = int(dbutils.widgets.get("chunk_size"))
overlap_percent = float(dbutils.widgets.get("overlap_percent"))
vector_endpoint_name = dbutils.widgets.get("vector_search_endpoint")
embedding_model = dbutils.widgets.get("embedding_model")
vector_index_name = dbutils.widgets.get("vector_index_name")

# Table and index names
silver_definitions_table = f"{catalog_name}.{schema_name}.hedis_definitions"
silver_chunks_table = f"{catalog_name}.{schema_name}.hedis_measures_chunks"
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
index_name = f"{catalog_name}.{schema_name}.{vector_index_name}"

print(f"📋 Configuration:")
print(f"   Source Table: {silver_definitions_table}")
print(f"   Chunks Table: {silver_chunks_table}")
print(f"   Volume Path: {volume_path}")
print(f"   Chunk Size: {chunk_size} tokens")
print(f"   Overlap: {overlap_percent * 100}%")
print(f"   Vector Search Endpoint: {vector_endpoint_name}")
print(f"   Embedding Model: {embedding_model}")
print(f"   Vector Search Index: {index_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

import sys
sys.path.append("../../src")

from pyspark.sql.types import *
from pyspark.sql import functions as F

# Set catalog/schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print("✅ Environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structure-Aware Chunking from Definitions
# MAGIC
# MAGIC This notebook reads pre-parsed elements from the `hedis_definitions` table (populated by notebook 02 using PyMuPDF).
# MAGIC The parsed elements include position-based classification for headers, footers, and body text.
# MAGIC
# MAGIC **How it helps chunking:**
# MAGIC - Page headers and footers are separated from body content
# MAGIC - Element classification enables semantic chunking
# MAGIC - Page numbers preserved for chunk context
# MAGIC - Ready for overlapping chunk generation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Chunks Table

# COMMAND ----------

# Drop existing table if schema doesn't match (for development)
if spark.catalog.tableExists(silver_chunks_table):
    try:
        existing_schema = spark.table(silver_chunks_table).schema
        expected_fields = {'chunk_id', 'file_id', 'measure_name', 'header', 'footer', 'page_content', 'chunk_content',
                          'chunk_sequence', 'token_count', 'page_start', 'page_end', 'effective_year', 'chunk_timestamp', 'metadata'}
        actual_fields = {field.name for field in existing_schema.fields}

        if expected_fields != actual_fields:
            print(f"⚠️  Schema mismatch detected. Dropping and recreating table...")
            print(f"   Expected: {sorted(expected_fields)}")
            print(f"   Actual: {sorted(actual_fields)}")
            spark.sql(f"DROP TABLE IF EXISTS {silver_chunks_table}")
            print(f"   ✅ Dropped old table")
    except Exception as e:
        print(f"   Error checking schema: {str(e)}")
else:
    print(f"   Table doesn't exist yet, will create it")

# Create table with correct schema and CDF enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_chunks_table} (
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
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
""")

print(f"✅ Silver chunks table created/verified with CDF enabled: {silver_chunks_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Files to Process
# MAGIC
# MAGIC Find files in definitions table that haven't been chunked yet.

# COMMAND ----------

files_to_process = spark.sql(f"""
    SELECT DISTINCT d.file_id, d.file_name, d.effective_year
    FROM {silver_definitions_table} d
    LEFT JOIN (
        SELECT DISTINCT file_id
        FROM {silver_chunks_table}
    ) c ON d.file_id = c.file_id
    WHERE c.file_id IS NULL
    ORDER BY d.file_name
""")

file_count = files_to_process.count()
print(f"📁 Found {file_count} files to process")

if file_count > 0:
    display(files_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Elements from Definitions Table
# MAGIC
# MAGIC Read pre-parsed elements from the `hedis_definitions` table (populated by notebook 02 using PyMuPDF).

# COMMAND ----------

if file_count > 0:
    # Get list of file IDs to process
    file_ids = [row.file_id for row in files_to_process.collect()]
    file_ids_str = "', '".join(file_ids)

    print(f"📖 Loading elements for {len(file_ids)} file(s) from definitions table...")

    # Read elements directly from definitions table
    elements_df = spark.sql(f"""
        SELECT
            file_id,
            file_name,
            effective_year,
            element_type,
            element_content,
            page_number,
            is_page_metadata
        FROM {silver_definitions_table}
        WHERE file_id IN ('{file_ids_str}')
    """)

    element_count = elements_df.count()
    elements_df.createOrReplaceTempView("elements")

    print(f"✅ Loaded {element_count:,} elements from definitions table")

    # Display element summary
    display(elements_df.groupBy("file_name", "element_type").count().orderBy("file_name", "element_type"))
else:
    element_count = 0
    print("⚠️  No files to process")

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
            cast(effective_year AS INT) AS effective_year,
            cast(row_number() OVER (PARTITION BY file_id ORDER BY chunk_id) AS INT) AS chunk_sequence,
            cast(page_start AS INT) AS page_start,
            cast(page_end AS INT) AS page_end,
            header,
            footer,
            page_content,
            chunk_content,
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
            DELETE FROM {silver_chunks_table}
            WHERE file_id IN ('{file_ids_str}')
        """)
        print(f"   🗑️  Removed existing chunks for {len(file_ids_processed)} files")

    # INSERT new chunks
    final_chunks_df.write.mode("append").saveAsTable(silver_chunks_table)

    result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_chunks_table}").first()["cnt"]
    print(f"✅ Wrote {chunk_count:,} chunks to silver table (DELETE+INSERT)")
    print(f"   Total chunks in table: {result_count:,}")

    # Display sample
    print(f"\n📋 Sample chunks from table:")
    display(spark.table(silver_chunks_table).orderBy(F.desc("chunk_timestamp")).limit(10))
else:
    print("⚠️  No chunks generated - skipping write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync to Vector Search

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Initialize Vector Search client
vsc = VectorSearchClient()

print(f"🔍 Vector Search Configuration:")
print(f"   Endpoint: {vector_endpoint_name}")
print(f"   Source Table: {silver_chunks_table}")
print(f"   Index: {index_name}")
print(f"   Embedding Model: {embedding_model}")

# COMMAND ----------

# Create or get vector search index
print(f"\n📊 Creating/updating vector search index: {index_name}")

try:
    # Try to get existing index
    existing_index = vsc.get_index(vector_endpoint_name, index_name)
    print(f"✅ Index already exists: {index_name}")
    index = existing_index

except Exception:
    # Index doesn't exist, create it
    print(f"🏗️  Creating new delta sync index...")

    index = vsc.create_delta_sync_index(
        endpoint_name=vector_endpoint_name,
        source_table_name=silver_chunks_table,
        index_name=index_name,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="chunk_content",
        embedding_model_endpoint_name=embedding_model
    )

    print(f"✅ Index created: {index_name}")

# COMMAND ----------

# Sync the index
print("🔄 Syncing index with delta table...")

import time

for attempt in range(1, 11):
    try:
        print(f"   Attempt {attempt}/10...")
        index.sync()
        print(f"   ✓ Sync started successfully")
        break
    except Exception as e:
        print(f"   Failed: {e}")
        if attempt < 10:
            print(f"   Waiting 30 seconds...")
            time.sleep(30)
else:
    print("   ✗ Sync timed out after 10 attempts")

print(f"\n✅ Vector search index synced!")
print(f"   Index: {index_name}")
print(f"   Source: {silver_chunks_table}")
print(f"   Embedding column: chunk_content")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test your Search Store

# COMMAND ----------

from databricks.vector_search.reranker import DatabricksReranker

query_text = "What are the initial population criteria for colorectal cancer screening?"

# Perform hybrid search with reranking
results = index.similarity_search(
    query_text=query_text,
    columns=["chunk_id", "chunk_content", "page_start", "page_end", "effective_year"],
    num_results=10,
    query_type="hybrid",  # Combines ANN semantic search with keyword matching
    reranker=DatabricksReranker(
        columns_to_rerank=["chunk_content"]  # Rerank based on chunk content
    )
)

# Display top 3 results; feel free to edit
print(f"📊 Retrieved {len(results['result']['data_array'])} results (showing top 3)\n")

for i, result in enumerate(results['result']['data_array'][:3], 1):
    chunk_id = result[0]
    chunk_content = result[1]
    page_start = result[2]
    page_end = result[3]
    effective_year = result[4]
    score = result[5]

    print(f"Match {i}: Score {score:.3f}")
    print(f"  Chunk ID: {chunk_id}")
    print(f"  Year: {effective_year}")
    print(f"  Pages: {page_start}-{page_end}")
    print(f"  Content: {chunk_content[:2000]}...")
    print()
