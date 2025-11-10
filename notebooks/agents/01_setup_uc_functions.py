# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog Functions for HEDIS Agent
# MAGIC
# MAGIC Creates two simple UC functions:
# MAGIC 1. **measure_definition_lookup** - SQL TVF for measure lookups
# MAGIC 2. **measures_document_search** - Python UDF for semantic search
# MAGIC
# MAGIC Total: ~150 lines. Simple, deployable, testable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "hedis_measurements", "Schema")
dbutils.widgets.text("vs_endpoint", "hedis_vector_endpoint", "Vector Search Endpoint")
dbutils.widgets.text("vs_index_name", "hedis_measures_chunks", "Vector Search Index Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")
VS_INDEX_NAME = dbutils.widgets.get("vs_index_name")

# Construct full vector search index path from catalog and schema
VS_INDEX = f"{CATALOG}.{SCHEMA}.{VS_INDEX_NAME}"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Vector Search Endpoint: {VS_ENDPOINT}")
print(f"Vector Search Index Name: {VS_INDEX_NAME}")
print(f"Vector Search Index (Full): {VS_INDEX}")

# COMMAND ----------

# Set catalog and schema context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create measure_definition_lookup (SQL TVF)
# MAGIC
# MAGIC Simple SQL table-valued function for measure lookups.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.measure_definition_lookup(
  search_acronym STRING,
  search_year INT
)
RETURNS TABLE(
  measure_id STRING,
  measure_acronym STRING,
  measure STRING,
  specifications STRING,
  initial_pop STRING,
  denominator ARRAY<STRING>,
  numerator ARRAY<STRING>,
  exclusion ARRAY<STRING>,
  effective_year INT,
  page_start INT,
  page_end INT,
  file_name STRING
)
COMMENT 'Lookup HEDIS measure by acronym and optional year. Returns measure definition details.'
RETURN
  SELECT
    measure_id,
    measure_acronym,
    measure,
    specifications,
    initial_pop,
    denominator,
    numerator,
    exclusion,
    effective_year,
    page_start,
    page_end,
    file_name
  FROM {CATALOG}.{SCHEMA}.hedis_measures_definitions
  WHERE
    UPPER(measure_acronym) = UPPER(search_acronym)
    AND (search_year IS NULL OR effective_year = search_year)
  ORDER BY effective_year DESC
  LIMIT 1
""")

print(f"Created: {CATALOG}.{SCHEMA}.measure_definition_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create measures_document_search (Python UDF)
# MAGIC
# MAGIC Simple Python UDF for semantic search over HEDIS chunks.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.measures_document_search(
  search_query STRING,
  num_results INT,
  filter_year INT
)
RETURNS TABLE(
  chunk_id STRING,
  chunk_content STRING,
  page_start INT,
  page_end INT,
  effective_year INT,
  measure_name STRING,
  score DOUBLE
)
COMMENT 'Semantic search over HEDIS chunks with optional year filtering'
RETURN
  WITH ranked_results AS (
    SELECT 
      chunk_id,
      chunk_content,
      page_start,
      page_end,
      effective_year,
      measure_name,
      search_score as score,
      ROW_NUMBER() OVER (ORDER BY search_score DESC) as rn
    FROM VECTOR_SEARCH(
      index => '{VS_INDEX}',
      query_text => search_query,
      num_results => 100,
      query_type => 'HYBRID'
    )
    WHERE filter_year IS NULL OR effective_year = filter_year
  )
  SELECT 
    chunk_id,
    chunk_content,
    page_start,
    page_end,
    effective_year,
    measure_name,
    score
  FROM ranked_results
  WHERE rn <= COALESCE(num_results, 5)
""")

print(f"Created: {CATALOG}.{SCHEMA}.measures_document_search")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Functions

# COMMAND ----------

# Test measure_definition_lookup
print("Testing measure_definition_lookup('BCS', NULL)...")
result = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.measure_definition_lookup('CWP', NULL)")
if result.count() > 0:
    display(result)
else:
    print("WARNING - No results. Ensure hedis_measures_definitions has data.")

# COMMAND ----------

# Test measures_document_search
print("\nTesting measures_document_search('diabetes screening', 3, NULL)...")
result = spark.sql(f"""
    SELECT score, chunk_content, page_start, page_end, effective_year
    FROM {CATALOG}.{SCHEMA}.measures_document_search('diabetes screening', 3, NULL)
""")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grant Permissions

# COMMAND ----------

# try:
#     spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.measure_definition_lookup TO `account users`")
#     spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.measures_document_search TO `account users`")
#     print("Granted EXECUTE to 'account users'")
# except Exception as e:
#     print(f"Could not grant permissions (may need admin): {e}")
