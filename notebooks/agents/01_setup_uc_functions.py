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
dbutils.widgets.text("vs_index_name", "hedis_chunks_index", "Vector Search Index Name")

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
  query STRING,
  num_results INT,
  effective_year INT
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Semantic search over HEDIS chunks. Returns JSON array of results with chunk_id, content, pages, year, measure, and score.'
AS $$
from databricks.vector_search.client import VectorSearchClient
import json

try:
    vsc = VectorSearchClient()
    index = vsc.get_index(
        endpoint_name="{VS_ENDPOINT}",
        index_name="{VS_INDEX}"
    )

    # Build filters
    filters = {{}}
    if effective_year is not None:
        filters["effective_year"] = effective_year

    # Columns to retrieve
    columns = [
        "chunk_id",
        "chunk_content",
        "page_start",
        "page_end",
        "effective_year",
        "measure_name"
    ]

    # Execute search
    raw_results = index.similarity_search(
        query_text=query,
        columns=columns,
        num_results=num_results if num_results else 5,
        filters=filters if filters else None,
        query_type="hybrid"
    )

    # Parse results
    results = []
    data_array = raw_results.get("result", {{}}).get("data_array", [])

    for row in data_array:
        row_dict = dict(zip(columns, row))
        score = row[-1] if len(row) > len(columns) else 0.0

        results.append({{
            "chunk_id": row_dict.get("chunk_id", ""),
            "chunk_content": row_dict.get("chunk_content", ""),
            "page_start": int(row_dict.get("page_start", 0)),
            "page_end": int(row_dict.get("page_end", 0)),
            "effective_year": int(row_dict.get("effective_year", 0)),
            "measure_name": row_dict.get("measure_name"),
            "score": float(score)
        }})

    return json.dumps(results)

except Exception as e:
    return json.dumps({{"error": str(e), "results": []}})
$$
""")

print(f"Created: {CATALOG}.{SCHEMA}.measures_document_search")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Functions

# COMMAND ----------

# Test measure_definition_lookup
print("Testing measure_definition_lookup('BCS', NULL)...")
result = spark.sql(f"SELECT * FROM TABLE({CATALOG}.{SCHEMA}.measure_definition_lookup('BCS', NULL))")
if result.count() > 0:
    print("SUCCESS - Results:")
    result.show(truncate=False)
else:
    print("WARNING - No results. Ensure hedis_measures_definitions has data.")

# COMMAND ----------

# Test measures_document_search
print("\nTesting measures_document_search('diabetes screening', 3, NULL)...")
result = spark.sql(f"SELECT {CATALOG}.{SCHEMA}.measures_document_search('diabetes screening', 3, NULL) as results")
result_json = result.first()["results"]

import json
results = json.loads(result_json)

if "error" in results:
    print(f"ERROR: {results['error']}")
elif isinstance(results, list) and len(results) > 0:
    print(f"SUCCESS - {len(results)} results:")
    for i, r in enumerate(results, 1):
        print(f"{i}. {r.get('measure_name')} (Score: {r.get('score', 0):.3f})")
        print(f"   Pages: {r.get('page_start')}-{r.get('page_end')}, Year: {r.get('effective_year')}")
        print(f"   Content: {r.get('chunk_content', '')[:80]}...")
else:
    print("WARNING - No results. Ensure vector search index is populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grant Permissions

# COMMAND ----------

try:
    spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.measure_definition_lookup TO `account users`")
    spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.measures_document_search TO `account users`")
    print("Granted EXECUTE to 'account users'")
except Exception as e:
    print(f"Could not grant permissions (may need admin): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Created two UC functions:
# MAGIC
# MAGIC **1. measure_definition_lookup (SQL TVF)**
# MAGIC ```sql
# MAGIC SELECT * FROM TABLE(measure_definition_lookup('BCS', 2025))
# MAGIC ```
# MAGIC
# MAGIC **2. measures_document_search (Python UDF)**
# MAGIC ```sql
# MAGIC SELECT measures_document_search('diabetes screening', 5, NULL)
# MAGIC ```
# MAGIC
# MAGIC Both functions are ready for agent use. No complex wrappers needed.
