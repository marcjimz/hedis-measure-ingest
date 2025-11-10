# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog Functions for HEDIS Agent
# MAGIC
# MAGIC Creates three simple UC functions:
# MAGIC 1. **measures_definition_lookup** - SQL TVF for measure lookups
# MAGIC 2. **measures_document_search** - SQL TVF for semantic search with VECTOR_SEARCH
# MAGIC 3. **measures_search_expansion** - SQL TVF for AI-powered query expansion
# MAGIC
# MAGIC Total: ~300 lines. Simple, deployable, testable.

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
dbutils.widgets.text("vs_index_name", "hedis_measures_index", "Vector Search Index Name")
dbutils.widgets.text("llm_endpoint", "databricks-claude-opus-4-1", "LLM Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")
VS_INDEX_NAME = dbutils.widgets.get("vs_index_name")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")

# Construct full vector search index path from catalog and schema
VS_INDEX = f"{CATALOG}.{SCHEMA}.{VS_INDEX_NAME}"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Vector Search Endpoint: {VS_ENDPOINT}")
print(f"Vector Search Index Name: {VS_INDEX_NAME}")
print(f"Vector Search Index (Full): {VS_INDEX}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")

# COMMAND ----------

# Set catalog and schema context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create measures_definition_lookup (SQL TVF)
# MAGIC
# MAGIC Simple SQL table-valued function for measure lookups.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.measures_definition_lookup(
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

print(f"Created: {CATALOG}.{SCHEMA}.measures_definition_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create measures_document_search
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
# MAGIC ## 3. Create measures_search_expansion (SQL TVF with AI_QUERY)
# MAGIC
# MAGIC Uses AI_QUERY to generate multiple search query expansions from a single term.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.measures_search_expansion(
  query_term STRING,
  num_expansions INT
)
RETURNS TABLE(
  expansion_id INT,
  expanded_query STRING
)
COMMENT 'Generate search query expansions using AI for better semantic coverage'
RETURN
  WITH expansion_prompt AS (
    SELECT CONCAT(
      'Given the HEDIS healthcare quality measure search term: "', query_term, '", ',
      'generate ', CAST(COALESCE(num_expansions, 3) AS STRING), ' different search query variations that would help find relevant information. ',
      'Include synonyms, related medical terms, and different phrasings. ',
      'Return ONLY a JSON array of strings, one per variation. ',
      'Example format: ["variation 1", "variation 2", "variation 3"]'
    ) AS prompt
  ),
  ai_response AS (
    SELECT AI_QUERY(
      '{LLM_ENDPOINT}',
      prompt
    ) AS response_text
    FROM expansion_prompt
  ),
  parsed_expansions AS (
    SELECT
      POSEXPLODE(
        FROM_JSON(
          response_text,
          'array<string>'
        )
      ) AS (expansion_id, expanded_query)
    FROM ai_response
  )
  SELECT
    expansion_id + 1 AS expansion_id,
    expanded_query
  FROM parsed_expansions
  WHERE expansion_id < COALESCE(num_expansions, 3)
""")

print(f"Created: {CATALOG}.{SCHEMA}.measures_search_expansion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test Functions

# COMMAND ----------

# Test measures_definition_lookup
print("Testing measures_definition_lookup('CWP', NULL)...")
result = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.measures_definition_lookup('CWP', NULL)")
if result.count() > 0:
    print("SUCCESS - Results:")
    display(result)
else:
    print("WARNING - No results. Ensure hedis_measures_definitions has data.")

# COMMAND ----------

# Test measures_search_expansion
print("\nTesting measures_search_expansion('diabetes screening', 3)...")
expansions = spark.sql(f"""
    SELECT expansion_id, expanded_query
    FROM {CATALOG}.{SCHEMA}.measures_search_expansion('diabetes screening', 3)
""")

print(f"Generated {expansions.count()} query expansions:")
display(expansions)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from marcin_demo.hedis_measurements.measures_document_search('diabtes', 5, 2025)

# COMMAND ----------

# MAGIC %md
# MAGIC # Search with AI-powered query expansion
# MAGIC
# MAGIC This demonstrates how AI can improve search results by:
# MAGIC
# MAGIC 1. Taking your original search term ("diabetes screening")
# MAGIC 2. Using AI to generate alternative phrasings (e.g., "HbA1c testing diabetic patients", "comprehensive diabetes care blood sugar monitoring")
# MAGIC 3. Searching with both the original query AND all AI-generated variations
# MAGIC 4. Combining and de-duplicating results to show you the top most relevant document chunks
# MAGIC 5. Using an LLM to score relevance and rerank results based on the original query
# MAGIC
# MAGIC This helps find relevant content even when documents use different terminology than your search term, and is helpful when considering queries that are not directly matching chunks in your documents.

# COMMAND ----------

print("\nTesting measures_document_search with AI-generated query expansions and reranking...")

# First, get the combined search results
combined_results = spark.sql(f"""
    WITH original_search AS (
        SELECT 
            'diabetes screening' as original_query,
            'diabetes screening' as expanded_query,
            s.chunk_id,
            s.score,
            s.chunk_content,
            s.page_start,
            s.page_end,
            s.effective_year
        FROM {CATALOG}.{SCHEMA}.measures_document_search('diabetes screening', 5, 2025) s
    ),
    expansions AS (
        SELECT expanded_query
        FROM {CATALOG}.{SCHEMA}.measures_search_expansion('diabetes screening', 3)
    ),
    expanded_search_results AS (
        SELECT 
            'diabetes screening' as original_query,
            e.expanded_query,
            s.chunk_id,
            s.score,
            s.chunk_content,
            s.page_start,
            s.page_end,
            s.effective_year
        FROM expansions e,
        LATERAL {CATALOG}.{SCHEMA}.measures_document_search(e.expanded_query, 5, 2025) s
    ),
    all_search_results AS (
        SELECT * FROM original_search
        UNION ALL
        SELECT * FROM expanded_search_results
    ),
    deduplicated AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY chunk_id 
                ORDER BY score DESC
            ) as rn
        FROM all_search_results
    ),
    top_candidates AS (
        SELECT 
            original_query,
            expanded_query,
            chunk_id,
            score as original_score,
            chunk_content,
            page_start,
            page_end,
            effective_year
        FROM deduplicated
        WHERE rn = 1
        ORDER BY score DESC
        LIMIT 20
    )
    SELECT 
        expanded_query as query,
        chunk_id,
        original_score as query_score,
        chunk_content,
        page_start,
        page_end,
        effective_year,
        ai_query(
            '{LLM_ENDPOINT}',
            CONCAT(
                'Rate the relevance of this document chunk to the query "', original_query, '" on a scale of 0.0 to 1.0. ',
                'Only respond with a single decimal number between 0.0 and 1.0. ',
                'Document chunk: ', chunk_content
            )
        ) as rerank_score
    FROM top_candidates
    ORDER BY CAST(rerank_score AS DOUBLE) DESC
    LIMIT 20
""")

print(f"\nTop results after AI reranking:")
display(combined_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Created three UC functions:
# MAGIC
# MAGIC **1. measures_definition_lookup (SQL TVF)**
# MAGIC ```sql
# MAGIC SELECT * FROM TABLE(measures_definition_lookup('BCS', 2025))
# MAGIC ```
# MAGIC
# MAGIC **2. measures_document_search (SQL TVF)**
# MAGIC ```sql
# MAGIC SELECT * FROM measures_document_search('diabetes screening', 5, NULL)
# MAGIC ```
# MAGIC
# MAGIC **3. measures_search_expansion (SQL TVF with AI_QUERY)**
# MAGIC ```sql
# MAGIC SELECT * FROM measures_search_expansion('diabetes', 3)
# MAGIC ```
# MAGIC
# MAGIC All functions are ready for agent use. No complex wrappers needed.
