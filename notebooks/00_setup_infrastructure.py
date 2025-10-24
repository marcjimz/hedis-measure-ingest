# Databricks notebook source
# MAGIC %md
# MAGIC # Infrastructure Setup for HEDIS Measure Ingestion Pipeline
# MAGIC
# MAGIC This notebook creates all necessary infrastructure for the HEDIS pipeline:
# MAGIC - Unity Catalog (Catalog + Schema)
# MAGIC - Volume for storing HEDIS PDFs
# MAGIC - Vector Search Endpoint
# MAGIC - Delta Tables (Bronze + Silver)
# MAGIC
# MAGIC **Run this notebook FIRST** before executing the pipeline notebooks.
# MAGIC
# MAGIC ## What Gets Created
# MAGIC
# MAGIC 1. **Catalog**: Unity Catalog namespace
# MAGIC 2. **Schema**: Database within catalog
# MAGIC 3. **Volume**: Managed storage for HEDIS PDFs
# MAGIC 4. **Vector Search Endpoint**: For semantic search
# MAGIC 5. **Bronze Table**: `hedis_file_metadata`
# MAGIC 6. **Silver Table 1**: `hedis_measures_definitions`
# MAGIC 7. **Silver Table 2**: `hedis_measures_chunks`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC
# MAGIC Customize these widgets based on your environment (dev/staging/prod).

# COMMAND ----------

# Configuration widgets
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "hedis_pipeline", "Schema Name")
dbutils.widgets.text("volume_name", "hedis", "Volume Name")
dbutils.widgets.text("vector_search_endpoint", "hedis_vector_endpoint", "Vector Search Endpoint")
dbutils.widgets.dropdown("create_catalog", "no", ["yes", "no"], "Create Catalog?")
dbutils.widgets.dropdown("create_schema", "yes", ["yes", "no"], "Create Schema?")
dbutils.widgets.dropdown("create_volume", "yes", ["yes", "no"], "Create Volume?")
dbutils.widgets.dropdown("create_vector_endpoint", "yes", ["yes", "no"], "Create Vector Search Endpoint?")
dbutils.widgets.dropdown("create_tables", "yes", ["yes", "no"], "Create Delta Tables?")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
vector_endpoint_name = dbutils.widgets.get("vector_search_endpoint")

# Flags
create_catalog = dbutils.widgets.get("create_catalog") == "yes"
create_schema = dbutils.widgets.get("create_schema") == "yes"
create_volume = dbutils.widgets.get("create_volume") == "yes"
create_vector_endpoint = dbutils.widgets.get("create_vector_endpoint") == "yes"
create_tables = dbutils.widgets.get("create_tables") == "yes"

# Display configuration
print("🔧 Infrastructure Configuration:")
print("=" * 60)
print(f"Catalog:        {catalog_name} {'[CREATE]' if create_catalog else '[USE EXISTING]'}")
print(f"Schema:         {schema_name} {'[CREATE]' if create_schema else '[USE EXISTING]'}")
print(f"Volume:         {volume_name} {'[CREATE]' if create_volume else '[USE EXISTING]'}")
print(f"Vector Endpoint: {vector_endpoint_name} {'[CREATE]' if create_vector_endpoint else '[USE EXISTING]'}")
print(f"Tables:         {'[CREATE]' if create_tables else '[SKIP]'}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
import time

# Initialize clients
spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
vsc = VectorSearchClient()

print("✅ Clients initialized")
print(f"   Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog (if needed)
# MAGIC
# MAGIC **Note**: Creating a catalog requires high-level permissions (metastore admin). If you don't have permissions, set `Create Catalog?` to "no" and use an existing catalog.

# COMMAND ----------

if create_catalog:
    try:
        print(f"🏗️  Creating catalog: {catalog_name}")

        spark.sql(f"""
            CREATE CATALOG IF NOT EXISTS {catalog_name}
            COMMENT 'HEDIS Measure Ingestion Pipeline catalog'
        """)

        print(f"✅ Catalog created: {catalog_name}")

    except Exception as e:
        print(f"⚠️  Catalog creation failed: {str(e)}")
        print("   Using existing catalog or insufficient permissions")
else:
    print(f"⏭️  Skipping catalog creation, using existing: {catalog_name}")

# Verify catalog exists
try:
    catalogs = [c.name for c in spark.sql("SHOW CATALOGS").collect()]
    if catalog_name in catalogs:
        print(f"✅ Catalog verified: {catalog_name}")
    else:
        raise ValueError(f"Catalog '{catalog_name}' does not exist")
except Exception as e:
    print(f"❌ Catalog verification failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schema

# COMMAND ----------

if create_schema:
    try:
        print(f"🏗️  Creating schema: {catalog_name}.{schema_name}")

        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
            COMMENT 'HEDIS pipeline tables and volumes'
        """)

        print(f"✅ Schema created: {catalog_name}.{schema_name}")

    except Exception as e:
        print(f"❌ Schema creation failed: {str(e)}")
        raise
else:
    print(f"⏭️  Skipping schema creation, using existing: {catalog_name}.{schema_name}")

# Set default catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Set default catalog/schema: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume for HEDIS PDFs

# COMMAND ----------

if create_volume:
    try:
        print(f"🏗️  Creating volume: {catalog_name}.{schema_name}.{volume_name}")

        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
            COMMENT 'Storage for HEDIS PDF documents'
        """)

        volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
        print(f"✅ Volume created: {volume_path}")

        # Display volume info
        volume_info = spark.sql(f"DESCRIBE VOLUME {catalog_name}.{schema_name}.{volume_name}").collect()
        print("\n📁 Volume Details:")
        for row in volume_info:
            print(f"   {row.info_name}: {row.info_value}")

    except Exception as e:
        print(f"❌ Volume creation failed: {str(e)}")
        raise
else:
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    print(f"⏭️  Skipping volume creation, using existing: {volume_path}")

# Verify volume is accessible
try:
    files = dbutils.fs.ls(volume_path)
    print(f"✅ Volume verified and accessible: {volume_path}")
    print(f"   Current files: {len(files)}")
except Exception as e:
    print(f"⚠️  Volume verification failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Vector Search Endpoint
# MAGIC
# MAGIC **Note**: Vector Search endpoint creation may take 5-10 minutes. The endpoint needs to be in "ONLINE" state before use.

# COMMAND ----------

if create_vector_endpoint:
    try:
        print(f"🏗️  Creating Vector Search endpoint: {vector_endpoint_name}")
        print("   ⏳ This may take 5-10 minutes...")

        # Check if endpoint already exists
        try:
            existing_endpoint = vsc.get_endpoint(vector_endpoint_name)
            print(f"✅ Endpoint already exists: {vector_endpoint_name}")
            print(f"   Status: {existing_endpoint.get('endpoint_status', {}).get('state', 'Unknown')}")
        except Exception:
            # Endpoint doesn't exist, create it
            vsc.create_endpoint(
                name=vector_endpoint_name,
                endpoint_type="STANDARD"
            )

            print(f"✅ Endpoint creation initiated: {vector_endpoint_name}")
            print("   Waiting for endpoint to come online...")

            # Wait for endpoint to be ready
            max_wait = 600  # 10 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                try:
                    endpoint = vsc.get_endpoint(vector_endpoint_name)
                    state = endpoint.get('endpoint_status', {}).get('state', 'Unknown')

                    print(f"   Status: {state}")

                    if state == "ONLINE":
                        print(f"✅ Endpoint is online: {vector_endpoint_name}")
                        break
                    elif state in ["OFFLINE", "PROVISIONING"]:
                        time.sleep(30)  # Check every 30 seconds
                    else:
                        print(f"⚠️  Unexpected state: {state}")
                        break
                except Exception as e:
                    print(f"   Waiting... ({int(time.time() - start_time)}s)")
                    time.sleep(30)
            else:
                print("⚠️  Endpoint creation timed out, but it may still be provisioning")
                print("   Check status with: vsc.get_endpoint('{vector_endpoint_name}')")

    except Exception as e:
        print(f"❌ Vector Search endpoint creation failed: {str(e)}")
        print("   You can create it manually or skip vector search integration")
else:
    print(f"⏭️  Skipping Vector Search endpoint creation")

    # Check if endpoint exists
    try:
        endpoint = vsc.get_endpoint(vector_endpoint_name)
        state = endpoint.get('endpoint_status', {}).get('state', 'Unknown')
        print(f"✅ Using existing endpoint: {vector_endpoint_name} (Status: {state})")
    except Exception as e:
        print(f"⚠️  Warning: Endpoint '{vector_endpoint_name}' not found: {str(e)}")
        print("   Vector search will not work until endpoint is created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Delta Tables
# MAGIC
# MAGIC Creates three tables:
# MAGIC 1. Bronze: `hedis_file_metadata`
# MAGIC 2. Silver 1: `hedis_measures_definitions`
# MAGIC 3. Silver 2: `hedis_measures_chunks`

# COMMAND ----------

if create_tables:
    print("🏗️  Creating Delta tables...")

    # Table 1: Bronze - File Metadata
    print("\n1️⃣  Creating bronze table: hedis_file_metadata")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hedis_file_metadata (
            file_id STRING NOT NULL COMMENT 'Unique file identifier (UUID)',
            file_name STRING NOT NULL COMMENT 'Original filename',
            file_path STRING NOT NULL COMMENT 'Full volume path',
            volume_ingestion_date TIMESTAMP COMMENT 'When uploaded to volume',
            published_date STRING COMMENT 'Extracted from filename',
            effective_year INT COMMENT 'HEDIS year (e.g., 2025)',
            file_size_bytes LONG COMMENT 'File size in bytes',
            page_count INT COMMENT 'Total pages in PDF',
            processing_status STRING COMMENT 'pending/processing/completed/failed',
            ingestion_timestamp TIMESTAMP COMMENT 'Pipeline ingestion time',
            last_modified TIMESTAMP COMMENT 'Last update timestamp',
            checksum STRING COMMENT 'SHA256 hash for deduplication'
        )
        USING DELTA
        COMMENT 'Bronze layer: HEDIS file metadata catalog'
    """)
    print("   ✅ Bronze table created")

    # Table 2: Silver - Measure Definitions
    print("\n2️⃣  Creating silver table: hedis_measures_definitions")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hedis_measures_definitions (
            measure_id STRING NOT NULL COMMENT 'Unique measure identifier',
            file_id STRING NOT NULL COMMENT 'Foreign key to bronze table',
            specifications STRING NOT NULL COMMENT 'Official measure description from NCQA',
            measure STRING NOT NULL COMMENT 'Measure name with standard acronym',
            initial_pop STRING COMMENT 'Initial population definition',
            denominator ARRAY<STRING> COMMENT 'Denominator components (normalized)',
            numerator ARRAY<STRING> COMMENT 'Numerator details (normalized)',
            exclusion ARRAY<STRING> COMMENT 'Exclusion conditions (normalized)',
            effective_year INT NOT NULL COMMENT 'Measure effective year',
            page_start INT COMMENT 'Starting page in source PDF',
            page_end INT COMMENT 'Ending page in source PDF',
            extraction_timestamp TIMESTAMP COMMENT 'When extracted',
            extraction_confidence DOUBLE COMMENT 'LLM confidence score (0.0-1.0)',
            source_text STRING COMMENT 'Raw extracted text for auditing'
        )
        USING DELTA
        COMMENT 'Silver layer: Structured HEDIS measure definitions'
        PARTITIONED BY (effective_year)
    """)
    print("   ✅ Silver definitions table created")

    # Table 3: Silver - Chunks for Vector Search
    print("\n3️⃣  Creating silver table: hedis_measures_chunks")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hedis_measures_chunks (
            chunk_id STRING NOT NULL COMMENT 'Unique chunk identifier',
            file_id STRING NOT NULL COMMENT 'Foreign key to bronze table',
            measure_name STRING COMMENT 'Associated measure name if available',
            chunk_text STRING NOT NULL COMMENT 'Chunk content with markdown headers',
            chunk_sequence INT NOT NULL COMMENT 'Sequence order within file',
            token_count INT COMMENT 'Approximate token count',
            page_start INT COMMENT 'Starting page number',
            page_end INT COMMENT 'Ending page number',
            headers ARRAY<STRING> COMMENT 'Header hierarchy (H1 > H2 > H3)',
            char_start LONG COMMENT 'Character offset start',
            char_end LONG COMMENT 'Character offset end',
            effective_year INT COMMENT 'HEDIS year',
            chunk_timestamp TIMESTAMP COMMENT 'Processing timestamp',
            metadata STRING COMMENT 'JSON metadata for vector search'
        )
        USING DELTA
        COMMENT 'Silver layer: Text chunks for vector search'
        PARTITIONED BY (effective_year)
    """)
    print("   ✅ Silver chunks table created")

    print("\n✅ All Delta tables created successfully")

else:
    print("⏭️  Skipping table creation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Infrastructure

# COMMAND ----------

print("🔍 Verifying infrastructure setup...")
print("=" * 60)

# Check catalog
try:
    catalogs = [c.name for c in spark.sql("SHOW CATALOGS").collect()]
    if catalog_name in catalogs:
        print(f"✅ Catalog exists: {catalog_name}")
    else:
        print(f"❌ Catalog not found: {catalog_name}")
except Exception as e:
    print(f"❌ Catalog check failed: {str(e)}")

# Check schema
try:
    schemas = [s.databaseName for s in spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()]
    if schema_name in schemas:
        print(f"✅ Schema exists: {catalog_name}.{schema_name}")
    else:
        print(f"❌ Schema not found: {catalog_name}.{schema_name}")
except Exception as e:
    print(f"❌ Schema check failed: {str(e)}")

# Check volume
try:
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    dbutils.fs.ls(volume_path)
    print(f"✅ Volume accessible: {volume_path}")
except Exception as e:
    print(f"❌ Volume not accessible: {str(e)}")

# Check vector search endpoint
try:
    endpoint = vsc.get_endpoint(vector_endpoint_name)
    state = endpoint.get('endpoint_status', {}).get('state', 'Unknown')
    if state == "ONLINE":
        print(f"✅ Vector Search endpoint online: {vector_endpoint_name}")
    else:
        print(f"⚠️  Vector Search endpoint exists but not online: {vector_endpoint_name} (Status: {state})")
except Exception as e:
    print(f"⚠️  Vector Search endpoint not found: {vector_endpoint_name}")

# Check tables
tables_to_check = [
    "hedis_file_metadata",
    "hedis_measures_definitions",
    "hedis_measures_chunks"
]

for table_name in tables_to_check:
    try:
        spark.table(f"{catalog_name}.{schema_name}.{table_name}")
        print(f"✅ Table exists: {catalog_name}.{schema_name}.{table_name}")
    except Exception as e:
        print(f"❌ Table not found: {catalog_name}.{schema_name}.{table_name}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Display Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("📊 INFRASTRUCTURE SETUP SUMMARY")
print("=" * 60)

print("\n🎯 Resources Created:")
print(f"   Catalog:  {catalog_name}")
print(f"   Schema:   {catalog_name}.{schema_name}")
print(f"   Volume:   /Volumes/{catalog_name}/{schema_name}/{volume_name}")
print(f"   Endpoint: {vector_endpoint_name}")

print("\n📋 Delta Tables:")
print(f"   1. {catalog_name}.{schema_name}.hedis_file_metadata (Bronze)")
print(f"   2. {catalog_name}.{schema_name}.hedis_measures_definitions (Silver)")
print(f"   3. {catalog_name}.{schema_name}.hedis_measures_chunks (Silver)")

print("\n📁 Volume Path for HEDIS PDFs:")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
print(f"   {volume_path}")

print("\n🚀 Next Steps:")
print("   1. Upload HEDIS PDF to volume:")
print(f"      databricks fs cp <local-path>/HEDIS*.pdf dbfs:{volume_path}/")
print("\n   2. Run the pipeline notebooks in order:")
print("      - 01_bronze_metadata_ingestion.py")
print("      - 02_silver_definitions_extraction.py")
print("      - 03_silver_chunks_processing.py")

print("\n   3. (Optional) Configure Vector Search index:")
print("      - Open the chunks table in Databricks UI")
print("      - Click 'Create Vector Search Index'")
print(f"      - Select endpoint: {vector_endpoint_name}")
print("      - Set primary key: chunk_id")
print("      - Set embedding source: chunk_text")

print("\n" + "=" * 60)
print("✅ Setup complete! Infrastructure is ready.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: Useful Commands

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Infrastructure Status
# MAGIC
# MAGIC ```python
# MAGIC # List catalogs
# MAGIC display(spark.sql("SHOW CATALOGS"))
# MAGIC
# MAGIC # List schemas
# MAGIC display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))
# MAGIC
# MAGIC # List tables
# MAGIC display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}"))
# MAGIC
# MAGIC # List volumes
# MAGIC display(spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema_name}"))
# MAGIC
# MAGIC # Check vector search endpoints
# MAGIC for endpoint in vsc.list_endpoints():
# MAGIC     print(f"{endpoint.name}: {endpoint.endpoint_status.state}")
# MAGIC ```
# MAGIC
# MAGIC ### Cleanup Commands (Use with caution!)
# MAGIC
# MAGIC ```python
# MAGIC # Drop tables (WARNING: Deletes all data!)
# MAGIC # spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.hedis_file_metadata")
# MAGIC # spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.hedis_measures_definitions")
# MAGIC # spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.hedis_measures_chunks")
# MAGIC
# MAGIC # Drop volume (WARNING: Deletes all files!)
# MAGIC # spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_name}.{volume_name}")
# MAGIC
# MAGIC # Drop schema (WARNING: Deletes all tables!)
# MAGIC # spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
# MAGIC
# MAGIC # Delete vector search endpoint (WARNING: Cannot be undone!)
# MAGIC # vsc.delete_endpoint(vector_endpoint_name)
# MAGIC ```
