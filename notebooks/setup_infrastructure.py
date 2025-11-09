# Databricks notebook source
# MAGIC %md
# MAGIC # Infrastructure Setup for HEDIS Measure Ingestion Pipeline
# MAGIC
# MAGIC This notebook creates all necessary infrastructure for the HEDIS pipeline:
# MAGIC - Unity Catalog (Catalog + Schema)
# MAGIC - Volume for storing HEDIS PDFs
# MAGIC - Vector Search Endpoint
# MAGIC - Lakebase PostgreSQL Instance (Optional)
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
# MAGIC 5. **Lakebase PostgreSQL Instance** (Optional): For conversation history persistence
# MAGIC 6. **Bronze Table**: `hedis_file_metadata`
# MAGIC 7. **Silver Table 1**: `hedis_measures_definitions`
# MAGIC 8. **Silver Table 2**: `hedis_measures_chunks`

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC
# MAGIC Customize these widgets based on your environment (dev/staging/prod).

# COMMAND ----------

# Configuration widgets - only resource names, no flags
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "hedis_measurements", "Schema Name")
dbutils.widgets.text("volume_name", "hedis", "Volume Name")
dbutils.widgets.text("vector_search_endpoint", "hedis_vector_endpoint", "Vector Search Endpoint")
dbutils.widgets.dropdown("create_lakebase", "No", ["Yes", "No"], "Create Lakebase Instance")
dbutils.widgets.text("lakebase_instance_name", "hedis_agent_pg", "Lakebase Instance Name")

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
vector_endpoint_name = dbutils.widgets.get("vector_search_endpoint")
create_lakebase = dbutils.widgets.get("create_lakebase") == "Yes"
lakebase_instance_name = dbutils.widgets.get("lakebase_instance_name")

# Display configuration
print("🔧 Infrastructure Configuration:")
print("=" * 60)
print(f"Catalog:         {catalog_name}")
print(f"Schema:          {schema_name}")
print(f"Volume:          {volume_name}")
print(f"Vector Endpoint: {vector_endpoint_name}")
print(f"Create Lakebase: {'Yes' if create_lakebase else 'No'}")
if create_lakebase:
    print(f"Lakebase Instance: {lakebase_instance_name}")
print("=" * 60)
print("\n💡 Infrastructure will be created only if it doesn't already exist")
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
w = WorkspaceClient()
vsc = VectorSearchClient()

print("✅ Clients initialized")
print(f"   Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog (if needed)
# MAGIC
# MAGIC **Note**: Creating a catalog requires high-level permissions (metastore admin). If catalog already exists, it will be reused.

# COMMAND ----------

print(f"🔍 Checking if catalog exists: {catalog_name}")

# Check if catalog exists
try:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    catalog_exists = catalog_name in catalogs

    if catalog_exists:
        print(f"✅ Catalog already exists: {catalog_name}")
    else:
        print(f"🏗️  Catalog not found, creating: {catalog_name}")
        try:
            spark.sql(f"""
                CREATE CATALOG {catalog_name}
                COMMENT 'HEDIS Measure Ingestion Pipeline catalog'
            """)
            print(f"✅ Catalog created: {catalog_name}")
        except Exception as e:
            print(f"❌ Catalog creation failed: {str(e)}")
            print("   You may need metastore admin permissions or use an existing catalog")
            raise

except Exception as e:
    print(f"❌ Error checking/creating catalog: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schema

# COMMAND ----------

print(f"🔍 Checking if schema exists: {catalog_name}.{schema_name}")

# Check if schema exists
try:
    schemas = [s.databaseName for s in spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()]
    schema_exists = schema_name in schemas

    if schema_exists:
        print(f"✅ Schema already exists: {catalog_name}.{schema_name}")
    else:
        print(f"🏗️  Schema not found, creating: {catalog_name}.{schema_name}")
        spark.sql(f"""
            CREATE SCHEMA {catalog_name}.{schema_name}
            COMMENT 'HEDIS pipeline tables and volumes'
        """)
        print(f"✅ Schema created: {catalog_name}.{schema_name}")

except Exception as e:
    print(f"❌ Error checking/creating schema: {str(e)}")
    raise

# Set default catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Set default catalog/schema: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume for HEDIS PDFs

# COMMAND ----------

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
print(f"🔍 Checking if volume exists: {catalog_name}.{schema_name}.{volume_name}")

# Check if volume exists by trying to list it
try:
    # Try to list the volume path - if it succeeds, volume exists
    dbutils.fs.ls(volume_path)
    print(f"✅ Volume already exists: {volume_path}")

except Exception:
    # Volume doesn't exist, create it
    print(f"🏗️  Volume not found, creating: {catalog_name}.{schema_name}.{volume_name}")
    try:
        spark.sql(f"""
            CREATE VOLUME {catalog_name}.{schema_name}.{volume_name}
            COMMENT 'Storage for HEDIS PDF documents'
        """)
        print(f"✅ Volume created: {volume_path}")

        # Display volume info
        volume_info = spark.sql(f"DESCRIBE VOLUME {catalog_name}.{schema_name}.{volume_name}").collect()
        print("\n📁 Volume Details:")
        for row in volume_info:
            print(f"   {row.col_name}: {row.data_type}")

    except Exception as e:
        print(f"❌ Volume creation failed: {str(e)}")
        raise

# Verify volume is accessible and show file count
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

print(f"🔍 Checking if Vector Search endpoint exists: {vector_endpoint_name}")

# Check if endpoint already exists
try:
    existing_endpoint = vsc.get_endpoint(vector_endpoint_name)
    endpoint_state = existing_endpoint.get('endpoint_status', {}).get('state', 'Unknown')
    print(f"✅ Endpoint already exists: {vector_endpoint_name}")
    print(f"   Status: {endpoint_state}")

    if endpoint_state != "ONLINE":
        print(f"⚠️  Endpoint is not online yet. Current state: {endpoint_state}")
        print("   It may still be provisioning. Check back in a few minutes.")

except Exception:
    # Endpoint doesn't exist, create it
    print(f"🏗️  Endpoint not found, creating: {vector_endpoint_name}")
    print("   ⏳ This may take 5-10 minutes...")

    try:
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

                elapsed = int(time.time() - start_time)
                print(f"   Status: {state} (elapsed: {elapsed}s)")

                if state == "ONLINE":
                    print(f"✅ Endpoint is online: {vector_endpoint_name}")
                    break
                elif state in ["OFFLINE", "PROVISIONING"]:
                    time.sleep(30)  # Check every 30 seconds
                else:
                    print(f"⚠️  Unexpected state: {state}")
                    break
            except Exception as e:
                elapsed = int(time.time() - start_time)
                print(f"   Waiting... ({elapsed}s)")
                time.sleep(30)
        else:
            print("⚠️  Endpoint creation timed out (10 minutes), but it may still be provisioning")
            print(f"   Check status with: vsc.get_endpoint('{vector_endpoint_name}')")

    except Exception as e:
        print(f"❌ Vector Search endpoint creation failed: {str(e)}")
        print("   You can create it manually later or skip vector search integration")
        print("   Continuing with setup...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.5: Create Lakebase PostgreSQL Instance (Optional)
# MAGIC
# MAGIC **Optional**: Create a Lakebase PostgreSQL database instance for conversation history persistence.
# MAGIC This is required if you want to enable stateful conversations with the HEDIS agent.
# MAGIC
# MAGIC **Note**: Instance creation may take 5-10 minutes. The instance needs to be in "ONLINE" state before use.

# COMMAND ----------

if create_lakebase:
    print(f"🔍 Checking if Lakebase instance exists: {lakebase_instance_name}")

    # Check if instance already exists
    try:
        existing_instance = w.database.get_database_instance(name=lakebase_instance_name)
        instance_state = existing_instance.state.value if existing_instance.state else 'Unknown'
        print(f"✅ Instance already exists: {lakebase_instance_name}")
        print(f"   Status: {instance_state}")

        if instance_state != "ONLINE":
            print(f"⚠️  Instance is not online yet. Current state: {instance_state}")
            print("   It may still be provisioning. Check back in a few minutes.")
        else:
            # Instance is online, try to setup checkpointer tables
            print("\n🔧 Setting up PostgresSaver checkpointer tables...")
            try:
                import sys
                sys.path.append("../src")

                from database.lakebase import LakebaseDatabase
                import os

                # Get credentials
                host = w.config.host
                client_id = os.getenv("DATABRICKS_CLIENT_ID")

                if not client_id:
                    print("⚠️  DATABRICKS_CLIENT_ID not set. Skipping checkpointer setup.")
                    print("   Set this environment variable to enable checkpointer table creation.")
                else:
                    # Initialize Lakebase connection
                    lakebase_db = LakebaseDatabase(host=host)

                    # Initialize connection with checkpointer setup
                    conn_string = lakebase_db.initialize_connection(
                        user=client_id,
                        instance_name=lakebase_instance_name,
                        database="databricks_postgres",
                        setup_checkpointer=True
                    )

                    print(f"✅ PostgresSaver tables created successfully!")
                    print(f"   Instance: {lakebase_instance_name}")
                    print(f"   Database: databricks_postgres")

            except Exception as e:
                if "already exists" in str(e).lower() or "relation" in str(e).lower():
                    print(f"✅ PostgresSaver tables already exist")
                else:
                    print(f"⚠️  Could not setup checkpointer tables: {str(e)}")
                    print("   You can setup tables manually later if needed")

    except Exception:
        # Instance doesn't exist, create it
        print(f"🏗️  Instance not found, creating: {lakebase_instance_name}")
        print("   ⏳ This may take 5-10 minutes...")

        try:
            from databricks.sdk.service.database import CreateDatabaseInstanceRequest

            # Create the database instance
            w.database.create_database_instance(
                request=CreateDatabaseInstanceRequest(
                    name=lakebase_instance_name,
                    instance_type="POSTGRESQL"
                )
            )

            print(f"✅ Instance creation initiated: {lakebase_instance_name}")
            print("   Waiting for instance to come online...")

            # Wait for instance to be ready
            max_wait = 600  # 10 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                try:
                    instance = w.database.get_database_instance(name=lakebase_instance_name)
                    state = instance.state.value if instance.state else 'Unknown'

                    elapsed = int(time.time() - start_time)
                    print(f"   Status: {state} (elapsed: {elapsed}s)")

                    if state == "ONLINE":
                        print(f"✅ Instance is online: {lakebase_instance_name}")

                        # Setup checkpointer tables
                        print("\n🔧 Setting up PostgresSaver checkpointer tables...")
                        try:
                            import sys
                            sys.path.append("../src")

                            from database.lakebase import LakebaseDatabase
                            import os

                            # Get credentials
                            host = w.config.host
                            client_id = os.getenv("DATABRICKS_CLIENT_ID")

                            if not client_id:
                                print("⚠️  DATABRICKS_CLIENT_ID not set. Skipping checkpointer setup.")
                                print("   Set this environment variable to enable checkpointer table creation.")
                            else:
                                # Initialize Lakebase connection
                                lakebase_db = LakebaseDatabase(host=host)

                                # Initialize connection with checkpointer setup
                                conn_string = lakebase_db.initialize_connection(
                                    user=client_id,
                                    instance_name=lakebase_instance_name,
                                    database="databricks_postgres",
                                    setup_checkpointer=True
                                )

                                print(f"✅ PostgresSaver tables created successfully!")
                                print(f"   Instance: {lakebase_instance_name}")
                                print(f"   Database: databricks_postgres")

                        except Exception as e:
                            print(f"⚠️  Could not setup checkpointer tables: {str(e)}")
                            print("   You can setup tables manually later if needed")

                        break
                    elif state in ["OFFLINE", "PROVISIONING"]:
                        time.sleep(30)  # Check every 30 seconds
                    else:
                        print(f"⚠️  Unexpected state: {state}")
                        break
                except Exception as e:
                    elapsed = int(time.time() - start_time)
                    print(f"   Waiting... ({elapsed}s)")
                    time.sleep(30)
            else:
                print("⚠️  Instance creation timed out (10 minutes), but it may still be provisioning")
                print(f"   Check status with: w.database.get_database_instance('{lakebase_instance_name}')")

        except Exception as e:
            print(f"❌ Lakebase instance creation failed: {str(e)}")
            print("   You can create it manually later or skip persistence features")
            print("   Continuing with setup...")
            raise
else:
    print("⏭️  Lakebase instance creation skipped (create_lakebase = No)")
    print("   To enable persistence features, set 'Create Lakebase Instance' widget to 'Yes'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Delta Tables
# MAGIC
# MAGIC Creates three tables:
# MAGIC 1. Bronze: `hedis_file_metadata`
# MAGIC 2. Silver 1: `hedis_measures_definitions`
# MAGIC 3. Silver 2: `hedis_measures_chunks`

# COMMAND ----------

print("🔍 Checking and creating Delta tables...")

# Get existing tables
existing_tables = [t.tableName for t in spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()]

# Table 1: Bronze - File Metadata
table_name = "hedis_file_metadata"
print(f"\n1️⃣  Checking bronze table: {table_name}")

if table_name in existing_tables:
    print(f"   ✅ Table already exists: {catalog_name}.{schema_name}.{table_name}")
    # Enable CDF if not already enabled
    try:
        spark.sql(f"""
            ALTER TABLE {catalog_name}.{schema_name}.{table_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        print(f"   📝 Change Data Feed enabled for incremental processing")
    except Exception as e:
        if "already set" not in str(e).lower():
            print(f"   ⚠️  Could not enable CDF: {str(e)}")
else:
    print(f"   🏗️  Creating table: {catalog_name}.{schema_name}.{table_name}")
    spark.sql(f"""
        CREATE TABLE {catalog_name}.{schema_name}.{table_name} (
            file_id STRING NOT NULL COMMENT 'Unique file identifier (UUID)',
            file_name STRING NOT NULL COMMENT 'Original filename',
            file_path STRING NOT NULL COMMENT 'Full volume path',
            volume_ingestion_date TIMESTAMP COMMENT 'When uploaded to volume',
            published_date STRING COMMENT 'Extracted from filename',
            effective_year INT COMMENT 'HEDIS year (e.g., 2025)',
            file_size_bytes LONG COMMENT 'File size in bytes',
            page_count INT COMMENT 'Total pages in PDF',
            ingestion_timestamp TIMESTAMP COMMENT 'Pipeline ingestion time',
            last_modified TIMESTAMP COMMENT 'Last update timestamp',
            checksum STRING COMMENT 'SHA256 hash for deduplication'
        )
        USING DELTA
        COMMENT 'Bronze layer: HEDIS file metadata catalog'
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"   ✅ Table created: {table_name}")
    print(f"   📝 Change Data Feed enabled for incremental processing")

# Table 2: Silver - Measure Definitions
table_name = "hedis_measures_definitions"
print(f"\n2️⃣  Checking silver table: {table_name}")

if table_name in existing_tables:
    print(f"   ✅ Table already exists: {catalog_name}.{schema_name}.{table_name}")
else:
    print(f"   🏗️  Creating table: {catalog_name}.{schema_name}.{table_name}")
    spark.sql(f"""
        CREATE TABLE {catalog_name}.{schema_name}.{table_name} (
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
    print(f"   ✅ Table created: {table_name}")

# Table 3: Silver - Chunks for Vector Search
table_name = "hedis_measures_chunks"
print(f"\n3️⃣  Checking silver table: {table_name}")

if table_name in existing_tables:
    print(f"   ✅ Table already exists: {catalog_name}.{schema_name}.{table_name}")
else:
    print(f"   🏗️  Creating table: {catalog_name}.{schema_name}.{table_name}")
    spark.sql(f"""
        CREATE TABLE {catalog_name}.{schema_name}.{table_name} (
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
    print(f"   ✅ Table created: {table_name}")

print("\n✅ All Delta tables verified/created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Infrastructure

# COMMAND ----------

print("🔍 Verifying infrastructure setup...")
print("=" * 60)

# Check catalog
try:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
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

# Check Lakebase instance
if create_lakebase:
    try:
        instance = w.database.get_database_instance(name=lakebase_instance_name)
        state = instance.state.value if instance.state else 'Unknown'
        if state == "ONLINE":
            print(f"✅ Lakebase instance online: {lakebase_instance_name}")
        else:
            print(f"⚠️  Lakebase instance exists but not online: {lakebase_instance_name} (Status: {state})")
    except Exception as e:
        print(f"⚠️  Lakebase instance not found: {lakebase_instance_name}")
else:
    print(f"⏭️  Lakebase instance creation was skipped")

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
