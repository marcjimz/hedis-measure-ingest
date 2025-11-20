# Databricks notebook source
# MAGIC %md
# MAGIC # Infrastructure Setup for HEDIS Measure Ingestion Pipeline
# MAGIC
# MAGIC This notebook creates all necessary infrastructure for the HEDIS pipeline:
# MAGIC - Unity Catalog (Catalog + Schema)
# MAGIC - Volume for storing HEDIS PDFs
# MAGIC - Vector Search Endpoint
# MAGIC - Lakebase PostgreSQL Instance (Optional)
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
# MAGIC
# MAGIC **Note**: Delta tables are created automatically by the pipeline notebooks when they run.

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

import yaml

# Load configuration from config.yaml
try:
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    # Fallback for different execution contexts
    with open("/Workspace/Repos/hedis-measure-ingest/notebooks/config.yaml", "r") as f:
        config = yaml.safe_load(f)

# Configuration widgets with config values as defaults
dbutils.widgets.text("catalog_name", config.get("catalog_name", "main"), "Catalog Name")
dbutils.widgets.text("schema_name", config.get("schema_name", "hedis_measurements"), "Schema Name")
dbutils.widgets.text("volume_name", config.get("volume_name", "hedis"), "Volume Name")
dbutils.widgets.text("vector_search_endpoint", config.get("vector_search_endpoint", "hedis_vector_endpoint"), "Vector Search Endpoint")
dbutils.widgets.dropdown("create_lakebase", config.get("create_lakebase", "Yes"), ["Yes", "No"], "Create Lakebase Instance")
dbutils.widgets.text("lakebase_instance_name", config.get("lakebase_instance", "hedis-agent-pg"), "Lakebase Instance Name")

# Get parameters (widgets override config if changed)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment

# COMMAND ----------

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

        import time
        time.sleep(5)

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
        # Convert enum to string for comparison
        instance_state = str(existing_instance.state).split('.')[-1] if existing_instance.state else 'UNKNOWN'
        print(f"✅ Instance already exists: {lakebase_instance_name}")
        print(f"   Status: {instance_state}")

        if instance_state != "AVAILABLE":
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

                # Use passthrough authentication
                lakebase_db = LakebaseDatabase()  # Uses default authentication

                # Initialize connection with checkpointer setup
                conn_string = lakebase_db.initialize_connection(
                    user=w.current_user.me().user_name,
                    instance_name=lakebase_instance_name,
                    database="databricks_postgres",
                    setup_checkpointer=True
                )

                print(f"✅ PostgresSaver tables created successfully!")
                print(f"   Instance: {lakebase_instance_name}")
                print(f"   Database: databricks_postgres")
                print(f"   User: {w.current_user.me().user_name}")

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
            from databricks.sdk.service.database import DatabaseInstance

            # Create the database instance
            w.database.create_database_instance(
                DatabaseInstance(
                    name=lakebase_instance_name,
                    capacity="CU_4"  # 4 Compute Units
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
                    # Convert enum to string for comparison
                    state_str = str(instance.state).split('.')[-1] if instance.state else 'UNKNOWN'

                    elapsed = int(time.time() - start_time)
                    print(f"   Status: {state_str} (elapsed: {elapsed}s)")

                    if state_str == "AVAILABLE":
                        print(f"✅ Instance is online: {lakebase_instance_name}")

                        # Setup checkpointer tables
                        print("\n🔧 Setting up PostgresSaver checkpointer tables...")
                        try:
                            import sys
                            sys.path.append("../src")

                            from database.lakebase import LakebaseDatabase

                            # Use passthrough authentication
                            lakebase_db = LakebaseDatabase()  # Uses default authentication

                            # Initialize connection with checkpointer setup
                            conn_string = lakebase_db.initialize_connection(
                                user=w.current_user.me().user_name,
                                instance_name=lakebase_instance_name,
                                database="databricks_postgres",
                                setup_checkpointer=True
                            )

                            print(f"✅ PostgresSaver tables created successfully!")
                            print(f"   Instance: {lakebase_instance_name}")
                            print(f"   Database: databricks_postgres")
                            print(f"   User: {w.current_user.me().user_name}")

                        except Exception as e:
                            print(f"⚠️  Could not setup checkpointer tables: {str(e)}")
                            print("   You can setup tables manually later if needed")

                        break
                    elif state_str in ["OFFLINE", "PROVISIONING", "CREATING"]:
                        time.sleep(30)  # Check every 30 seconds
                    else:
                        print(f"⚠️  Unexpected state: {state_str}")
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
# MAGIC ## Step 4.6: Register Lakebase with Unity Catalog (Optional)
# MAGIC
# MAGIC **Optional**: Register the Lakebase instance with Unity Catalog for unified governance.
# MAGIC This allows SQL access to Lakebase data and integration with Delta Lake features.
# MAGIC
# MAGIC **Note**: This step is idempotent - safe to run multiple times.

# COMMAND ----------

if create_lakebase:
    from databricks.sdk.service.database import DatabaseCatalog

    # UC catalog names can't have hyphens - replace with underscores
    uc_catalog_name = f"{lakebase_instance_name.replace('-', '_')}_pg"
    database_name = "databricks_postgres"  # Default Lakebase database name

    print(f"🔍 Checking if Unity Catalog '{uc_catalog_name}' is registered...")

    try:
        # Check if catalog already exists
        existing_catalog = w.catalogs.get(name=uc_catalog_name)
        print(f"✅ Unity Catalog '{uc_catalog_name}' already exists - skipping creation")
        print(f"   Full Name: {existing_catalog.full_name}")
        print(f"   Owner: {existing_catalog.owner}")
        catalog_exists = True

    except Exception:
        # Catalog doesn't exist, proceed with creation
        print(f"🏗️  Catalog not found - creating Unity Catalog integration...")
        catalog_exists = False

    # Create catalog if it doesn't exist
    if not catalog_exists:
        try:
            catalog = w.database.create_database_catalog(
                DatabaseCatalog(
                    name=uc_catalog_name,
                    database_instance_name=lakebase_instance_name,
                    database_name=database_name
                )
            )

            print(f"✅ Successfully created Unity Catalog integration!")
            print(f"\n📊 Catalog Details:")
            print(f"   Catalog Name: {catalog.name}")
            print(f"   Database Instance: {lakebase_instance_name}")
            print(f"   Postgres Database: {database_name}")
            print(f"\n💡 Benefits:")
            print(f"   • Read-only SQL access to Lakebase data")
            print(f"   • Unified governance across lakehouse and OLTP data")
            print(f"   • Automatic schema discovery and metadata sync")
            print(f"   • Integration with feature serving and synced tables")

        except Exception as e:
            print(f"⚠️  Failed to create Unity Catalog integration: {str(e)}")
            print(f"   You may need additional permissions or the instance may still be provisioning")
            print(f"   Continuing with setup...")

    # Verify the catalog configuration
    print(f"\n🔍 Verifying Unity Catalog integration...")

    try:
        # Confirm catalog exists in Unity Catalog
        catalog_info = w.catalogs.get(name=uc_catalog_name)
        print(f"✅ Verified catalog exists in Unity Catalog")
        print(f"   Full Name: {catalog_info.full_name}")
        print(f"   Owner: {catalog_info.owner}")

        # List schemas in the catalog
        print(f"\n📁 Schemas in catalog '{uc_catalog_name}':")
        try:
            schemas = list(w.schemas.list(catalog_name=uc_catalog_name))
            if schemas:
                for schema in schemas:
                    print(f"   • {schema.name}")
            else:
                print(f"   (No schemas found - may need to create tables in Postgres first)")
        except Exception as schema_error:
            print(f"   ⚠️  Could not list schemas: {schema_error}")

    except Exception as verify_error:
        print(f"⚠️  Could not verify catalog: {verify_error}")
        print(f"   The catalog may still be provisioning...")

    print(f"\n✅ Unity Catalog registration complete!")
else:
    print("⏭️  Unity Catalog registration skipped (Lakebase not created)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Infrastructure

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
        state_str = str(instance.state).split('.')[-1] if instance.state else 'UNKNOWN'
        if state_str == "AVAILABLE":
            print(f"✅ Lakebase instance online: {lakebase_instance_name}")
        else:
            print(f"⚠️  Lakebase instance exists but not online: {lakebase_instance_name} (Status: {state_str})")
    except Exception as e:
        print(f"⚠️  Lakebase instance not found: {lakebase_instance_name}")
else:
    print(f"⏭️  Lakebase instance creation was skipped")

print("=" * 60)
print("\n✅ Infrastructure setup complete!")
print("\nNext steps:")
print("1. Upload HEDIS PDF files to the volume")
print("2. Run the E2E pipeline job to process files and deploy the agent")

# COMMAND ----------

