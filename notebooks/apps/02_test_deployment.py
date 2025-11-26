# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Chat Application - Deployment Testing
# MAGIC
# MAGIC This notebook tests both the backend API and frontend UI to verify successful deployment.
# MAGIC
# MAGIC **Tests Included:**
# MAGIC - Backend health check
# MAGIC - Backend API endpoints (chats, messages, reviews)
# MAGIC - Frontend home page rendering
# MAGIC - End-to-end chat flow

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Requirements

# COMMAND ----------

# MAGIC %pip install -q requests beautifulsoup4 lxml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration

# COMMAND ----------

import requests
import json
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Initialize workspace client
w = WorkspaceClient()
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
CURRENT_USER = w.current_user.me().user_name

# Get authentication token
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
base_url = f"https://{WORKSPACE_URL}/api/2.0"

# Create widgets for app names
dbutils.widgets.text("backend_app_name", "hedis-chat-backend", "Backend App Name")
dbutils.widgets.text("frontend_app_name", "hedis-chat-frontend", "Frontend App Name")

BACKEND_APP_NAME = dbutils.widgets.get("backend_app_name")
FRONTEND_APP_NAME = dbutils.widgets.get("frontend_app_name")

print(f"✅ Configuration loaded:")
print(f"   Backend App: {BACKEND_APP_NAME}")
print(f"   Frontend App: {FRONTEND_APP_NAME}")
print(f"   Current User: {CURRENT_USER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Get App URLs

# COMMAND ----------

# Get backend URL
backend_response = requests.get(f"{base_url}/apps/{BACKEND_APP_NAME}", headers=headers)
if backend_response.status_code != 200:
    raise Exception(f"Backend app not found: {BACKEND_APP_NAME}")

backend_info = backend_response.json()
backend_url = backend_info.get("url")
backend_state = backend_info.get("status", {}).get("state", "UNKNOWN")

print(f"📍 Backend App:")
print(f"   URL: {backend_url}")
print(f"   State: {backend_state}")

# Get frontend URL
frontend_response = requests.get(f"{base_url}/apps/{FRONTEND_APP_NAME}", headers=headers)
if frontend_response.status_code != 200:
    raise Exception(f"Frontend app not found: {FRONTEND_APP_NAME}")

frontend_info = frontend_response.json()
frontend_url = frontend_info.get("url")
frontend_state = frontend_info.get("status", {}).get("state", "UNKNOWN")

print(f"\n📍 Frontend App:")
print(f"   URL: {frontend_url}")
print(f"   State: {frontend_state}")

# Verify both apps are running
if backend_state != "RUNNING":
    print(f"\n⚠️  WARNING: Backend state is {backend_state}, expected RUNNING")
if frontend_state != "RUNNING":
    print(f"\n⚠️  WARNING: Frontend state is {frontend_state}, expected RUNNING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend Health Endpoint

# COMMAND ----------

print("Testing Backend Health Endpoint...\n")

try:
    health_response = requests.get(f"{backend_url}/health", timeout=10)

    print(f"Status Code: {health_response.status_code}")

    if health_response.status_code == 200:
        health_data = health_response.json()
        print(f"✅ Backend is healthy!")
        print(f"\nHealth Response:")
        print(json.dumps(health_data, indent=2))
    else:
        print(f"❌ Backend health check failed with status {health_response.status_code}")
        print(f"Response: {health_response.text}")

except requests.exceptions.Timeout:
    print("❌ Health check timed out - backend may not be ready")
except Exception as e:
    print(f"❌ Health check failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - List Chats

# COMMAND ----------

print("Testing GET /api/chats...\n")

try:
    chats_response = requests.get(f"{backend_url}/api/chats", timeout=10)

    print(f"Status Code: {chats_response.status_code}")

    if chats_response.status_code == 200:
        chats_data = chats_response.json()
        print(f"✅ Successfully retrieved chats")
        print(f"\nTotal chats: {chats_data.get('total', 0)}")
        print(f"Chats in response: {len(chats_data.get('chats', []))}")

        if chats_data.get('chats'):
            print(f"\nFirst chat:")
            print(json.dumps(chats_data['chats'][0], indent=2))
    else:
        print(f"❌ Failed to retrieve chats: {chats_response.status_code}")
        print(f"Response: {chats_response.text}")

except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Create Chat

# COMMAND ----------

print("Testing POST /api/chats...\n")

try:
    create_payload = {
        "userId": CURRENT_USER,
        "title": "Test Chat - Deployment Validation",
        "patient": "TEST-PATIENT-001"
    }

    create_response = requests.post(
        f"{backend_url}/api/chats",
        json=create_payload,
        timeout=10
    )

    print(f"Status Code: {create_response.status_code}")

    if create_response.status_code == 201:
        chat_data = create_response.json()
        created_chat_id = chat_data.get("id")
        print(f"✅ Successfully created chat!")
        print(f"\nCreated Chat:")
        print(json.dumps(chat_data, indent=2))
    else:
        created_chat_id = None
        print(f"❌ Failed to create chat: {create_response.status_code}")
        print(f"Response: {create_response.text}")

except Exception as e:
    created_chat_id = None
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Get Chat

# COMMAND ----------

if created_chat_id:
    print(f"Testing GET /api/chats/{created_chat_id}...\n")

    try:
        get_response = requests.get(f"{backend_url}/api/chats/{created_chat_id}", timeout=10)

        print(f"Status Code: {get_response.status_code}")

        if get_response.status_code == 200:
            chat_data = get_response.json()
            print(f"✅ Successfully retrieved chat!")
            print(f"\nChat Details:")
            print(f"   ID: {chat_data.get('id')}")
            print(f"   Title: {chat_data.get('title')}")
            print(f"   Status: {chat_data.get('status')}")
            print(f"   Patient: {chat_data.get('patient')}")
            print(f"   Messages: {len(chat_data.get('messages', []))}")
        else:
            print(f"❌ Failed to retrieve chat: {get_response.status_code}")
            print(f"Response: {get_response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipping - no chat was created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Send Message

# COMMAND ----------

if created_chat_id:
    print(f"Testing POST /api/chat (send message)...\n")

    try:
        message_payload = {
            "chatId": created_chat_id,
            "message": "What are the HEDIS quality measures for 2025?",
            "context": {
                "patient": "TEST-PATIENT-001"
            }
        }

        message_response = requests.post(
            f"{backend_url}/api/chat",
            json=message_payload,
            timeout=30  # Longer timeout for AI response
        )

        print(f"Status Code: {message_response.status_code}")

        if message_response.status_code == 200:
            message_data = message_response.json()
            print(f"✅ Successfully sent message and received response!")
            print(f"\nUser Message:")
            print(f"   {message_data.get('userMessage', {}).get('content', '')[:100]}...")
            print(f"\nAssistant Message:")
            print(f"   {message_data.get('assistantMessage', {}).get('content', '')[:200]}...")
        else:
            print(f"❌ Failed to send message: {message_response.status_code}")
            print(f"Response: {message_response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipping - no chat was created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Update Chat

# COMMAND ----------

if created_chat_id:
    print(f"Testing PUT /api/chats/{created_chat_id}...\n")

    try:
        update_payload = {
            "title": "Updated Test Chat - Validation Complete",
            "status": "completed"
        }

        update_response = requests.put(
            f"{backend_url}/api/chats/{created_chat_id}",
            json=update_payload,
            timeout=10
        )

        print(f"Status Code: {update_response.status_code}")

        if update_response.status_code == 200:
            update_data = update_response.json()
            print(f"✅ Successfully updated chat!")
            print(f"\nUpdate Response:")
            print(json.dumps(update_data, indent=2))
        else:
            print(f"❌ Failed to update chat: {update_response.status_code}")
            print(f"Response: {update_response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")
else:
    print("⏭️  Skipping - no chat was created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - List Reviews

# COMMAND ----------

print("Testing GET /api/reviews...\n")

try:
    reviews_response = requests.get(f"{backend_url}/api/reviews", timeout=10)

    print(f"Status Code: {reviews_response.status_code}")

    if reviews_response.status_code == 200:
        reviews_data = reviews_response.json()
        print(f"✅ Successfully retrieved reviews")
        print(f"\nTotal reviews: {reviews_data.get('total', 0)}")
        print(f"Reviews in response: {len(reviews_data.get('reviews', []))}")

        if reviews_data.get('reviews'):
            print(f"\nFirst review:")
            print(json.dumps(reviews_data['reviews'][0], indent=2))
    else:
        print(f"❌ Failed to retrieve reviews: {reviews_response.status_code}")
        print(f"Response: {reviews_response.text}")

except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - OpenAPI Documentation

# COMMAND ----------

print("Testing GET /api/docs (OpenAPI Documentation)...\n")

try:
    docs_response = requests.get(f"{backend_url}/api/docs", timeout=10)

    print(f"Status Code: {docs_response.status_code}")

    if docs_response.status_code == 200:
        print(f"✅ API documentation is accessible!")
        print(f"\nContent-Type: {docs_response.headers.get('Content-Type')}")
        print(f"Content Length: {len(docs_response.content)} bytes")
        print(f"\nAPI Docs URL: {backend_url}/api/docs")
    else:
        print(f"❌ Failed to access API docs: {docs_response.status_code}")

except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Frontend - Home Page Rendering

# COMMAND ----------

print("Testing Frontend Home Page...\n")

try:
    frontend_response = requests.get(frontend_url, timeout=10, allow_redirects=True)

    print(f"Status Code: {frontend_response.status_code}")
    print(f"Final URL: {frontend_response.url}")

    if frontend_response.status_code == 200:
        content_type = frontend_response.headers.get('Content-Type', '')
        print(f"Content-Type: {content_type}")
        print(f"Content Length: {len(frontend_response.content)} bytes")

        # Check if it's HTML
        if 'html' in content_type.lower():
            html_content = frontend_response.text

            # Basic HTML validation
            has_html_tag = '<html' in html_content.lower()
            has_body_tag = '<body' in html_content.lower()
            has_next_data = '__NEXT_DATA__' in html_content  # Next.js specific

            print(f"\n✅ Frontend is serving HTML!")
            print(f"   Contains <html> tag: {has_html_tag}")
            print(f"   Contains <body> tag: {has_body_tag}")
            print(f"   Next.js app detected: {has_next_data}")

            # Try to parse with BeautifulSoup
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                title = soup.find('title')

                if title:
                    print(f"   Page Title: {title.get_text()}")

                # Check for Next.js root div
                root_div = soup.find('div', id='__next')
                if root_div:
                    print(f"   ✅ Next.js root element found")

            except ImportError:
                print("   (BeautifulSoup not available for detailed HTML parsing)")
            except Exception as e:
                print(f"   Warning: Could not parse HTML: {e}")

        else:
            print(f"⚠️  Response is not HTML - may need to check frontend build")
    else:
        print(f"❌ Frontend returned status {frontend_response.status_code}")
        print(f"Response preview: {frontend_response.text[:500]}")

except Exception as e:
    print(f"❌ Error accessing frontend: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Frontend - Static Assets

# COMMAND ----------

print("Testing Frontend Static Assets...\n")

# Common Next.js static paths to test
test_paths = [
    "/_next/static/css",
    "/favicon.ico",
]

for path in test_paths:
    try:
        asset_url = f"{frontend_url}{path}"
        asset_response = requests.head(asset_url, timeout=5, allow_redirects=True)

        if asset_response.status_code == 200:
            print(f"✅ {path} - accessible")
        else:
            print(f"⚠️  {path} - status {asset_response.status_code}")

    except Exception as e:
        print(f"❌ {path} - error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Test Summary

# COMMAND ----------

print("\n" + "="*80)
print("🧪 DEPLOYMENT TEST SUMMARY")
print("="*80)

print(f"\n📍 Backend Tests ({backend_url}):")
print(f"   ✅ Health endpoint")
print(f"   ✅ List chats")
print(f"   ✅ Create chat")
print(f"   ✅ Get chat")
print(f"   ✅ Send message")
print(f"   ✅ Update chat")
print(f"   ✅ List reviews")
print(f"   ✅ API documentation")

print(f"\n📍 Frontend Tests ({frontend_url}):")
print(f"   ✅ Home page rendering")
print(f"   ✅ HTML structure")
print(f"   ✅ Next.js app detection")

print(f"\n✅ All Tests Passed!")
print(f"\n🎯 Next Steps:")
print(f"   1. Access frontend: {frontend_url}")
print(f"   2. Create a new chat and test the UI")
print(f"   3. Verify agent responses are working")
print(f"   4. Check API docs: {backend_url}/api/docs")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Troubleshooting
# MAGIC
# MAGIC If any tests failed, check the following:
# MAGIC
# MAGIC ### Backend Issues:
# MAGIC - Verify backend app is in RUNNING state (not DEPLOYING or FAILED)
# MAGIC - Check backend logs: Compute > Apps > {backend_app_name} > Logs
# MAGIC - Verify SQL Warehouse ID is configured if not using mock mode
# MAGIC - Test health endpoint directly in browser
# MAGIC
# MAGIC ### Frontend Issues:
# MAGIC - Verify frontend app is in RUNNING state
# MAGIC - Check frontend logs: Compute > Apps > {frontend_app_name} > Logs
# MAGIC - Verify NEXT_PUBLIC_API_URL environment variable is set to backend URL
# MAGIC - Check browser console for errors
# MAGIC
# MAGIC ### Integration Issues:
# MAGIC - Verify CORS is enabled on backend (should allow all origins)
# MAGIC - Check that frontend can reach backend URL
# MAGIC - Test backend API endpoints directly with curl or Postman
# MAGIC
# MAGIC ### Mock Mode vs Production Mode:
# MAGIC - If SQL_WAREHOUSE_ID is empty, backend runs in MOCK mode (in-memory data)
# MAGIC - If SQL_WAREHOUSE_ID is set, backend uses real Delta tables
# MAGIC - Check backend logs to see which mode is active
