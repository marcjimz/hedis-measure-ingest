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
# MAGIC
# MAGIC **Failure Handling:**
# MAGIC - Raises exceptions on test failures
# MAGIC - Shows full response details for debugging
# MAGIC - Validates app state correctly

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
dbutils.widgets.dropdown("debug_mode", "No", ["Yes", "No"], "Debug Mode (Show Full Responses)")

BACKEND_APP_NAME = dbutils.widgets.get("backend_app_name")
FRONTEND_APP_NAME = dbutils.widgets.get("frontend_app_name")
DEBUG_MODE = dbutils.widgets.get("debug_mode") == "Yes"

print(f"✅ Configuration loaded:")
print(f"   Backend App: {BACKEND_APP_NAME}")
print(f"   Frontend App: {FRONTEND_APP_NAME}")
print(f"   Debug Mode: {DEBUG_MODE}")
print(f"   Current User: {CURRENT_USER}")

# Test tracking
test_results = []

def log_test(test_name, passed, details=None, error=None):
    """Log test result and optionally show debug info."""
    result = {
        "test": test_name,
        "passed": passed,
        "details": details,
        "error": error,
        "timestamp": datetime.utcnow().isoformat()
    }
    test_results.append(result)

    if passed:
        print(f"✅ {test_name}")
    else:
        print(f"❌ {test_name}")
        if error:
            print(f"   Error: {error}")
        if details and DEBUG_MODE:
            print(f"   Debug Details:")
            print(f"   {json.dumps(details, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Get App URLs and Verify State

# COMMAND ----------

print("="*80)
print("VERIFYING APP DEPLOYMENTS")
print("="*80)

# Get backend app info
print(f"\n📍 Checking Backend App: {BACKEND_APP_NAME}")
backend_response = requests.get(f"{base_url}/apps/{BACKEND_APP_NAME}", headers=headers)

if backend_response.status_code != 200:
    print(f"\n❌ FAILED: Backend app not found")
    print(f"Status Code: {backend_response.status_code}")
    print(f"Response: {backend_response.text}")
    raise Exception(f"Backend app '{BACKEND_APP_NAME}' not found. Status: {backend_response.status_code}")

backend_info = backend_response.json()

# Debug: Show full response structure in debug mode
if DEBUG_MODE:
    print(f"\nDEBUG - Full Backend App Response:")
    print(json.dumps(backend_info, indent=2))

# Extract backend info - correct paths based on API structure
backend_url = backend_info.get("url")
backend_status_obj = backend_info.get("status", {})
backend_state = backend_status_obj.get("state") if backend_status_obj else None
backend_message = backend_status_obj.get("message", "") if backend_status_obj else ""

# Validate we got the state
if not backend_state:
    print(f"\n❌ FAILED: Could not retrieve backend state")
    print(f"Status object: {backend_status_obj}")
    print(f"Full app info keys: {list(backend_info.keys())}")
    raise Exception("Backend state not found in app info response")

print(f"   URL: {backend_url}")
print(f"   State: {backend_state}")
if backend_message:
    print(f"   Message: {backend_message}")

# Verify backend is running
if backend_state != "RUNNING":
    error_msg = f"Backend state is '{backend_state}', expected 'RUNNING'"
    print(f"\n❌ FAILED: {error_msg}")
    if backend_message:
        print(f"   Message: {backend_message}")
    raise Exception(error_msg)

print(f"   ✅ Backend is RUNNING")

# Get frontend app info
print(f"\n📍 Checking Frontend App: {FRONTEND_APP_NAME}")
frontend_response = requests.get(f"{base_url}/apps/{FRONTEND_APP_NAME}", headers=headers)

if frontend_response.status_code != 200:
    print(f"\n❌ FAILED: Frontend app not found")
    print(f"Status Code: {frontend_response.status_code}")
    print(f"Response: {frontend_response.text}")
    raise Exception(f"Frontend app '{FRONTEND_APP_NAME}' not found. Status: {frontend_response.status_code}")

frontend_info = frontend_response.json()

# Debug: Show full response structure in debug mode
if DEBUG_MODE:
    print(f"\nDEBUG - Full Frontend App Response:")
    print(json.dumps(frontend_info, indent=2))

# Extract frontend info
frontend_url = frontend_info.get("url")
frontend_status_obj = frontend_info.get("status", {})
frontend_state = frontend_status_obj.get("state") if frontend_status_obj else None
frontend_message = frontend_status_obj.get("message", "") if frontend_status_obj else ""

# Validate we got the state
if not frontend_state:
    print(f"\n❌ FAILED: Could not retrieve frontend state")
    print(f"Status object: {frontend_status_obj}")
    print(f"Full app info keys: {list(frontend_info.keys())}")
    raise Exception("Frontend state not found in app info response")

print(f"   URL: {frontend_url}")
print(f"   State: {frontend_state}")
if frontend_message:
    print(f"   Message: {frontend_message}")

# Verify frontend is running
if frontend_state != "RUNNING":
    error_msg = f"Frontend state is '{frontend_state}', expected 'RUNNING'"
    print(f"\n❌ FAILED: {error_msg}")
    if frontend_message:
        print(f"   Message: {frontend_message}")
    raise Exception(error_msg)

print(f"   ✅ Frontend is RUNNING")

print("\n✅ Both apps are running and accessible\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend Health Endpoint

# COMMAND ----------

print("="*80)
print("TEST: Backend Health Endpoint")
print("="*80)

try:
    health_response = requests.get(f"{backend_url}/health", timeout=10)

    print(f"\nStatus Code: {health_response.status_code}")

    if health_response.status_code == 200:
        health_data = health_response.json()
        print(f"\nHealth Response:")
        print(json.dumps(health_data, indent=2))
        log_test("Backend Health Check", True, health_data)
    else:
        error_details = {
            "status_code": health_response.status_code,
            "response": health_response.text,
            "headers": dict(health_response.headers)
        }
        print(f"\nResponse Body: {health_response.text}")
        print(f"Response Headers: {dict(health_response.headers)}")
        log_test("Backend Health Check", False, error_details, f"Status {health_response.status_code}")
        raise Exception(f"Backend health check failed with status {health_response.status_code}")

except requests.exceptions.Timeout:
    log_test("Backend Health Check", False, None, "Request timed out")
    raise Exception("Backend health check timed out - backend may not be ready")
except requests.exceptions.RequestException as e:
    log_test("Backend Health Check", False, None, str(e))
    raise Exception(f"Backend health check failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - List Chats

# COMMAND ----------

print("="*80)
print("TEST: GET /api/chats")
print("="*80)

try:
    chats_response = requests.get(f"{backend_url}/api/chats", timeout=10)

    print(f"\nStatus Code: {chats_response.status_code}")

    if chats_response.status_code == 200:
        chats_data = chats_response.json()
        print(f"\nTotal chats: {chats_data.get('total', 0)}")
        print(f"Chats in response: {len(chats_data.get('chats', []))}")

        if chats_data.get('chats'):
            print(f"\nFirst chat:")
            print(json.dumps(chats_data['chats'][0], indent=2))

        log_test("List Chats", True, chats_data)
    else:
        error_details = {
            "status_code": chats_response.status_code,
            "response": chats_response.text,
            "headers": dict(chats_response.headers)
        }
        print(f"\nResponse Body: {chats_response.text}")
        log_test("List Chats", False, error_details, f"Status {chats_response.status_code}")
        raise Exception(f"Failed to retrieve chats: {chats_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("List Chats", False, None, str(e))
    raise Exception(f"List chats request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Create Chat

# COMMAND ----------

print("="*80)
print("TEST: POST /api/chats")
print("="*80)

try:
    create_payload = {
        "userId": CURRENT_USER,
        "title": "Test Chat - Deployment Validation",
        "patient": "TEST-PATIENT-001"
    }

    print(f"\nRequest Payload:")
    print(json.dumps(create_payload, indent=2))

    create_response = requests.post(
        f"{backend_url}/api/chats",
        json=create_payload,
        timeout=10
    )

    print(f"\nStatus Code: {create_response.status_code}")

    if create_response.status_code == 201:
        chat_data = create_response.json()
        created_chat_id = chat_data.get("id")
        print(f"\nCreated Chat:")
        print(json.dumps(chat_data, indent=2))
        log_test("Create Chat", True, chat_data)
    else:
        created_chat_id = None
        error_details = {
            "status_code": create_response.status_code,
            "request": create_payload,
            "response": create_response.text,
            "headers": dict(create_response.headers)
        }
        print(f"\nResponse Body: {create_response.text}")
        log_test("Create Chat", False, error_details, f"Status {create_response.status_code}")
        raise Exception(f"Failed to create chat: {create_response.status_code}")

except requests.exceptions.RequestException as e:
    created_chat_id = None
    log_test("Create Chat", False, None, str(e))
    raise Exception(f"Create chat request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Get Chat

# COMMAND ----------

print("="*80)
print(f"TEST: GET /api/chats/{created_chat_id}")
print("="*80)

try:
    get_response = requests.get(f"{backend_url}/api/chats/{created_chat_id}", timeout=10)

    print(f"\nStatus Code: {get_response.status_code}")

    if get_response.status_code == 200:
        chat_data = get_response.json()
        print(f"\nChat Details:")
        print(f"   ID: {chat_data.get('id')}")
        print(f"   Title: {chat_data.get('title')}")
        print(f"   Status: {chat_data.get('status')}")
        print(f"   Patient: {chat_data.get('patient')}")
        print(f"   Messages: {len(chat_data.get('messages', []))}")
        log_test("Get Chat", True, chat_data)
    else:
        error_details = {
            "status_code": get_response.status_code,
            "chat_id": created_chat_id,
            "response": get_response.text,
            "headers": dict(get_response.headers)
        }
        print(f"\nResponse Body: {get_response.text}")
        log_test("Get Chat", False, error_details, f"Status {get_response.status_code}")
        raise Exception(f"Failed to retrieve chat: {get_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("Get Chat", False, None, str(e))
    raise Exception(f"Get chat request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Send Message

# COMMAND ----------

print("="*80)
print("TEST: POST /api/chat (send message)")
print("="*80)

try:
    message_payload = {
        "chatId": created_chat_id,
        "message": "What are the HEDIS quality measures for 2025?",
        "context": {
            "patient": "TEST-PATIENT-001"
        }
    }

    print(f"\nRequest Payload:")
    print(json.dumps(message_payload, indent=2))

    message_response = requests.post(
        f"{backend_url}/api/chat",
        json=message_payload,
        timeout=60  # Longer timeout for AI response
    )

    print(f"\nStatus Code: {message_response.status_code}")

    if message_response.status_code == 200:
        message_data = message_response.json()
        user_content = message_data.get('userMessage', {}).get('content', '')
        assistant_content = message_data.get('assistantMessage', {}).get('content', '')

        print(f"\nUser Message:")
        print(f"   {user_content[:100]}...")
        print(f"\nAssistant Message:")
        print(f"   {assistant_content[:300]}...")

        log_test("Send Message", True, {
            "user_message_length": len(user_content),
            "assistant_message_length": len(assistant_content)
        })
    else:
        error_details = {
            "status_code": message_response.status_code,
            "request": message_payload,
            "response": message_response.text,
            "headers": dict(message_response.headers)
        }
        print(f"\nResponse Body: {message_response.text}")
        log_test("Send Message", False, error_details, f"Status {message_response.status_code}")
        raise Exception(f"Failed to send message: {message_response.status_code}")

except requests.exceptions.Timeout:
    log_test("Send Message", False, None, "Request timed out after 60s")
    raise Exception("Send message timed out - AI agent may not be responding")
except requests.exceptions.RequestException as e:
    log_test("Send Message", False, None, str(e))
    raise Exception(f"Send message request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - Update Chat

# COMMAND ----------

print("="*80)
print(f"TEST: PUT /api/chats/{created_chat_id}")
print("="*80)

try:
    update_payload = {
        "title": "Updated Test Chat - Validation Complete",
        "status": "completed"
    }

    print(f"\nRequest Payload:")
    print(json.dumps(update_payload, indent=2))

    update_response = requests.put(
        f"{backend_url}/api/chats/{created_chat_id}",
        json=update_payload,
        timeout=10
    )

    print(f"\nStatus Code: {update_response.status_code}")

    if update_response.status_code == 200:
        update_data = update_response.json()
        print(f"\nUpdate Response:")
        print(json.dumps(update_data, indent=2))
        log_test("Update Chat", True, update_data)
    else:
        error_details = {
            "status_code": update_response.status_code,
            "request": update_payload,
            "response": update_response.text,
            "headers": dict(update_response.headers)
        }
        print(f"\nResponse Body: {update_response.text}")
        log_test("Update Chat", False, error_details, f"Status {update_response.status_code}")
        raise Exception(f"Failed to update chat: {update_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("Update Chat", False, None, str(e))
    raise Exception(f"Update chat request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - List Reviews

# COMMAND ----------

print("="*80)
print("TEST: GET /api/reviews")
print("="*80)

try:
    reviews_response = requests.get(f"{backend_url}/api/reviews", timeout=10)

    print(f"\nStatus Code: {reviews_response.status_code}")

    if reviews_response.status_code == 200:
        reviews_data = reviews_response.json()
        print(f"\nTotal reviews: {reviews_data.get('total', 0)}")
        print(f"Reviews in response: {len(reviews_data.get('reviews', []))}")

        if reviews_data.get('reviews'):
            print(f"\nFirst review:")
            print(json.dumps(reviews_data['reviews'][0], indent=2))

        log_test("List Reviews", True, reviews_data)
    else:
        error_details = {
            "status_code": reviews_response.status_code,
            "response": reviews_response.text,
            "headers": dict(reviews_response.headers)
        }
        print(f"\nResponse Body: {reviews_response.text}")
        log_test("List Reviews", False, error_details, f"Status {reviews_response.status_code}")
        raise Exception(f"Failed to retrieve reviews: {reviews_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("List Reviews", False, None, str(e))
    raise Exception(f"List reviews request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Backend API - OpenAPI Documentation

# COMMAND ----------

print("="*80)
print("TEST: GET /api/docs (OpenAPI Documentation)")
print("="*80)

try:
    docs_response = requests.get(f"{backend_url}/api/docs", timeout=10)

    print(f"\nStatus Code: {docs_response.status_code}")

    if docs_response.status_code == 200:
        print(f"\nContent-Type: {docs_response.headers.get('Content-Type')}")
        print(f"Content Length: {len(docs_response.content)} bytes")
        print(f"\nAPI Docs URL: {backend_url}/api/docs")
        log_test("API Documentation", True, {
            "content_type": docs_response.headers.get('Content-Type'),
            "content_length": len(docs_response.content)
        })
    else:
        error_details = {
            "status_code": docs_response.status_code,
            "response": docs_response.text[:500],
            "headers": dict(docs_response.headers)
        }
        print(f"\nResponse preview: {docs_response.text[:500]}")
        log_test("API Documentation", False, error_details, f"Status {docs_response.status_code}")
        raise Exception(f"Failed to access API docs: {docs_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("API Documentation", False, None, str(e))
    raise Exception(f"API docs request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Frontend - Home Page Rendering

# COMMAND ----------

print("="*80)
print("TEST: Frontend Home Page")
print("="*80)

try:
    frontend_response = requests.get(frontend_url, timeout=10, allow_redirects=True)

    print(f"\nStatus Code: {frontend_response.status_code}")
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

            print(f"\nHTML Validation:")
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
                    print(f"   Next.js root element: Found")

            except ImportError:
                print("   (BeautifulSoup not available for detailed HTML parsing)")
            except Exception as e:
                print(f"   Warning: Could not parse HTML: {e}")

            if not (has_html_tag and has_body_tag):
                raise Exception("Invalid HTML structure - missing required tags")

            log_test("Frontend Home Page", True, {
                "has_html": has_html_tag,
                "has_body": has_body_tag,
                "is_nextjs": has_next_data,
                "content_length": len(html_content)
            })
        else:
            error_details = {
                "status_code": frontend_response.status_code,
                "content_type": content_type,
                "response_preview": frontend_response.text[:500]
            }
            print(f"\nResponse is not HTML!")
            print(f"Response preview: {frontend_response.text[:500]}")
            log_test("Frontend Home Page", False, error_details, "Not HTML content")
            raise Exception(f"Frontend response is not HTML: {content_type}")
    else:
        error_details = {
            "status_code": frontend_response.status_code,
            "response": frontend_response.text[:500],
            "headers": dict(frontend_response.headers)
        }
        print(f"\nResponse preview: {frontend_response.text[:500]}")
        log_test("Frontend Home Page", False, error_details, f"Status {frontend_response.status_code}")
        raise Exception(f"Frontend returned status {frontend_response.status_code}")

except requests.exceptions.RequestException as e:
    log_test("Frontend Home Page", False, None, str(e))
    raise Exception(f"Frontend request failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Test Summary

# COMMAND ----------

print("\n" + "="*80)
print("🧪 DEPLOYMENT TEST SUMMARY")
print("="*80)

# Count results
total_tests = len(test_results)
passed_tests = sum(1 for t in test_results if t["passed"])
failed_tests = total_tests - passed_tests

print(f"\nTotal Tests: {total_tests}")
print(f"Passed: {passed_tests}")
print(f"Failed: {failed_tests}")

print(f"\n📍 Test Results:")
for result in test_results:
    status = "✅" if result["passed"] else "❌"
    print(f"   {status} {result['test']}")
    if not result["passed"] and result["error"]:
        print(f"      Error: {result['error']}")

print(f"\n📍 App URLs:")
print(f"   Backend: {backend_url}")
print(f"   Frontend: {frontend_url}")

if failed_tests == 0:
    print(f"\n✅ ALL TESTS PASSED!")
    print(f"\n🎯 Next Steps:")
    print(f"   1. Access frontend: {frontend_url}")
    print(f"   2. Create a new chat and test the UI")
    print(f"   3. Verify agent responses are working")
    print(f"   4. Check API docs: {backend_url}/api/docs")
else:
    print(f"\n❌ {failed_tests} TEST(S) FAILED")
    print(f"\n🔧 Troubleshooting:")
    print(f"   1. Check app logs: Compute > Apps > [app_name] > Logs")
    print(f"   2. Verify SQL Warehouse is configured correctly")
    print(f"   3. Run notebook with Debug Mode = Yes for full responses")
    print(f"   4. Check backend health: {backend_url}/health")

    # Raise exception to fail the notebook
    raise Exception(f"{failed_tests} deployment test(s) failed. See summary above for details.")

print("\n" + "="*80)
