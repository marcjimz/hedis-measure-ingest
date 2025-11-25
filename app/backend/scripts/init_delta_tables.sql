-- ============================================================================
-- Initialize Delta Tables for HEDIS Chat Application
-- ============================================================================
-- This script creates the Delta tables needed for chat history and reviews.
-- Run this in Databricks SQL or Databricks Notebook before starting the API.
--
-- Usage:
--   1. Update the catalog and schema names below
--   2. Run each CREATE TABLE statement
--   3. Verify tables are created with SHOW TABLES
-- ============================================================================

-- Set your catalog and schema
USE CATALOG marcin_demo2;
USE SCHEMA hedis_measurements;

-- ============================================================================
-- Chats Table
-- ============================================================================
-- Stores chat session metadata
CREATE TABLE IF NOT EXISTS hedis_chats (
    id STRING COMMENT 'Unique chat identifier',
    user_id STRING COMMENT 'User who owns this chat',
    title STRING COMMENT 'Chat title',
    patient STRING COMMENT 'Optional patient identifier',
    status STRING COMMENT 'Chat status: active, under_review, completed, returned',
    created_at TIMESTAMP COMMENT 'Chat creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    deleted BOOLEAN COMMENT 'Soft delete flag'
)
USING DELTA
COMMENT 'Chat sessions for HEDIS measure discussions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_chats_user_status
ON hedis_chats (user_id, status, deleted);

-- ============================================================================
-- Messages Table
-- ============================================================================
-- Stores individual messages within chats
CREATE TABLE IF NOT EXISTS hedis_messages (
    id STRING COMMENT 'Unique message identifier',
    chat_id STRING COMMENT 'Parent chat identifier',
    role STRING COMMENT 'Message role: user, assistant, system, tool',
    content STRING COMMENT 'Message content',
    timestamp TIMESTAMP COMMENT 'Message timestamp',
    name STRING COMMENT 'Optional sender name',
    tool_calls STRING COMMENT 'Optional JSON-encoded tool calls',
    tool_call_id STRING COMMENT 'Optional tool call identifier'
)
USING DELTA
COMMENT 'Messages within chat sessions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_messages_chat_timestamp
ON hedis_messages (chat_id, timestamp);

-- ============================================================================
-- Reviews Table
-- ============================================================================
-- Stores review requests for human expert validation
CREATE TABLE IF NOT EXISTS hedis_reviews (
    id STRING COMMENT 'Unique review identifier',
    chat_id STRING COMMENT 'Associated chat identifier',
    status STRING COMMENT 'Review status: pending, assigned, resolved',
    patient STRING COMMENT 'Optional patient identifier',
    requested_by_id STRING COMMENT 'User ID who requested the review',
    requested_by_name STRING COMMENT 'User name who requested the review',
    assigned_to_id STRING COMMENT 'Optional assigned reviewer user ID',
    assigned_to_name STRING COMMENT 'Optional assigned reviewer name',
    ai_suggestion STRING COMMENT 'AI-generated suggested response',
    ai_suggestion_feedback STRING COMMENT 'Feedback on AI suggestion: thumbs-up, thumbs-down',
    response STRING COMMENT 'Reviewer response text',
    feedback STRING COMMENT 'Additional feedback or notes',
    ai_quality_rating INT COMMENT 'Quality rating of AI suggestion (1-5)',
    created_at TIMESTAMP COMMENT 'Review creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    resolved_at TIMESTAMP COMMENT 'Resolution timestamp'
)
USING DELTA
COMMENT 'Review requests for expert validation'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_reviews_status_assigned
ON hedis_reviews (status, assigned_to_id);

-- ============================================================================
-- Verify Table Creation
-- ============================================================================
SHOW TABLES LIKE 'hedis_*';

-- ============================================================================
-- Sample Queries for Validation
-- ============================================================================

-- Check chats table schema
DESCRIBE TABLE hedis_chats;

-- Check messages table schema
DESCRIBE TABLE hedis_messages;

-- Check reviews table schema
DESCRIBE TABLE hedis_reviews;

-- ============================================================================
-- Optional: Create Sample Data for Testing
-- ============================================================================

-- Insert sample chat
INSERT INTO hedis_chats VALUES (
    'chat_test_001',
    'user_test_001',
    'Test Chat',
    'Patient_001',
    'active',
    current_timestamp(),
    current_timestamp(),
    false
);

-- Insert sample message
INSERT INTO hedis_messages VALUES (
    'msg_test_001',
    'chat_test_001',
    'user',
    'What is the CWP measure?',
    current_timestamp(),
    NULL,
    NULL,
    NULL
);

-- Verify inserts
SELECT * FROM hedis_chats WHERE id = 'chat_test_001';
SELECT * FROM hedis_messages WHERE chat_id = 'chat_test_001';

-- ============================================================================
-- Cleanup (Optional)
-- ============================================================================
-- Uncomment to drop sample data
-- DELETE FROM hedis_messages WHERE chat_id = 'chat_test_001';
-- DELETE FROM hedis_chats WHERE id = 'chat_test_001';
