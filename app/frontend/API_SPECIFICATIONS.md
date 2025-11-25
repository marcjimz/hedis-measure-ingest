// ... existing code ...

## 3. Chats API

### GET /api/chats
Retrieve all chats for the current user with status filtering.

**Query Parameters:**
- `status` (string, optional) - Filter: "active", "requested_reviews", "resolved"
- `page` (number, optional, default: 1)
- `limit` (number, optional, default: 20)

**Response:**
\`\`\`json
{
  "chats": [
    {
      "id": "string",
      "title": "string",
      "patient": "string | null",
      "status": "active" | "under_review" | "completed" | "returned",
      "lastMessage": "string",
      "lastMessageTime": "ISO 8601 timestamp",
      "createdAt": "ISO 8601 timestamp",
      "messageCount": 5
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 45,
    "totalPages": 3
  }
}
\`\`\`

**Note:** 
- `status` can be "active" (no review requested), "under_review" (review requested and pending), "completed" (review completed with response), or "returned" (returned to user from review)
- Chats are general Q&A conversations about HEDIS measures, not tied to specific measures
- Patient context is optional and only available when the patients feature is enabled

### POST /api/chat
Send a message and receive AI response (streaming or single response).

**Request Body:**
\`\`\`json
{
  "chatId": "string | null",
  "message": "string",
  "context": {
    "patient": "string | null"
  }
}
\`\`\`

**Response:**
\`\`\`json
{
  "chatId": "string",
  "userMessage": {
    "id": "string",
    "role": "user",
    "content": "string",
    "timestamp": "ISO 8601 timestamp"
  },
  "assistantMessage": {
    "id": "string",
    "role": "assistant",
    "content": "string",
    "timestamp": "ISO 8601 timestamp"
  }
}
\`\`\`

**Note:** 
- If `chatId` is null, a new chat session is created
- Context includes only patient information (no measure context)
- AI responses are general HEDIS measure guidance

// ... existing code ...

## 4. Reviews API (Reviewer Role Required)

### GET /api/reviews
Get review queue with status filtering.

**Query Parameters:**
- `status` (string, optional) - Filter: "pending", "assigned", "resolved"
- `assignedTo` (string, optional) - Filter by reviewer ID (use "me" for current user)
- `page` (number, optional, default: 1)
- `limit` (number, optional, default: 20)

**Response:**
\`\`\`json
{
  "reviews": [
    {
      "id": "string",
      "chatId": "string",
      "status": "pending" | "assigned" | "resolved",
      "patient": "string | null",
      "requestedBy": {
        "id": "string",
        "name": "string"
      },
      "assignedTo": {
        "id": "string",
        "name": "string"
      } | null,
      "aiSuggestion": "string",
      "aiSuggestionFeedback": "thumbs-up" | "thumbs-down" | null,
      "createdAt": "ISO 8601 timestamp"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 15,
    "totalPages": 1
  }
}
\`\`\`

**Note:**
- No priority levels; reviews processed in FIFO order or by manual assignment
- AI suggestions can receive thumbs up/down feedback from reviewers

// ... existing code ...

## Chats Management

### CREATE - POST /api/chats
Create a new chat session.

**Request Body:**
\`\`\`json
{
  "userId": "string",
  "patient": "string | null",
  "title": "string (optional)"
}
\`\`\`

**Response:**
\`\`\`json
{
  "id": "string",
  "userId": "string",
  "title": "string",
  "status": "active",
  "createdAt": "ISO 8601 timestamp"
}
\`\`\`

**Note:** 
- Chats are general Q&A about HEDIS measures
- No measure context; conversations are open-ended about any HEDIS topic
- Patient context is optional

### READ - GET /api/chats/:id
Read a single chat with all messages.

**Response:** Returns complete chat object with messages array.

### UPDATE - PUT /api/chats/:id
Update chat metadata.

**Request Body:**
\`\`\`json
{
  "title": "string (optional)",
  "patient": "string | null (optional)",
  "status": "active" | "under_review" | "completed" | "returned (optional)"
}
\`\`\`

**Response:**
\`\`\`json
{
  "id": "string",
  "updatedAt": "ISO 8601 timestamp"
}
\`\`\`

**Note:**
- Status transitions: active → under_review (when review requested) → completed/returned (after review)
- "completed" means review was done and chat is closed
- "returned" means review was done and chat returned to user for follow-up

// ... existing code ...

## Reviews Management

### CREATE - POST /api/reviews
Create a new review request.

**Request Body:**
\`\`\`json
{
  "chatId": "string",
  "requestedBy": "string",
  "reason": "string (optional)",
  "aiSuggestion": "string"
}
\`\`\`

**Response:**
\`\`\`json
{
  "id": "string",
  "chatId": "string",
  "status": "pending",
  "createdAt": "ISO 8601 timestamp"
}
\`\`\`

**Note:**
- No priority levels assigned to reviews
- Reviews are processed FIFO or by manual assignment

### READ - GET /api/reviews/:id
Read a single review with full context.

**Response:**
\`\`\`json
{
  "id": "string",
  "chatId": "string",
  "status": "pending" | "assigned" | "resolved",
  "requestedBy": {
    "id": "string",
    "name": "string"
  },
  "assignedTo": {
    "id": "string",
    "name": "string"
  } | null,
  "aiSuggestion": "string",
  "aiSuggestionFeedback": "thumbs-up" | "thumbs-down" | null,
  "response": "string | null",
  "feedback": "string | null",
  "aiQualityRating": 1 | 2 | 3 | 4 | 5 | null,
  "chatContext": {
    "patient": "string | null",
    "messages": [
      {
        "role": "user" | "assistant",
        "content": "string",
        "timestamp": "ISO 8601 timestamp"
      }
    ]
  },
  "createdAt": "ISO 8601 timestamp",
  "updatedAt": "ISO 8601 timestamp",
  "resolvedAt": "ISO 8601 timestamp | null"
}
\`\`\`

// ... existing code ...

## Additional Notes

1. **Timestamps**: All timestamps use ISO 8601 format (e.g., "2025-01-15T14:30:00Z")
2. **Pagination**: Consistent across all list endpoints with `page`, `limit`, `total`, and `totalPages`
3. **Authentication**: JWT tokens required for all endpoints except public health checks
4. **Rate Limiting**: 1000 requests per hour per authenticated user
5. **Soft Deletes**: Recommended for patients, chats, and reviews to maintain audit trails
6. **WebSocket Support**: Real-time updates available at `wss://api.hedischat.example.com/v1/ws/chats/:id`
7. **CRUD Pattern**: All resources follow Create (POST), Read (GET), Update (PUT/PATCH), Delete (DELETE)
8. **Idempotency**: PUT and DELETE operations are idempotent; POST operations should use idempotency keys for critical operations
9. **AI Feedback**: Thumbs up/down feedback on AI suggestions helps improve model performance
10. **No Priority Levels**: Reviews are processed in FIFO order unless manually assigned
11. **Chat Context**: Chats are general Q&A about HEDIS measures, not specific to individual measures. Patient context is optional.
12. **Review Status Flow**: pending → assigned → resolved. Chat status updates: active → under_review → completed/returned
13. **Feature Flags**: Patient features are controlled by feature flags and may not be available in all deployments

---

## FastAPI Implementation Notes

### Recommended Project Structure
\`\`\`
app/
├── main.py                 # FastAPI app entry point
├── routers/
│   ├── measures.py         # Measures endpoints
│   ├── patients.py         # Patients endpoints
│   ├── chats.py            # Chats endpoints
│   ├── reviews.py          # Reviews endpoints
│   └── users.py            # Users endpoints
├── models/
│   ├── measure.py          # Pydantic models for measures
│   ├── patient.py          # Pydantic models for patients
│   ├── chat.py             # Pydantic models for chats
│   ├── review.py           # Pydantic models for reviews
│   └── user.py             # Pydantic models for users
├── services/
│   ├── ai_service.py       # AI/LLM integration
│   ├── auth_service.py     # Authentication/authorization
│   └── db_service.py       # Database operations
├── middleware/
│   ├── auth.py             # JWT authentication middleware
│   └── rate_limit.py       # Rate limiting middleware
└── config.py               # Configuration settings
\`\`\`

### Key FastAPI Features to Use
1. **Dependency Injection** - Use for authentication, database connections
2. **Pydantic Models** - Type validation for all request/response bodies
3. **Background Tasks** - For async AI processing and notifications
4. **WebSocket** - For real-time chat updates
5. **CORS Middleware** - For frontend connectivity
6. **APIRouter** - Organize endpoints by resource type
7. **OpenAPI/Swagger** - Auto-generated from these specs

### Example Endpoint Implementation
\`\`\`python
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional
from models.chat import Chat, ChatCreate, ChatUpdate
from services.auth_service import get_current_user
from services.db_service import get_db

router = APIRouter(prefix="/api/chats", tags=["chats"])

@router.get("/", response_model=List[Chat])
async def get_chats(
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
    current_user = Depends(get_current_user),
    db = Depends(get_db)
):
    # Implementation
    pass
