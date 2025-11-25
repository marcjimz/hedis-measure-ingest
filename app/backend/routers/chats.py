"""
Chats Router

API endpoints for chat management and messaging.
Implements the Chat API from API_SPECIFICATIONS.md
"""

import logging
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
import uuid

from app.backend.models.chat import (
    Chat,
    ChatCreate,
    ChatUpdate,
    ChatListResponse,
    ChatMessageRequest,
    ChatMessageResponse,
    Message,
    ChatStatus
)
from app.backend.config import settings

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Initialize services based on mock mode
if settings.mock_mode:
    logger.info("Using MOCK services for local development")
    from app.backend.services.mock_chat_history import MockChatHistoryManager
    from app.backend.databricks.mock_agent_service import MockAgentService
    chat_history = MockChatHistoryManager()
    agent_service = MockAgentService()
else:
    logger.info("Using PRODUCTION services with Databricks")
    from app.backend.services.delta_table_chat_history import DeltaTableChatHistoryManager
    from app.backend.databricks.agent_service import AgentService
    chat_history = DeltaTableChatHistoryManager()
    agent_service = AgentService()


# ============================================================================
# Chat List and Management Endpoints
# ============================================================================

@router.get("/chats", response_model=ChatListResponse)
async def get_chats(
    status: Optional[ChatStatus] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page")
):
    """
    Retrieve all chats for the current user with status filtering.

    Query Parameters:
    - status: Filter by "active", "under_review", "completed", "returned"
    - page: Page number (default: 1)
    - limit: Results per page (default: 20, max: 100)
    """
    try:
        # TODO: Get current user from authentication context
        user_id = "current_user"  # Placeholder

        result = await chat_history.list_chats(
            user_id=user_id,
            status=status,
            page=page,
            limit=limit
        )

        return result

    except Exception as e:
        logger.error(f"Error listing chats: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve chats"
        )


@router.post("/chats", status_code=status.HTTP_201_CREATED)
async def create_chat(chat_create: ChatCreate):
    """
    Create a new chat session.

    Request Body:
    - userId: User ID
    - patient: Optional patient identifier
    - title: Optional chat title
    """
    try:
        chat_id = await chat_history.create_chat(
            user_id=chat_create.userId,
            patient=chat_create.patient,
            title=chat_create.title or "New Chat"
        )

        return {
            "id": chat_id,
            "userId": chat_create.userId,
            "title": chat_create.title or "New Chat",
            "status": "active",
            "createdAt": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Error creating chat: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create chat"
        )


@router.get("/chats/{chat_id}", response_model=Chat)
async def get_chat(chat_id: str):
    """
    Read a single chat with all messages.

    Path Parameters:
    - chat_id: Chat identifier
    """
    try:
        chat = await chat_history.get_chat(chat_id)

        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {chat_id} not found"
            )

        return chat

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving chat {chat_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve chat"
        )


@router.put("/chats/{chat_id}")
async def update_chat(chat_id: str, chat_update: ChatUpdate):
    """
    Update chat metadata.

    Path Parameters:
    - chat_id: Chat identifier

    Request Body:
    - title: Optional new title
    - patient: Optional patient identifier
    - status: Optional new status
    """
    try:
        success = await chat_history.update_chat(chat_id, chat_update)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {chat_id} not found"
            )

        return {
            "id": chat_id,
            "updatedAt": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating chat {chat_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update chat"
        )


@router.delete("/chats/{chat_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_chat(chat_id: str):
    """
    Delete a chat (soft delete).

    Path Parameters:
    - chat_id: Chat identifier
    """
    try:
        success = await chat_history.delete_chat(chat_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {chat_id} not found"
            )

        return None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting chat {chat_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete chat"
        )


# ============================================================================
# Chat Messaging Endpoint
# ============================================================================

@router.post("/chat", response_model=ChatMessageResponse)
async def send_message(request: ChatMessageRequest):
    """
    Send a message and receive AI response.

    Request Body:
    - chatId: Optional chat ID (creates new chat if null)
    - message: User message content
    - context: Optional context (patient)

    Returns:
    - chatId: Chat identifier
    - userMessage: User's message
    - assistantMessage: AI response
    """
    try:
        chat_id = request.chatId
        user_id = "current_user"  # TODO: Get from auth context

        # Create new chat if needed
        if not chat_id:
            title = request.message[:50] + "..." if len(request.message) > 50 else request.message
            chat_id = await chat_history.create_chat(
                user_id=user_id,
                patient=request.context.patient,
                title=title
            )
            logger.info(f"Created new chat {chat_id}")

        # Save user message
        user_message_id = await chat_history.save_message(
            chat_id=chat_id,
            role="user",
            content=request.message,
            timestamp=datetime.utcnow()
        )

        # Get chat history for context
        chat = await chat_history.get_chat(chat_id)
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {chat_id} not found"
            )

        # Call agent for response
        agent_response = await agent_service.chat_completion(
            messages=chat.messages,
            chat_id=chat_id,
            patient_context=request.context.patient,
            stream=False
        )

        # Save assistant message
        assistant_message_id = await chat_history.save_message(
            chat_id=chat_id,
            role="assistant",
            content=agent_response["content"],
            timestamp=datetime.utcnow()
        )

        # Build response
        user_message = Message(
            id=user_message_id,
            role="user",
            content=request.message,
            timestamp=datetime.utcnow()
        )

        assistant_message = Message(
            id=assistant_message_id,
            role="assistant",
            content=agent_response["content"],
            timestamp=datetime.utcnow()
        )

        return ChatMessageResponse(
            chatId=chat_id,
            userMessage=user_message,
            assistantMessage=assistant_message
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing chat message: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process message"
        )


@router.post("/chat/stream")
async def send_message_stream(request: ChatMessageRequest):
    """
    Send a message and receive streaming AI response.

    Same as /chat but returns a streaming response.
    Response format: Server-Sent Events (SSE)
    """
    try:
        chat_id = request.chatId
        user_id = "current_user"  # TODO: Get from auth context

        # Create new chat if needed
        if not chat_id:
            title = request.message[:50] + "..." if len(request.message) > 50 else request.message
            chat_id = await chat_history.create_chat(
                user_id=user_id,
                patient=request.context.patient,
                title=title
            )

        # Save user message
        await chat_history.save_message(
            chat_id=chat_id,
            role="user",
            content=request.message,
            timestamp=datetime.utcnow()
        )

        # Get chat history
        chat = await chat_history.get_chat(chat_id)
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {chat_id} not found"
            )

        # Stream response from agent
        async def event_generator():
            """Generate SSE events for streaming response."""
            try:
                # Send chat ID first
                yield f"data: {{'chatId': '{chat_id}', 'type': 'start'}}\n\n"

                # Get streaming response from agent
                agent_response = await agent_service.chat_completion(
                    messages=chat.messages,
                    chat_id=chat_id,
                    patient_context=request.context.patient,
                    stream=True
                )

                full_response = ""
                stream_obj = agent_response.get("stream")

                # Stream chunks
                for chunk in stream_obj:
                    if hasattr(chunk, 'delta'):
                        content = chunk.delta.get('content', '')
                        if content:
                            full_response += content
                            yield f"data: {{'type': 'content', 'content': '{content}'}}\n\n"

                # Save complete assistant message
                await chat_history.save_message(
                    chat_id=chat_id,
                    role="assistant",
                    content=full_response,
                    timestamp=datetime.utcnow()
                )

                # Send completion event
                yield f"data: {{'type': 'done'}}\n\n"

            except Exception as e:
                logger.error(f"Error in stream: {e}", exc_info=True)
                yield f"data: {{'type': 'error', 'error': '{str(e)}'}}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing streaming message: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process streaming message"
        )
