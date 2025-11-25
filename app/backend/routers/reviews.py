"""
Reviews Router

API endpoints for review management (reviewer role required).
Implements the Reviews API from API_SPECIFICATIONS.md
"""

import logging
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, status

from backend.models.review import (
    Review,
    ReviewCreate,
    ReviewUpdate,
    ReviewListResponse,
    ReviewStatus
)
from backend.config import settings

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Initialize services based on mock mode
if settings.mock_mode:
    logger.info("Using MOCK services for local development")
    from backend.services.mock_chat_history import MockChatHistoryManager
    from backend.databricks.mock_agent_service import MockAgentService
    chat_history = MockChatHistoryManager()
    agent_service = MockAgentService()
else:
    logger.info("Using PRODUCTION services with Databricks")
    from backend.services.delta_table_chat_history import DeltaTableChatHistoryManager
    from backend.databricks.agent_service import AgentService
    chat_history = DeltaTableChatHistoryManager()
    agent_service = AgentService()


# ============================================================================
# Review List Endpoint
# ============================================================================

@router.get("/reviews", response_model=ReviewListResponse)
async def get_reviews(
    status: Optional[ReviewStatus] = Query(None, description="Filter by status"),
    assigned_to: Optional[str] = Query(None, description="Filter by reviewer ID (use 'me' for current user)"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page")
):
    """
    Get review queue with status filtering.

    Query Parameters:
    - status: Filter by "pending", "assigned", "resolved"
    - assignedTo: Filter by reviewer ID (use "me" for current user)
    - page: Page number (default: 1)
    - limit: Results per page (default: 20, max: 100)

    Note: This endpoint requires reviewer role (not yet implemented).
    """
    try:
        # TODO: Check if user has reviewer role
        current_user_id = "current_user"  # Placeholder

        # Handle "me" filter
        if assigned_to == "me":
            assigned_to = current_user_id

        result = await chat_history.list_reviews(
            status=status,
            assigned_to=assigned_to,
            page=page,
            limit=limit
        )

        return result

    except Exception as e:
        logger.error(f"Error listing reviews: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve reviews"
        )


# ============================================================================
# Review CRUD Endpoints
# ============================================================================

@router.post("/reviews", status_code=status.HTTP_201_CREATED)
async def create_review(review_create: ReviewCreate):
    """
    Create a new review request.

    Request Body:
    - chatId: Associated chat ID
    - requestedBy: User ID who requested review
    - reason: Optional reason for review
    - aiSuggestion: AI-generated suggestion

    Note: Reviews are processed FIFO or by manual assignment (no priority levels).
    """
    try:
        # Validate that chat exists
        chat = await chat_history.get_chat(review_create.chatId)
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat {review_create.chatId} not found"
            )

        # Generate AI suggestion if not provided
        if not review_create.aiSuggestion:
            logger.info(f"Generating AI suggestion for review request")
            review_create.aiSuggestion = await agent_service.generate_ai_suggestion(
                chat_messages=chat.messages,
                patient_context=chat.patient
            )

        # Create review
        review_id = await chat_history.create_review(review_create)

        return {
            "id": review_id,
            "chatId": review_create.chatId,
            "status": "pending",
            "createdAt": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating review: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create review"
        )


@router.get("/reviews/{review_id}", response_model=Review)
async def get_review(review_id: str):
    """
    Read a single review with full context.

    Path Parameters:
    - review_id: Review identifier

    Returns full review details including:
    - Review metadata
    - AI suggestion and feedback
    - Chat context (patient, messages)
    - Reviewer response and quality rating
    """
    try:
        review = await chat_history.get_review(review_id)

        if not review:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Review {review_id} not found"
            )

        return review

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving review {review_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve review"
        )


@router.put("/reviews/{review_id}")
async def update_review(review_id: str, review_update: ReviewUpdate):
    """
    Update a review.

    Path Parameters:
    - review_id: Review identifier

    Request Body:
    - status: Optional new status (pending, assigned, resolved)
    - assignedTo: Optional reviewer ID to assign
    - aiSuggestionFeedback: Optional feedback (thumbs-up, thumbs-down)
    - response: Optional reviewer response
    - feedback: Optional additional feedback
    - aiQualityRating: Optional quality rating (1-5)

    Note:
    - AI suggestions can receive thumbs up/down feedback from reviewers
    - Quality ratings help improve model performance
    """
    try:
        # Validate review exists
        existing_review = await chat_history.get_review(review_id)
        if not existing_review:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Review {review_id} not found"
            )

        # Update review
        success = await chat_history.update_review(review_id, review_update)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update review"
            )

        # If review is resolved, update chat status
        if review_update.status == "resolved":
            # Determine chat status based on response
            # "returned" if reviewer wants user to follow up
            # "completed" if review is done and chat is closed
            chat_status = "returned" if review_update.response else "completed"
            await chat_history.update_chat_status(existing_review.chatId, chat_status)

        return {
            "id": review_id,
            "updatedAt": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating review {review_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update review"
        )


@router.post("/reviews/{review_id}/assign")
async def assign_review(review_id: str, assignee_id: str):
    """
    Assign a review to a reviewer.

    Path Parameters:
    - review_id: Review identifier

    Query Parameters:
    - assignee_id: User ID to assign the review to

    This is a convenience endpoint for the most common review action.
    """
    try:
        update = ReviewUpdate(
            status="assigned",
            assignedTo=assignee_id
        )

        success = await chat_history.update_review(review_id, update)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Review {review_id} not found"
            )

        return {
            "id": review_id,
            "status": "assigned",
            "assignedTo": assignee_id,
            "updatedAt": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning review {review_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to assign review"
        )


@router.post("/reviews/{review_id}/feedback")
async def submit_ai_feedback(
    review_id: str,
    feedback: str = Query(..., description="thumbs-up or thumbs-down")
):
    """
    Submit feedback on AI suggestion.

    Path Parameters:
    - review_id: Review identifier

    Query Parameters:
    - feedback: "thumbs-up" or "thumbs-down"

    Note: This feedback helps improve AI model performance.
    """
    try:
        if feedback not in ["thumbs-up", "thumbs-down"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Feedback must be 'thumbs-up' or 'thumbs-down'"
            )

        update = ReviewUpdate(aiSuggestionFeedback=feedback)
        success = await chat_history.update_review(review_id, update)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Review {review_id} not found"
            )

        return {
            "id": review_id,
            "aiSuggestionFeedback": feedback,
            "updatedAt": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting feedback for review {review_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to submit feedback"
        )
