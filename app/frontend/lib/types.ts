// Core types for the AI Chat Support System

export type UserRole = "user" | "reviewer"

export type MessageRole = "user" | "assistant"

export type ReviewStatus = "Pending Review" | "Pending Assignment" | "Under Review" | "Resolved"

export type ChatReviewStatus = "Under Review" | "Completed" | "Returned"

export type FeedbackType = "thumbs-up" | "thumbs-down"

export interface User {
  id: string
  name: string
  email: string
  role: UserRole
}

export interface Message {
  id: string
  role: MessageRole
  content: string
  timestamp: Date
  feedback?: FeedbackType
}

export interface ChatContext {
  patient: string
  measure: string
}

export interface Chat {
  id: string
  userId: string
  context: ChatContext
  messages: Message[]
  createdAt: Date
  updatedAt: Date
  reviewRequestId?: string
  reviewStatus?: ChatReviewStatus
}

export interface ReviewRequest {
  id: string
  chatId: string
  userId: string
  status: ReviewStatus
  createdAt: Date
  updatedAt: Date
  assignedTo?: string
  aiSuggestedAnswer?: string
  aiSuggestedFeedback?: FeedbackType // Added feedback for AI suggestions
  aiSupportingContext?: string
  reviewerResponse?: string
  reviewerFeedback?: string
}

export interface Measure {
  id: string
  name: string
  description?: string
}

export interface NCQAMeasure {
  specifications: string
  measure: string
  initial_pop: string
  denominator: string[]
  numerator: string[]
  exclusion: string[]
  effective_year: number
  version?: string
}

export interface Patient {
  id: string
  name: string
  dateOfBirth: string
  memberId: string
  status: "active" | "inactive"
}
