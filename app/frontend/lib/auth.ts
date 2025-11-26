import type { User, UserRole } from "./types"
import { mockCurrentUser } from "./mock-data"

// Simple auth utilities for role-based access control
// In production, this would integrate with a real auth system

export function getCurrentUser(): User {
  // In production, get from session/JWT
  return mockCurrentUser
}

export function hasRole(user: User, role: UserRole): boolean {
  return user.role === role
}

export function isReviewer(user: User): boolean {
  return hasRole(user, "reviewer")
}

export function canAccessReviews(user: User): boolean {
  return isReviewer(user)
}

export function canAccessCases(user: User): boolean {
  return isReviewer(user)
}

export function canReviewChat(user: User, chatUserId: string): boolean {
  // Reviewers can review any chat
  // Regular users can only view their own chats
  return isReviewer(user) || user.id === chatUserId
}
