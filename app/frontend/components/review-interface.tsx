"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { MessageList } from "@/components/message-list"
import type { ReviewRequest, Chat, User } from "@/lib/types"
import { ArrowLeft, Send, CheckCircle } from "lucide-react"
import Link from "next/link"

interface ReviewInterfaceProps {
  review: ReviewRequest
  chat: Chat
  currentUser: User
}

export function ReviewInterface({ review: initialReview, chat, currentUser }: ReviewInterfaceProps) {
  const router = useRouter()
  const [review, setReview] = useState<ReviewRequest>(initialReview)
  const [reviewerResponse, setReviewerResponse] = useState(review.reviewerResponse || "")
  const [reviewerFeedback, setReviewerFeedback] = useState(review.reviewerFeedback || "")
  const [isSubmitting, setIsSubmitting] = useState(false)

  const canEdit = review.status !== "Resolved"

  const handleAssign = async () => {
    setReview((prev) => ({
      ...prev,
      status: "Under Review",
      assignedTo: currentUser.id,
    }))
    console.log("[v0] Review assigned to:", currentUser.id)
  }

  const handleSubmit = async () => {
    if (!reviewerResponse.trim()) return

    setIsSubmitting(true)

    try {
      // In a real app, submit via API
      await new Promise((resolve) => setTimeout(resolve, 500))

      setReview((prev) => ({
        ...prev,
        status: "Resolved",
        reviewerResponse,
        reviewerFeedback,
        updatedAt: new Date(),
      }))

      console.log("[v0] Review submitted:", {
        reviewId: review.id,
        reviewerResponse,
        reviewerFeedback,
      })

      // Show success and redirect
      setTimeout(() => {
        router.push("/reviews")
      }, 1500)
    } catch (error) {
      console.error("[v0] Failed to submit review:", error)
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="container mx-auto p-6 max-w-6xl">
      <Link href="/reviews">
        <Button variant="ghost" className="gap-2 mb-6">
          <ArrowLeft className="h-4 w-4" />
          Back to Reviews
        </Button>
      </Link>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Left Column - Chat Context */}
        <div className="space-y-6">
          <Card className="p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-bold text-foreground">Chat Context</h2>
              <Badge variant="secondary" className="bg-orange-500/10 text-orange-600 dark:text-orange-400">
                {review.status}
              </Badge>
            </div>

            <div className="space-y-4">
              <div>
                <Label className="text-muted-foreground">Patient</Label>
                <p className="font-medium text-foreground">{chat.context.patient}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Measure</Label>
                <p className="font-medium text-foreground">{chat.context.measure}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Submitted</Label>
                <p className="text-sm text-foreground">{review.createdAt.toLocaleString()}</p>
              </div>
            </div>
          </Card>

          <Card className="p-6">
            <h3 className="font-semibold text-foreground mb-4">Conversation History</h3>
            <div className="max-h-[500px] overflow-y-auto">
              <MessageList messages={chat.messages} />
            </div>
          </Card>
        </div>

        {/* Right Column - Review Details */}
        <div className="space-y-6">
          <Card className="p-6">
            <Tabs defaultValue="ai-suggestion" className="w-full">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="ai-suggestion">AI Suggestion</TabsTrigger>
                <TabsTrigger value="supporting-context">Context</TabsTrigger>
              </TabsList>

              <TabsContent value="ai-suggestion" className="mt-4">
                <div className="space-y-2">
                  <Label>AI Suggested Answer</Label>
                  <div className="p-4 bg-secondary rounded-lg">
                    <p className="text-sm text-secondary-foreground leading-relaxed">
                      {review.aiSuggestedAnswer || "No AI suggestion available"}
                    </p>
                  </div>
                </div>
              </TabsContent>

              <TabsContent value="supporting-context" className="mt-4">
                <div className="space-y-2">
                  <Label>Supporting Context</Label>
                  <div className="p-4 bg-secondary rounded-lg">
                    <p className="text-sm text-secondary-foreground leading-relaxed">
                      {review.aiSupportingContext || "No additional context available"}
                    </p>
                  </div>
                </div>
              </TabsContent>
            </Tabs>
          </Card>

          <Card className="p-6">
            <h3 className="font-semibold text-foreground mb-4">Your Review</h3>

            {review.status === "Pending Assignment" && (
              <div className="text-center py-6">
                <p className="text-muted-foreground mb-4">Assign this review to yourself to begin</p>
                <Button onClick={handleAssign}>Assign to Me</Button>
              </div>
            )}

            {review.status !== "Pending Assignment" && (
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="response">Reviewer Response</Label>
                  <Textarea
                    id="response"
                    placeholder="Provide your expert response to the user's question..."
                    value={reviewerResponse}
                    onChange={(e) => setReviewerResponse(e.target.value)}
                    className="min-h-[150px]"
                    disabled={!canEdit}
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="feedback">Internal Feedback (Optional)</Label>
                  <Textarea
                    id="feedback"
                    placeholder="Notes on AI performance, areas for improvement..."
                    value={reviewerFeedback}
                    onChange={(e) => setReviewerFeedback(e.target.value)}
                    className="min-h-[100px]"
                    disabled={!canEdit}
                  />
                </div>

                {canEdit ? (
                  <Button
                    onClick={handleSubmit}
                    disabled={!reviewerResponse.trim() || isSubmitting}
                    className="w-full gap-2"
                  >
                    {isSubmitting ? (
                      "Submitting..."
                    ) : (
                      <>
                        <Send className="h-4 w-4" />
                        Submit Review
                      </>
                    )}
                  </Button>
                ) : (
                  <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                    <CheckCircle className="h-5 w-5" />
                    <span className="font-medium">Review Completed</span>
                  </div>
                )}
              </div>
            )}
          </Card>
        </div>
      </div>
    </div>
  )
}
