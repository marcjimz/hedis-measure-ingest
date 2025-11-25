"use client"

import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { ReviewRequest, Chat } from "@/lib/types"
import { Clock, AlertCircle, CheckCircle, ThumbsUp, ThumbsDown } from "lucide-react"
import Link from "next/link"
import { useState } from "react"

interface ReviewCardProps {
  review: ReviewRequest
  chat: Chat
}

export function ReviewCard({ review, chat }: ReviewCardProps) {
  const [aiFeedback, setAiFeedback] = useState<"thumbs-up" | "thumbs-down" | undefined>(review.aiSuggestedFeedback)

  const getStatusIcon = () => {
    switch (review.status) {
      case "Pending Review":
      case "Pending Assignment":
        return <Clock className="h-4 w-4" />
      case "Under Review":
        return <AlertCircle className="h-4 w-4" />
      case "Resolved":
        return <CheckCircle className="h-4 w-4" />
    }
  }

  const getStatusColor = () => {
    switch (review.status) {
      case "Pending Review":
      case "Pending Assignment":
        return "bg-orange-500/10 text-orange-600 dark:text-orange-400"
      case "Under Review":
        return "bg-blue-500/10 text-blue-600 dark:text-blue-400"
      case "Resolved":
        return "bg-green-500/10 text-green-600 dark:text-green-400"
    }
  }

  const handleAiFeedback = (feedback: "thumbs-up" | "thumbs-down") => {
    setAiFeedback(feedback === aiFeedback ? undefined : feedback)
    // TODO: Send feedback to API
    console.log("[v0] AI Suggestion feedback:", feedback)
  }

  return (
    <Card className="p-6">
      <div className="flex items-start justify-between gap-4 mb-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-2">
            <h3 className="font-semibold text-foreground">
              {chat.context.patient} - {chat.context.measure}
            </h3>
            <Badge variant="secondary" className={getStatusColor()}>
              <span className="flex items-center gap-1">
                {getStatusIcon()}
                {review.status}
              </span>
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">Submitted {review.createdAt.toLocaleString()}</p>
        </div>
        <Link href={`/reviews/${review.id}`}>
          <Button>View Details</Button>
        </Link>
      </div>

      {review.aiSuggestedAnswer && (
        <div className="mt-4 p-4 bg-secondary rounded-lg">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-medium text-secondary-foreground">AI Suggested Answer:</h4>
            <div className="flex gap-2">
              <Button
                variant={aiFeedback === "thumbs-up" ? "default" : "ghost"}
                size="sm"
                onClick={() => handleAiFeedback("thumbs-up")}
                className="h-8 w-8 p-0"
              >
                <ThumbsUp className="h-4 w-4" />
              </Button>
              <Button
                variant={aiFeedback === "thumbs-down" ? "default" : "ghost"}
                size="sm"
                onClick={() => handleAiFeedback("thumbs-down")}
                className="h-8 w-8 p-0"
              >
                <ThumbsDown className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <p className="text-sm text-secondary-foreground/80 line-clamp-3">{review.aiSuggestedAnswer}</p>
        </div>
      )}

      {review.reviewerResponse && (
        <div className="mt-4 p-4 bg-primary/10 rounded-lg">
          <h4 className="text-sm font-medium text-foreground mb-2">Reviewer Response:</h4>
          <p className="text-sm text-muted-foreground line-clamp-3">{review.reviewerResponse}</p>
        </div>
      )}
    </Card>
  )
}
