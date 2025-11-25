"use client"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import type { Message, FeedbackType } from "@/lib/types"
import { ThumbsUp, ThumbsDown, Bot, User } from "lucide-react"
import { cn } from "@/lib/utils"

interface MessageListProps {
  messages: Message[]
  onFeedback?: (messageId: string, feedback: FeedbackType) => void
}

export function MessageList({ messages, onFeedback }: MessageListProps) {
  return (
    <div className="space-y-4">
      {messages.map((message) => (
        <div key={message.id} className={cn("flex gap-4", message.role === "user" ? "justify-end" : "justify-start")}>
          {message.role === "assistant" && (
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-primary">
              <Bot className="h-4 w-4 text-primary-foreground" />
            </div>
          )}

          <Card
            className={cn(
              "p-4 max-w-[80%]",
              message.role === "user" ? "bg-primary text-primary-foreground" : "bg-card",
            )}
          >
            <p className="text-sm leading-relaxed whitespace-pre-wrap">{message.content}</p>
            <div className="flex items-center justify-between gap-4 mt-2">
              <span
                className={cn(
                  "text-xs",
                  message.role === "user" ? "text-primary-foreground/70" : "text-muted-foreground",
                )}
              >
                {message.timestamp.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
              </span>
              {message.role === "assistant" && onFeedback && (
                <div className="flex gap-1">
                  <Button
                    variant="ghost"
                    size="icon"
                    className={cn("h-6 w-6", message.feedback === "thumbs-up" && "text-green-600 dark:text-green-400")}
                    onClick={() => onFeedback(message.id, "thumbs-up")}
                  >
                    <ThumbsUp className="h-3 w-3" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className={cn("h-6 w-6", message.feedback === "thumbs-down" && "text-red-600 dark:text-red-400")}
                    onClick={() => onFeedback(message.id, "thumbs-down")}
                  >
                    <ThumbsDown className="h-3 w-3" />
                  </Button>
                </div>
              )}
            </div>
          </Card>

          {message.role === "user" && (
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-secondary">
              <User className="h-4 w-4 text-secondary-foreground" />
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
