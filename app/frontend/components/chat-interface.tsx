"use client"

import type React from "react"

import { useState, useRef, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { MessageList } from "@/components/message-list"
import type { Chat, User, Message } from "@/lib/types"
import { ArrowLeft, Send, Loader2, UserCheck } from "lucide-react"
import Link from "next/link"
import { isFeatureEnabled } from "@/lib/feature-flags"
import { apiUrl } from "@/lib/api"

interface ChatInterfaceProps {
  chat: Chat
  currentUser: User
  showBackButton?: boolean
}

export function ChatInterface({ chat: initialChat, currentUser, showBackButton = false }: ChatInterfaceProps) {
  const [chat, setChat] = useState<Chat>(initialChat)
  const [input, setInput] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [reviewRequested, setReviewRequested] = useState(!!chat.reviewRequestId)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const reviewsEnabled = isFeatureEnabled("reviews")

  useEffect(() => {
    setChat(initialChat)
  }, [initialChat])

  const handleSend = async () => {
    if (!input.trim() || isLoading) return

    const userMessage: Message = {
      id: `msg-${Date.now()}`,
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    }

    // Add user message immediately
    setChat((prev) => ({
      ...prev,
      messages: [...prev.messages, userMessage],
    }))

    setInput("")
    setIsLoading(true)

    try {
      // Call AI API
      const response = await fetch(apiUrl("/api/chat"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chatId: chat.id,
          message: userMessage.content,
          context: chat.context,
          history: chat.messages,
        }),
      })

      if (!response.ok) throw new Error("Failed to get AI response")

      const data = await response.json()

      const aiMessage: Message = {
        id: `msg-${Date.now()}-ai`,
        role: "assistant",
        content: data.message,
        timestamp: new Date(),
      }

      setChat((prev) => ({
        ...prev,
        messages: [...prev.messages, aiMessage],
      }))
    } catch (error) {
      console.error("[v0] AI response error:", error)
      // Add error message
      const errorMessage: Message = {
        id: `msg-${Date.now()}-error`,
        role: "assistant",
        content: "I apologize, but I'm having trouble responding right now. Please try again.",
        timestamp: new Date(),
      }
      setChat((prev) => ({
        ...prev,
        messages: [...prev.messages, errorMessage],
      }))
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const handleFeedback = async (messageId: string, feedback: "thumbs-up" | "thumbs-down") => {
    setChat((prev) => ({
      ...prev,
      messages: prev.messages.map((msg) => (msg.id === messageId ? { ...msg, feedback } : msg)),
    }))

    // In a real app, send feedback to API
    console.log("[v0] Feedback submitted:", messageId, feedback)
  }

  const handleRequestReview = async () => {
    try {
      const response = await fetch(apiUrl("/api/reviews"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chatId: chat.id,
          userId: currentUser.id,
        }),
      })

      if (!response.ok) throw new Error("Failed to create review request")

      const reviewData = await response.json()

      setReviewRequested(true)
      setChat((prev) => ({
        ...prev,
        reviewRequestId: reviewData.id,
      }))
    } catch (error) {
      console.error("Review request error:", error)
      alert("Failed to submit review request. Please try again.")
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border bg-background p-4">
        <div className="container mx-auto max-w-4xl flex items-center justify-between">
          <div className="flex items-center gap-4">
            {showBackButton && (
              <Link href="/chats">
                <Button variant="ghost" size="icon">
                  <ArrowLeft className="h-4 w-4" />
                </Button>
              </Link>
            )}
            <div>
              <h2 className="font-semibold text-foreground">
                {chat.context.patient ? `${chat.context.patient} - Chat` : "HEDIS Chat"}
              </h2>
              <p className="text-sm text-muted-foreground">AI Support Chat</p>
            </div>
          </div>
          {reviewsEnabled && chat.messages.length > 0 && !reviewRequested && (
            <Button variant="outline" onClick={handleRequestReview} className="gap-2 bg-transparent">
              <UserCheck className="h-4 w-4" />
              Request Human Review
            </Button>
          )}
          {reviewsEnabled && reviewRequested && (
            <Badge variant="secondary" className="bg-orange-500/10 text-orange-600 dark:text-orange-400">
              Review Requested
            </Badge>
          )}
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        <div className="container mx-auto max-w-4xl py-6">
          {chat.messages.length === 0 ? (
            <div className="text-center py-12">
              <h3 className="text-lg font-semibold text-foreground mb-2">Start the conversation</h3>
              <p className="text-muted-foreground">
                {chat.context.patient
                  ? `Ask any questions about HEDIS measures for ${chat.context.patient}`
                  : "Ask any questions about HEDIS measures"}
              </p>
            </div>
          ) : (
            <MessageList messages={chat.messages} onFeedback={handleFeedback} />
          )}
        </div>
      </div>

      {/* Input */}
      <div className="border-t border-border bg-background p-4">
        <div className="container mx-auto max-w-4xl">
          <Card className="p-4">
            <div className="flex gap-4">
              <Textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Type your message... (Shift+Enter for new line)"
                className="min-h-[60px] max-h-[200px] resize-none"
                disabled={isLoading}
              />
              <Button onClick={handleSend} disabled={!input.trim() || isLoading} className="self-end">
                {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              </Button>
            </div>
          </Card>
        </div>
      </div>
    </div>
  )
}
