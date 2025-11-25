import { mockChats } from "@/lib/mock-data"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Plus, MessageSquare } from "lucide-react"
import Link from "next/link"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Badge } from "@/components/ui/badge"
import { mockCurrentUser } from "@/lib/mock-data"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { AlertCircle } from "lucide-react"
import { isFeatureEnabled } from "@/lib/feature-flags"

export default function ChatsPage() {
  const isReviewer = mockCurrentUser.role === "reviewer"
  const reviewsEnabled = isFeatureEnabled("reviews")

  const activeChats = mockChats.filter((chat) => !chat.reviewRequestId)
  const requestedReviewChats = mockChats.filter((chat) => chat.reviewRequestId)

  return (
    <div className="container mx-auto p-6 max-w-6xl">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Your Chats</h1>
          <p className="text-muted-foreground">AI-powered support conversations</p>
        </div>
        <Link href="/chats/create">
          <Button className="gap-2">
            <Plus className="h-4 w-4" />
            New Chat
          </Button>
        </Link>
      </div>

      <Tabs defaultValue="active" className="w-full">
        <TabsList className={`grid w-full max-w-2xl ${reviewsEnabled ? "grid-cols-3" : "grid-cols-2"}`}>
          <TabsTrigger value="active" className="gap-2">
            Active
            {activeChats.length > 0 && (
              <Badge variant="secondary" className="ml-1 px-1.5 py-0 text-xs min-w-[20px] justify-center">
                {activeChats.length}
              </Badge>
            )}
          </TabsTrigger>
          {reviewsEnabled && (
            <TabsTrigger value="requested-reviews" className="gap-2">
              Requested Reviews
              {requestedReviewChats.length > 0 && (
                <Badge variant="secondary" className="ml-1 px-1.5 py-0 text-xs min-w-[20px] justify-center">
                  {requestedReviewChats.length}
                </Badge>
              )}
            </TabsTrigger>
          )}
          <TabsTrigger value="to-review" disabled={!isReviewer || !reviewsEnabled} className="gap-2">
            To Review
            {(!isReviewer || !reviewsEnabled) && (
              <Badge variant="outline" className="ml-1 px-1.5 py-0 text-xs">
                🔒
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="active" className="mt-6">
          <div className="grid gap-4">
            {activeChats.map((chat) => {
              const firstUserMessage = chat.messages.find((m) => m.role === "user")
              return (
                <Link key={chat.id} href={`/chats/${chat.id}`}>
                  <Card className="p-4 hover:bg-accent transition-colors cursor-pointer">
                    <div className="flex items-start gap-3">
                      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary/10">
                        <MessageSquare className="h-4 w-4 text-primary" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between gap-4 mb-1">
                          <span className="text-xs text-muted-foreground">{chat.context.patient || "General Q&A"}</span>
                          <span className="text-xs text-muted-foreground whitespace-nowrap">
                            {chat.updatedAt.toLocaleDateString()}
                          </span>
                        </div>
                        <p className="text-sm text-foreground line-clamp-2">
                          {firstUserMessage?.content || "No messages yet"}
                        </p>
                      </div>
                    </div>
                  </Card>
                </Link>
              )
            })}
          </div>

          {activeChats.length === 0 && (
            <div className="text-center py-12">
              <MessageSquare className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-foreground mb-2">No active chats</h3>
              <p className="text-muted-foreground mb-4">Start a new conversation to get AI-powered support</p>
              <Link href="/chats/create">
                <Button>
                  <Plus className="h-4 w-4 mr-2" />
                  Start Your First Chat
                </Button>
              </Link>
            </div>
          )}
        </TabsContent>

        {reviewsEnabled && (
          <TabsContent value="requested-reviews" className="mt-6">
            <div className="grid gap-4">
              {requestedReviewChats.map((chat) => {
                const firstUserMessage = chat.messages.find((m) => m.role === "user")

                const statusConfig = {
                  "Under Review": {
                    color: "bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/30",
                    cardBorder: "border-orange-500/50",
                    iconBg: "bg-orange-500/10",
                    iconColor: "text-orange-600 dark:text-orange-400",
                  },
                  Completed: {
                    color: "bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/30",
                    cardBorder: "border-green-500/50",
                    iconBg: "bg-green-500/10",
                    iconColor: "text-green-600 dark:text-green-400",
                  },
                  Returned: {
                    color: "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/30",
                    cardBorder: "border-blue-500/50",
                    iconBg: "bg-blue-500/10",
                    iconColor: "text-blue-600 dark:text-blue-400",
                  },
                }

                const config = statusConfig[chat.reviewStatus || "Under Review"]

                return (
                  <Link key={chat.id} href={`/chats/${chat.id}`}>
                    <Card className={`p-4 hover:bg-accent transition-colors cursor-pointer ${config.cardBorder}`}>
                      <div className="flex items-start gap-3">
                        <div
                          className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${config.iconBg}`}
                        >
                          <MessageSquare className={`h-4 w-4 ${config.iconColor}`} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-4 mb-1">
                            <div className="flex items-center gap-2">
                              <span className="text-xs text-muted-foreground">
                                {chat.context.patient || "General Q&A"}
                              </span>
                              <Badge variant="outline" className={`text-xs ${config.color}`}>
                                {chat.reviewStatus || "Under Review"}
                              </Badge>
                            </div>
                            <span className="text-xs text-muted-foreground whitespace-nowrap">
                              {chat.updatedAt.toLocaleDateString()}
                            </span>
                          </div>
                          <p className="text-sm text-foreground line-clamp-2">
                            {firstUserMessage?.content || "No messages yet"}
                          </p>
                        </div>
                      </div>
                    </Card>
                  </Link>
                )
              })}
            </div>

            {requestedReviewChats.length === 0 && (
              <div className="text-center py-12">
                <MessageSquare className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-foreground mb-2">No requested reviews</h3>
                <p className="text-muted-foreground">Chats awaiting human review will appear here</p>
              </div>
            )}
          </TabsContent>
        )}

        <TabsContent value="to-review" className="mt-6">
          {(!isReviewer || !reviewsEnabled) && (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                {!reviewsEnabled
                  ? "The review feature is coming soon. This section will be available in a future release."
                  : "Only reviewers can access this section. This tab is for reviewing chats that need human expertise."}
              </AlertDescription>
            </Alert>
          )}
        </TabsContent>
      </Tabs>
    </div>
  )
}
