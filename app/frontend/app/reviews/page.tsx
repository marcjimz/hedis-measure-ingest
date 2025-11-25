import { mockReviewRequests, mockChats, mockCurrentUser } from "@/lib/mock-data"
import { ReviewCard } from "@/components/review-card"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { RoleGuard } from "@/components/role-guard"
import { ClipboardList } from "lucide-react"

export default function ReviewsPage() {
  const pendingReviews = mockReviewRequests.filter(
    (r) => r.status === "Pending Review" || r.status === "Pending Assignment",
  )
  const myReviews = mockReviewRequests.filter((r) => r.status === "Under Review" && r.assignedTo === mockCurrentUser.id)
  const resolvedReviews = mockReviewRequests.filter((r) => r.status === "Resolved")

  return (
    <RoleGuard allowedRoles={["reviewer"]}>
      <div className="container mx-auto p-6 max-w-6xl">
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-foreground">Review Queue</h1>
          <p className="text-muted-foreground">Review AI responses and provide human expertise</p>
        </div>

        <Tabs defaultValue="pending" className="w-full">
          <TabsList className="grid w-full grid-cols-3 max-w-md">
            <TabsTrigger value="pending">
              Pending
              {pendingReviews.length > 0 && (
                <Badge variant="secondary" className="ml-2">
                  {pendingReviews.length}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="mine">
              My Reviews
              {myReviews.length > 0 && (
                <Badge variant="secondary" className="ml-2">
                  {myReviews.length}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="resolved">Resolved</TabsTrigger>
          </TabsList>

          <TabsContent value="pending" className="mt-6">
            <div className="space-y-4">
              {pendingReviews.length > 0 ? (
                pendingReviews.map((review) => {
                  const chat = mockChats.find((c) => c.id === review.chatId)
                  return chat ? <ReviewCard key={review.id} review={review} chat={chat} /> : null
                })
              ) : (
                <div className="text-center py-12">
                  <ClipboardList className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                  <h3 className="text-lg font-semibold text-foreground mb-2">No pending reviews</h3>
                  <p className="text-muted-foreground">All review requests have been assigned or completed</p>
                </div>
              )}
            </div>
          </TabsContent>

          <TabsContent value="mine" className="mt-6">
            <div className="space-y-4">
              {myReviews.length > 0 ? (
                myReviews.map((review) => {
                  const chat = mockChats.find((c) => c.id === review.chatId)
                  return chat ? <ReviewCard key={review.id} review={review} chat={chat} /> : null
                })
              ) : (
                <div className="text-center py-12">
                  <ClipboardList className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                  <h3 className="text-lg font-semibold text-foreground mb-2">No active reviews</h3>
                  <p className="text-muted-foreground">You have no reviews currently assigned to you</p>
                </div>
              )}
            </div>
          </TabsContent>

          <TabsContent value="resolved" className="mt-6">
            <div className="space-y-4">
              {resolvedReviews.length > 0 ? (
                resolvedReviews.map((review) => {
                  const chat = mockChats.find((c) => c.id === review.chatId)
                  return chat ? <ReviewCard key={review.id} review={review} chat={chat} /> : null
                })
              ) : (
                <div className="text-center py-12">
                  <ClipboardList className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                  <h3 className="text-lg font-semibold text-foreground mb-2">No resolved reviews</h3>
                  <p className="text-muted-foreground">Completed reviews will appear here</p>
                </div>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </RoleGuard>
  )
}
