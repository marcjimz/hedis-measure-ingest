import { ReviewInterface } from "@/components/review-interface"
import { RoleGuard } from "@/components/role-guard"
import { mockReviewRequests, mockChats, mockCurrentUser } from "@/lib/mock-data"
import { notFound } from "next/navigation"

// For static export: pre-generate pages for all mock reviews
// Dynamic IDs will be handled by FastAPI fallback to index.html
export function generateStaticParams() {
  return mockReviewRequests.map((review) => ({
    id: review.id,
  }))
}

export default async function ReviewDetailPage({
  params,
}: {
  params: Promise<{ id: string }>
}) {
  const { id } = await params

  const review = mockReviewRequests.find((r) => r.id === id)
  if (!review) {
    notFound()
  }

  const chat = mockChats.find((c) => c.id === review.chatId)
  if (!chat) {
    notFound()
  }

  return (
    <RoleGuard allowedRoles={["reviewer"]}>
      <ReviewInterface review={review} chat={chat} currentUser={mockCurrentUser} />
    </RoleGuard>
  )
}
