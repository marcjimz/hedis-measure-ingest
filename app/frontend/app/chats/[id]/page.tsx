import { ChatInterface } from "@/components/chat-interface"
import { NewChatForm } from "@/components/new-chat-form"
import { mockChats, mockCurrentUser } from "@/lib/mock-data"

// For static export: pre-generate pages for all mock chats
// Dynamic IDs will be handled by FastAPI fallback to index.html
export function generateStaticParams() {
  return mockChats.map((chat) => ({
    id: chat.id,
  }))
}

export default async function ChatPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>
  searchParams: Promise<{ patient?: string; measure?: string }>
}) {
  const { id } = await params
  const search = await searchParams
  const patient = search.patient
  const measure = search.measure

  if (id === "create") {
    return <NewChatForm initialPatient={patient} />
  }

  let chat = mockChats.find((c) => c.id === id)

  if (!chat) {
    chat = {
      id,
      userId: mockCurrentUser.id,
      context: {
        patient: patient || undefined,
      },
      messages: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    }
  }

  return <ChatInterface chat={chat} currentUser={mockCurrentUser} showBackButton />
}
