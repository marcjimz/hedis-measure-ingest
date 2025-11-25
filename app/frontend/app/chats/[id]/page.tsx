import { ChatInterface } from "@/components/chat-interface"
import { NewChatForm } from "@/components/new-chat-form"
import { mockChats, mockCurrentUser } from "@/lib/mock-data"

export default async function ChatPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>
  searchParams: Promise<{ patient?: string; measure?: string }>
}) {
  const { id } = await params
  const search = await searchParams

  if (id === "create") {
    return <NewChatForm initialPatient={search.patient} />
  }

  let chat = mockChats.find((c) => c.id === id)

  if (!chat) {
    chat = {
      id,
      userId: mockCurrentUser.id,
      context: {
        patient: search.patient || undefined,
      },
      messages: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    }
  }

  return <ChatInterface chat={chat} currentUser={mockCurrentUser} showBackButton />
}
