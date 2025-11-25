import { MessageSquare, ClipboardList, Users, FileText } from "lucide-react"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { mockCurrentUser } from "@/lib/mock-data"
import { isFeatureEnabled } from "@/lib/feature-flags"

export function Header() {
  const isReviewer = mockCurrentUser.role === "reviewer"

  const chatsEnabled = isFeatureEnabled("chats")
  const measuresEnabled = isFeatureEnabled("measures")
  const patientsEnabled = isFeatureEnabled("patients")
  const reviewsEnabled = isFeatureEnabled("reviews")

  return (
    <header className="sticky top-0 z-50 w-full border-b border-border bg-secondary">
      <div className="flex h-16 items-center gap-4 px-6">
        <Link href="/" className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center">
            <svg viewBox="0 0 32 32" className="h-10 w-10" fill="none">
              <path d="M16 4L8 8L16 12L24 8L16 4Z" fill="#FF3621" stroke="#FF3621" strokeWidth="1" />
              <path d="M8 12L16 16L24 12" stroke="#FF3621" strokeWidth="1.5" fill="none" />
              <path d="M8 16L16 20L24 16" stroke="#FF3621" strokeWidth="1.5" fill="none" />
              <path d="M8 20L16 24L24 20" stroke="#FF3621" strokeWidth="1.5" fill="none" />
              <path d="M16 24L8 28L16 32L24 28L16 24Z" fill="#FF3621" stroke="#FF3621" strokeWidth="1" opacity="0.8" />
            </svg>
          </div>
          <div>
            <h1 className="text-lg font-semibold text-secondary-foreground">HEDIS Chat Agent</h1>
            <p className="text-xs text-secondary-foreground/70">Powered by Databricks</p>
          </div>
        </Link>

        <nav className="ml-8 flex gap-2">
          {chatsEnabled ? (
            <Link href="/chats">
              <Button variant="ghost" className="gap-2 text-secondary-foreground hover:text-secondary-foreground">
                <MessageSquare className="h-4 w-4" />
                Chats
              </Button>
            </Link>
          ) : (
            <Button
              variant="ghost"
              className="gap-2 text-secondary-foreground/40 cursor-not-allowed"
              disabled
              title="Coming soon"
            >
              <MessageSquare className="h-4 w-4" />
              Chats
            </Button>
          )}

          {measuresEnabled ? (
            <Link href="/measures">
              <Button variant="ghost" className="gap-2 text-secondary-foreground hover:text-secondary-foreground">
                <FileText className="h-4 w-4" />
                Measures
              </Button>
            </Link>
          ) : (
            <Button
              variant="ghost"
              className="gap-2 text-secondary-foreground/40 cursor-not-allowed"
              disabled
              title="Coming soon"
            >
              <FileText className="h-4 w-4" />
              Measures
            </Button>
          )}

          {patientsEnabled ? (
            <Link href="/patients">
              <Button variant="ghost" className="gap-2 text-secondary-foreground hover:text-secondary-foreground">
                <Users className="h-4 w-4" />
                Patients
              </Button>
            </Link>
          ) : (
            <Button
              variant="ghost"
              className="gap-2 text-secondary-foreground/40 cursor-not-allowed"
              disabled
              title="Coming soon"
            >
              <Users className="h-4 w-4" />
              Patients
            </Button>
          )}

          {isReviewer &&
            (reviewsEnabled ? (
              <Link href="/reviews">
                <Button variant="ghost" className="gap-2 text-secondary-foreground hover:text-secondary-foreground">
                  <ClipboardList className="h-4 w-4" />
                  Reviews
                </Button>
              </Link>
            ) : (
              <Button
                variant="ghost"
                className="gap-2 text-secondary-foreground/40 cursor-not-allowed"
                disabled
                title="Coming soon"
              >
                <ClipboardList className="h-4 w-4" />
                Reviews
              </Button>
            ))}
        </nav>
      </div>
    </header>
  )
}
