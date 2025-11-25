import Link from "next/link"
import { MessageSquare, FileText, Users } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { isFeatureEnabled } from "@/lib/feature-flags"

export default function Home() {
  const isPatientsEnabled = isFeatureEnabled("patients")

  return (
    <div className="min-h-[calc(100vh-4rem)] flex items-center justify-center p-6">
      <div className="max-w-4xl w-full space-y-8">
        {/* Hero Section */}
        <div className="text-center space-y-4">
          <h1 className="text-4xl font-bold text-balance">Welcome to HEDIS Chat Agent</h1>
          <p className="text-lg text-muted-foreground text-balance">
            Your AI-powered assistant for NCQA measure compliance and patient care coordination
          </p>
        </div>

        {/* About Section */}
        <Card className="p-6 space-y-4">
          <h2 className="text-2xl font-semibold">How to Navigate</h2>
          <div className="space-y-3 text-muted-foreground">
            <p>
              <strong className="text-foreground">Measures:</strong> Browse and search through NCQA healthcare quality
              measures. View detailed specifications including denominators, numerators, and exclusions for each
              measure.
            </p>
            <p>
              <strong className="text-foreground">Patients:</strong> Look up patient information and demographics.
              Quickly find patients to start contextual conversations about their care.
            </p>
            <p>
              <strong className="text-foreground">Chats:</strong> Start AI-powered conversations about HEDIS measures
              and patient care. Get instant assistance with measure compliance questions, or request human review for
              complex scenarios.
            </p>
          </div>
        </Card>

        {/* Action Buttons */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Link href="/measures" className="block">
            <Card className="p-6 hover:bg-accent transition-colors cursor-pointer h-full">
              <div className="flex flex-col items-center text-center space-y-3">
                <div className="h-12 w-12 rounded-full bg-primary/10 flex items-center justify-center">
                  <FileText className="h-6 w-6 text-primary" />
                </div>
                <div className="space-y-1">
                  <h3 className="font-semibold">Measures</h3>
                  <p className="text-sm text-muted-foreground">Browse NCQA measures</p>
                </div>
                <Button className="w-full">View Measures</Button>
              </div>
            </Card>
          </Link>

          <div className={isPatientsEnabled ? "block" : "cursor-not-allowed"}>
            <Card
              className={`p-6 h-full ${isPatientsEnabled ? "hover:bg-accent transition-colors cursor-pointer" : "opacity-60 cursor-not-allowed"}`}
            >
              <div className="flex flex-col items-center text-center space-y-3">
                <div
                  className={`h-12 w-12 rounded-full flex items-center justify-center ${isPatientsEnabled ? "bg-primary/10" : "bg-muted"}`}
                >
                  <Users className={`h-6 w-6 ${isPatientsEnabled ? "text-primary" : "text-muted-foreground"}`} />
                </div>
                <div className="space-y-1">
                  <h3 className="font-semibold">Patients</h3>
                  <p className="text-sm text-muted-foreground">Search patient records</p>
                </div>
                {isPatientsEnabled ? (
                  <Button className="w-full" asChild>
                    <Link href="/patients">Find Patients</Link>
                  </Button>
                ) : (
                  <div className="w-full px-4 py-2 text-sm font-medium text-muted-foreground bg-muted rounded-md">
                    Coming Soon
                  </div>
                )}
              </div>
            </Card>
          </div>

          <Link href="/chats" className="block">
            <Card className="p-6 hover:bg-accent transition-colors cursor-pointer h-full">
              <div className="flex flex-col items-center text-center space-y-3">
                <div className="h-12 w-12 rounded-full bg-primary/10 flex items-center justify-center">
                  <MessageSquare className="h-6 w-6 text-primary" />
                </div>
                <div className="space-y-1">
                  <h3 className="font-semibold">Chats</h3>
                  <p className="text-sm text-muted-foreground">Start AI conversations</p>
                </div>
                <Button className="w-full">Start Chat</Button>
              </div>
            </Card>
          </Link>
        </div>

        {/* Footer Note */}
        <p className="text-center text-sm text-muted-foreground">Powered by Databricks</p>
      </div>
    </div>
  )
}
