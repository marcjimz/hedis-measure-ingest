"use client"

import { useState } from "react"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import { mockPatients } from "@/lib/mock-data"
import { MessageSquare, Sparkles } from "lucide-react"
import { useRouter } from "next/navigation"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { isFeatureEnabled } from "@/lib/feature-flags"

interface NewChatFormProps {
  initialPatient?: string
}

export function NewChatForm({ initialPatient }: NewChatFormProps) {
  const router = useRouter()
  const [patient, setPatient] = useState(initialPatient || "")
  const [customPatient, setCustomPatient] = useState("")

  const patientsEnabled = isFeatureEnabled("patients")

  const handleStartChat = () => {
    const patientValue = patientsEnabled && patient !== "none" ? (patient === "custom" ? customPatient : patient) : ""

    // Generate a new chat ID
    const newChatId = `chat-${Date.now()}`

    const params = new URLSearchParams()
    if (patientValue) {
      params.set("patient", patientValue)
    }

    const queryString = params.toString()
    router.push(`/chats/${newChatId}${queryString ? `?${queryString}` : ""}`)
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-6 max-w-2xl">
        <div className="mb-8 text-center">
          <div className="flex items-center justify-center gap-2 mb-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
              <MessageSquare className="h-6 w-6 text-primary" />
            </div>
          </div>
          <h1 className="text-3xl font-bold mb-2">Start a New Chat</h1>
          <p className="text-muted-foreground">Get AI-powered assistance with HEDIS measures and patient care</p>
        </div>

        <Card className="p-6">
          <div className="space-y-6">
            <div className="space-y-2 relative">
              <Label htmlFor="patient" className={!patientsEnabled ? "text-muted-foreground" : ""}>
                Patient (Optional) {!patientsEnabled && <span className="text-xs">(Coming Soon)</span>}
              </Label>
              <Select value={patient} onValueChange={setPatient} disabled={!patientsEnabled}>
                <SelectTrigger id="patient" className={!patientsEnabled ? "opacity-50 cursor-not-allowed" : ""}>
                  <SelectValue
                    placeholder={patientsEnabled ? "Select a patient or enter custom..." : "Feature coming soon..."}
                  />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">None</SelectItem>
                  {mockPatients.map((p) => (
                    <SelectItem key={p.id} value={p.name}>
                      {p.name} - {p.mrn}
                    </SelectItem>
                  ))}
                  <SelectItem value="custom">Enter custom patient...</SelectItem>
                </SelectContent>
              </Select>
              {patient === "custom" && patientsEnabled && (
                <Input
                  placeholder="Enter patient name"
                  value={customPatient}
                  onChange={(e) => setCustomPatient(e.target.value)}
                  className="mt-2"
                />
              )}
              <p className="text-xs text-muted-foreground">
                {patientsEnabled
                  ? "Select a patient to provide context for your conversation"
                  : "Patient selection will be available in the next release"}
              </p>
            </div>

            <div className="pt-4 border-t">
              <Button onClick={handleStartChat} className="w-full gap-2" size="lg">
                <Sparkles className="h-4 w-4" />
                Start Chat
              </Button>
            </div>
          </div>
        </Card>

        <div className="mt-6 text-center">
          <p className="text-sm text-muted-foreground">
            Ask any questions about HEDIS measures and get AI-powered guidance
          </p>
        </div>
      </div>
    </div>
  )
}
