"use client"

import { useState } from "react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Search, MessageSquare, Calendar, Award as IdCard } from "lucide-react"
import { useRouter } from "next/navigation"
import type { Patient } from "@/lib/types"

export default function PatientsPage() {
  const router = useRouter()
  const [searchQuery, setSearchQuery] = useState("")
  const [patients, setPatients] = useState<Patient[]>([])
  const [isSearching, setIsSearching] = useState(false)

  const handleSearch = async () => {
    if (!searchQuery.trim()) return

    setIsSearching(true)
    // TODO: Replace with actual API call
    // const response = await fetch(`/api/patients/search?q=${encodeURIComponent(searchQuery)}`)
    // const data = await response.json()
    // setPatients(data.patients)

    // Mock delay for demonstration
    await new Promise((resolve) => setTimeout(resolve, 500))
    setPatients([])
    setIsSearching(false)
  }

  const handleStartChat = (patient: Patient) => {
    router.push(`/chats/new?patient=${encodeURIComponent(patient.name)}`)
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString("en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
    })
  }

  return (
    <div className="min-h-screen bg-background">
      <main className="container mx-auto p-6">
        <div className="mb-6">
          <h1 className="text-3xl font-bold mb-2">Patient Lookup</h1>
          <p className="text-muted-foreground">Search for patients by name or member ID</p>
        </div>

        <div className="mb-6">
          <div className="flex gap-2">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search by patient name or member ID..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                className="pl-10"
              />
            </div>
            <Button onClick={handleSearch} disabled={isSearching || !searchQuery.trim()}>
              {isSearching ? "Searching..." : "Search"}
            </Button>
          </div>
        </div>

        {searchQuery && patients.length === 0 && !isSearching && (
          <div className="text-center py-12">
            <p className="text-muted-foreground">
              {searchQuery ? "No patients found matching your search." : "Enter a search term to find patients."}
            </p>
          </div>
        )}

        {!searchQuery && (
          <div className="text-center py-12">
            <Search className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
            <p className="text-muted-foreground">Enter a patient name or member ID to begin your search.</p>
          </div>
        )}

        <div className="grid gap-4">
          {patients.map((patient) => (
            <Card key={patient.id} className="p-6">
              <div className="flex items-center justify-between gap-4">
                <div className="flex-1 space-y-3">
                  <div className="flex items-center gap-3">
                    <h3 className="text-xl font-semibold">{patient.name}</h3>
                    <Badge variant={patient.status === "active" ? "default" : "secondary"}>{patient.status}</Badge>
                  </div>

                  <div className="flex gap-6 text-sm text-muted-foreground">
                    <div className="flex items-center gap-2">
                      <IdCard className="h-4 w-4" />
                      <span>Member ID: {patient.memberId}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Calendar className="h-4 w-4" />
                      <span>DOB: {formatDate(patient.dateOfBirth)}</span>
                    </div>
                  </div>
                </div>

                <Button
                  onClick={() => handleStartChat(patient)}
                  className="gap-2 shrink-0"
                  disabled={patient.status === "inactive"}
                >
                  <MessageSquare className="h-4 w-4" />
                  Start Chat
                </Button>
              </div>
            </Card>
          ))}
        </div>
      </main>
    </div>
  )
}
