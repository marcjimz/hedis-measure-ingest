"use client"

import { useState, useMemo } from "react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { mockNCQAMeasures } from "@/lib/mock-data"
import { Search } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

export default function MeasuresPage() {
  const [searchQuery, setSearchQuery] = useState("")
  const [expandedMeasure, setExpandedMeasure] = useState<string | null>(null)
  const [selectedYear, setSelectedYear] = useState<string>("all")
  const [selectedVersion, setSelectedVersion] = useState<string>("all")

  const availableYears = useMemo(() => {
    const years = Array.from(new Set(mockNCQAMeasures.map((m) => m.effective_year))).sort((a, b) => b - a)
    return years
  }, [])

  const availableVersions = useMemo(() => {
    if (selectedYear === "all") {
      return Array.from(new Set(mockNCQAMeasures.map((m) => m.version).filter(Boolean))).sort()
    }
    const versions = mockNCQAMeasures
      .filter((m) => m.effective_year === Number.parseInt(selectedYear))
      .map((m) => m.version)
      .filter(Boolean)
    return Array.from(new Set(versions)).sort()
  }, [selectedYear])

  const handleYearChange = (year: string) => {
    setSelectedYear(year)
    setSelectedVersion("all")
  }

  const filteredMeasures = mockNCQAMeasures.filter((measure) => {
    const matchesSearch =
      measure.measure.toLowerCase().includes(searchQuery.toLowerCase()) ||
      measure.specifications.toLowerCase().includes(searchQuery.toLowerCase())

    const matchesYear = selectedYear === "all" || measure.effective_year === Number.parseInt(selectedYear)

    const matchesVersion = selectedVersion === "all" || measure.version === selectedVersion

    return matchesSearch && matchesYear && matchesVersion
  })

  return (
    <div className="min-h-screen bg-background">
      <main className="container mx-auto p-6">
        <div className="mb-6">
          <h1 className="text-3xl font-bold mb-2">NCQA Measures</h1>
          <p className="text-muted-foreground">Search and explore HEDIS quality measures</p>
        </div>

        <div className="mb-6 space-y-4">
          <div className="flex gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  placeholder="Search measures by name or specification..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <div className="w-40">
              <Select value={selectedYear} onValueChange={handleYearChange}>
                <SelectTrigger>
                  <SelectValue placeholder="Year" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Years</SelectItem>
                  {availableYears.map((year) => (
                    <SelectItem key={year} value={year.toString()}>
                      {year}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-40">
              <Select value={selectedVersion} onValueChange={setSelectedVersion} disabled={selectedYear === "all"}>
                <SelectTrigger>
                  <SelectValue placeholder="Version" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Versions</SelectItem>
                  {availableVersions.map((version) => (
                    <SelectItem key={version} value={version!}>
                      {version}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          {selectedYear !== "all" && (
            <p className="text-sm text-muted-foreground">
              Showing {filteredMeasures.length} measure{filteredMeasures.length !== 1 ? "s" : ""} for {selectedYear}
              {selectedVersion !== "all" && ` (${selectedVersion})`}
            </p>
          )}
        </div>

        <div className="space-y-4">
          {filteredMeasures.map((measure, idx) => (
            <Card key={`${measure.measure}-${measure.effective_year}-${measure.version}-${idx}`} className="p-6">
              <div className="space-y-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <h3 className="text-xl font-semibold">{measure.measure}</h3>
                      <Badge variant="outline">{measure.effective_year}</Badge>
                      {measure.version && <Badge variant="secondary">{measure.version}</Badge>}
                    </div>
                    <p className="text-sm text-muted-foreground">{measure.specifications}</p>
                  </div>
                </div>

                {expandedMeasure === `${measure.measure}-${measure.effective_year}-${idx}` && (
                  <div className="grid gap-4 pt-4 border-t">
                    <div>
                      <h4 className="font-medium mb-2">Initial Population</h4>
                      <p className="text-sm text-muted-foreground">{measure.initial_pop}</p>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Denominator</h4>
                      <ul className="list-disc list-inside space-y-1">
                        {measure.denominator.map((item, idx) => (
                          <li key={idx} className="text-sm text-muted-foreground">
                            {item}
                          </li>
                        ))}
                      </ul>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Numerator</h4>
                      <ul className="list-disc list-inside space-y-1">
                        {measure.numerator.map((item, idx) => (
                          <li key={idx} className="text-sm text-muted-foreground">
                            {item}
                          </li>
                        ))}
                      </ul>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Exclusions</h4>
                      <ul className="list-disc list-inside space-y-1">
                        {measure.exclusion.map((item, idx) => (
                          <li key={idx} className="text-sm text-muted-foreground">
                            {item}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() =>
                    setExpandedMeasure(
                      expandedMeasure === `${measure.measure}-${measure.effective_year}-${idx}`
                        ? null
                        : `${measure.measure}-${measure.effective_year}-${idx}`,
                    )
                  }
                  className="w-full"
                >
                  {expandedMeasure === `${measure.measure}-${measure.effective_year}-${idx}`
                    ? "Show Less"
                    : "Show Details"}
                </Button>
              </div>
            </Card>
          ))}

          {filteredMeasures.length === 0 && (
            <div className="text-center py-12">
              <p className="text-muted-foreground">No measures found matching your search.</p>
            </div>
          )}
        </div>
      </main>
    </div>
  )
}
