"use client"

import type React from "react"

import { mockCurrentUser } from "@/lib/mock-data"
import { hasRole } from "@/lib/auth"
import type { UserRole } from "@/lib/types"
import { AlertCircle } from "lucide-react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import Link from "next/link"

interface RoleGuardProps {
  children: React.ReactNode
  allowedRoles: UserRole[]
  fallback?: React.ReactNode
}

export function RoleGuard({ children, allowedRoles, fallback }: RoleGuardProps) {
  const currentUser = mockCurrentUser
  const hasAccess = allowedRoles.some((role) => hasRole(currentUser, role))

  if (!hasAccess) {
    return (
      fallback || (
        <div className="container mx-auto p-6 max-w-2xl">
          <Card className="p-8 text-center">
            <AlertCircle className="h-12 w-12 text-destructive mx-auto mb-4" />
            <h2 className="text-2xl font-bold text-foreground mb-2">Access Denied</h2>
            <p className="text-muted-foreground mb-6">
              You don't have permission to access this page. This area is restricted to reviewers only.
            </p>
            <Link href="/chats">
              <Button>Go to Chats</Button>
            </Link>
          </Card>
        </div>
      )
    )
  }

  return <>{children}</>
}
