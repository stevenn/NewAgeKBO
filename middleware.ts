import { clerkMiddleware, createRouteMatcher } from '@clerk/nextjs/server'
import { NextResponse } from 'next/server'

// Define protected routes (admin routes require authentication)
const isProtectedRoute = createRouteMatcher([
  '/admin(.*)',
])

// Routes that should skip Clerk entirely (called by external services)
const isPublicApiRoute = createRouteMatcher([
  '/api/restate(.*)',
])

export default clerkMiddleware(async (auth, req) => {
  // Skip Clerk for public API routes (called by Restate server)
  if (isPublicApiRoute(req)) {
    return NextResponse.next()
  }

  // Protect admin routes
  if (isProtectedRoute(req)) {
    const { sessionClaims } = await auth.protect()

    // After configuring custom session claims in Clerk Dashboard,
    // metadata will be available in sessionClaims
    const metadata = sessionClaims?.metadata as { role?: string } | undefined

    if (metadata?.role !== 'admin') {
      return NextResponse.redirect(new URL('/unauthorized', req.url))
    }
  }
})

export const config = {
  matcher: [
    // Skip Next.js internals and all static files, unless found in search params
    '/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)',
    // Always run for API routes
    '/(api|trpc)(.*)',
  ],
}
