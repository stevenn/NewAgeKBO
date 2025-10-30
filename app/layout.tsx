import type { Metadata } from 'next'
import { ClerkProvider } from '@clerk/nextjs'
import { LanguageProvider } from '@/lib/contexts/language-context'
import './globals.css'

export const metadata: Metadata = {
  title: 'KBO/BCE/CBE for the New Age',
  description: 'Modern CBE Open Data administration and API',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <ClerkProvider>
      <html lang="en">
        <body>
          <LanguageProvider>
            {children}
          </LanguageProvider>
        </body>
      </html>
    </ClerkProvider>
  )
}
