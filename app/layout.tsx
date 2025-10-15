import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'KBO for the New Age',
  description: 'Modern KBO Open Data administration and API',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
