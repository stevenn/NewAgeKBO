import { NextResponse } from 'next/server'
import { auth } from '@clerk/nextjs/server'

export async function GET() {
  try {
    // Check authentication
    const { userId } = await auth()
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const config = {
      motherduckDatabase: process.env.MOTHERDUCK_DATABASE || 'kbo',
    }

    return NextResponse.json(config)
  } catch (error) {
    console.error('Failed to get config:', error)
    return NextResponse.json({ error: 'Failed to get config' }, { status: 500 })
  }
}
