import { NextResponse } from 'next/server'

export interface MachineClient {
  clientId: string
  name: string
  allowedEndpoints?: string[]
}

/**
 * Validates machine API requests using API key authentication
 * Returns the client config if valid, or a NextResponse error if not
 *
 * API keys are configured via MACHINE_API_KEYS environment variable:
 * { "sk_machine_xxx": { "clientId": "openclaw", "name": "OpenClaw Skill" } }
 */
export async function checkMachineAccess(
  request: Request
): Promise<MachineClient | NextResponse> {
  const apiKey = request.headers.get('X-API-Key')

  if (!apiKey) {
    return NextResponse.json(
      { error: 'Missing X-API-Key header' },
      { status: 401 }
    )
  }

  const machineKeys = process.env.MACHINE_API_KEYS
  if (!machineKeys) {
    console.error('MACHINE_API_KEYS environment variable not configured')
    return NextResponse.json(
      { error: 'Machine API not configured' },
      { status: 500 }
    )
  }

  let keyConfig: Record<string, MachineClient>
  try {
    keyConfig = JSON.parse(machineKeys)
  } catch {
    console.error('Invalid MACHINE_API_KEYS JSON')
    return NextResponse.json(
      { error: 'Machine API misconfigured' },
      { status: 500 }
    )
  }

  const client = keyConfig[apiKey]
  if (!client) {
    return NextResponse.json({ error: 'Invalid API key' }, { status: 401 })
  }

  return client
}

/**
 * Type guard to check if the result is an error response
 */
export function isMachineAuthError(
  result: MachineClient | NextResponse
): result is NextResponse {
  return result instanceof NextResponse
}
