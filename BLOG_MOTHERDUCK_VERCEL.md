# Connecting to Motherduck from Vercel: A Serverless Journey

**TL;DR:** Connecting to Motherduck from a Next.js app on Vercel requires a different approach than local development. You can't use the standard `duckdb` npm package - you need `@duckdb/node-api` with a special in-memory + ATTACH configuration. This took us 11 commits and a lot of head-scratching to figure out.

---

## The Problem: It Works Locally... But Not on Vercel

You've built a beautiful Next.js app that connects to Motherduck (the cloud-hosted DuckDB service). It works perfectly on your local machine. You deploy to Vercel, and... 💥

```
Error: IO Error: Can't find the home directory at ''
Specify a home directory using the SET home_directory='/path/to/dir' option.
```

Or maybe:

```
Error: /lib64/libstdc++.so.6: version 'GLIBCXX_3.4.30' not found
```

Sound familiar? You're not alone. Here's what we learned the hard way.

---

## The Architectural Mistake: Local != Serverless

### What Works Locally

Locally, you're probably doing something like this:

```typescript
import * as duckdb from 'duckdb'

// Create a local DuckDB database file
const db = new duckdb.Database('/tmp/my_db.db', (err) => {
  if (err) throw err

  // Connect to Motherduck
  db.run(`ATTACH 'md:my_database?motherduck_token=${token}' AS md`, (err) => {
    if (err) throw err
    // Use the database...
  })
})
```

This works because:
- You have a writable filesystem
- The DuckDB native binaries match your OS
- Extensions can be downloaded and cached
- You have a persistent home directory

### Why This Fails on Vercel

Vercel's serverless functions are:
- **Read-only filesystem** (except `/tmp`, which is ephemeral)
- **Different environment** (Amazon Linux 2 vs your Mac/Windows)
- **Cold starts** (no persistent state between invocations)
- **Limited execution time** (can't download large extensions on every request)

The `duckdb` npm package was built for traditional server environments, not serverless. It needs:
- A writable home directory for metadata
- Ability to download and cache extensions
- Native bindings compiled for the target OS

---

## The Solution: A Lucky Find

After several failed attempts, we stumbled upon a [Japanese developer's blog post](https://zenn.dev/terrierscript/articles/2025-09-10-duckdb-node-api-next-js-vercel) that documented their success deploying DuckDB to Vercel.

### Key Discovery: Use `@duckdb/node-api`

There's a **newer DuckDB client** that's designed for modern environments:

```bash
npm install @duckdb/node-api@1.4.1-r.4
```

**Why this package is better for serverless:**
- Promise-based API (no callbacks)
- Better TypeScript support
- Pre-built binaries for multiple platforms
- More predictable behavior in containerized environments

**Important:** Use version `1.4.1-r.4` or later. Earlier versions like `1.3.4-alpha.27` have GLIBC compatibility issues on Vercel.

---

## Trial and Error: Getting the Configuration Right

Even with the right package, we hit several walls. Here's what we learned:

### ❌ Attempt 1: Direct Connection

```typescript
// This doesn't work in serverless
const instance = await DuckDBInstance.create('md:my_database', {
  motherduck_token: token
})
```

**Error:** `Can't find the home directory`

The problem: DuckDB checks for a home directory during initialization, before you can configure it.

### ❌ Attempt 2: Set Home Directory After Connection

```typescript
const instance = await DuckDBInstance.create('md:my_database', {
  motherduck_token: token
})
const connection = await instance.connect()
await connection.run("SET home_directory='/tmp'") // Too late!
```

**Error:** Still `Can't find the home directory`

The problem: The Motherduck extension loads during `create()`, which happens before `SET` commands.

### ❌ Attempt 3: Set Home Directory in Config

```typescript
const instance = await DuckDBInstance.create('md:my_database', {
  motherduck_token: token,
  home_directory: '/tmp' // Not a valid config option!
})
```

**Error:** `Failed to set config`

The problem: `home_directory` isn't a valid configuration parameter for `DuckDBInstance.create()`.

### ✅ The Working Solution: In-Memory + ATTACH

The trick is to create an in-memory database first, configure everything, then attach to Motherduck:

```typescript
// 1. Create in-memory database (no filesystem needed)
const instance = await DuckDBInstance.create(':memory:')
const connection = await instance.connect()

// 2. Set ALL directory configs BEFORE loading Motherduck extension
await connection.run("SET home_directory='/tmp'")
await connection.run("SET extension_directory='/tmp/.duckdb/extensions'")
await connection.run("SET temp_directory='/tmp'")

// 3. Set Motherduck token as environment variable
process.env.motherduck_token = token

// 4. ATTACH to Motherduck (extension auto-installs to /tmp)
await connection.run(`ATTACH 'md:${database}' AS md`)

// 5. USE the attached database
await connection.run(`USE md`)
```

**Why this works:**
- `:memory:` database doesn't need any filesystem access
- Directory configs are set before the Motherduck extension loads
- Token is provided via environment variable (standard DuckDB pattern)
- ATTACH happens after all configuration is in place
- Extensions get installed to `/tmp` (the only writable location)

---

## The Complete Implementation

Here's everything you need to get Motherduck working on Vercel:

### 1. Install Dependencies

```bash
npm install @duckdb/node-api@1.4.1-r.4
```

### 2. Configure Next.js

Add to `next.config.ts`:

```typescript
const nextConfig = {
  // Prevent webpack from bundling native modules
  serverExternalPackages: ['@duckdb/node-api'],
}
```

### 3. Connection Helper (`lib/motherduck.ts`)

```typescript
import { DuckDBInstance } from '@duckdb/node-api'
import type { DuckDBConnection } from '@duckdb/node-api'

export async function connectMotherduck(): Promise<DuckDBConnection> {
  const token = process.env.MOTHERDUCK_TOKEN
  const database = process.env.MOTHERDUCK_DATABASE || 'my_db'

  if (!token) {
    throw new Error('MOTHERDUCK_TOKEN is required')
  }

  // Create in-memory database first
  const instance = await DuckDBInstance.create(':memory:')
  const connection = await instance.connect()

  // Configure directories BEFORE Motherduck extension loads
  await connection.run("SET home_directory='/tmp'")
  await connection.run("SET extension_directory='/tmp/.duckdb/extensions'")
  await connection.run("SET temp_directory='/tmp'")

  // Set token as environment variable
  process.env.motherduck_token = token

  // Attach to Motherduck
  await connection.run(`ATTACH 'md:${database}' AS md`)
  await connection.run(`USE md`)

  return connection
}

export async function executeQuery<T = unknown>(
  connection: DuckDBConnection,
  sql: string
): Promise<T[]> {
  const result = await connection.run(sql)
  const chunks = await result.fetchAllChunks()
  const columnNames = result.columnNames()

  const rows: T[] = []
  for (const chunk of chunks) {
    const rowArrays = chunk.getRows()
    for (const rowArray of rowArrays) {
      const rowObject: Record<string, unknown> = {}
      columnNames.forEach((colName, idx) => {
        rowObject[colName] = rowArray[idx]
      })
      rows.push(rowObject as T)
    }
  }

  return rows
}

export function closeMotherduck(connection: DuckDBConnection): void {
  connection.closeSync()
}
```

### 4. API Route Example

```typescript
// app/api/data/route.ts
import { NextResponse } from 'next/server'
import { connectMotherduck, executeQuery, closeMotherduck } from '@/lib/motherduck'

export async function GET() {
  const connection = await connectMotherduck()

  try {
    const results = await executeQuery(
      connection,
      'SELECT * FROM my_table LIMIT 10'
    )
    return NextResponse.json({ results })
  } finally {
    closeMotherduck(connection)
  }
}
```

### 5. Environment Variables

In Vercel, set:

```bash
MOTHERDUCK_TOKEN=your_token_here
MOTHERDUCK_DATABASE=your_database_name
```

### 6. Force Dynamic Rendering

For any pages that query Motherduck during SSR:

```typescript
// app/dashboard/page.tsx
export const dynamic = 'force-dynamic' // Prevent static generation

export default async function DashboardPage() {
  const connection = await connectMotherduck()
  // ... use connection
}
```

---

## Key Differences: Local vs Vercel

Here's a quick comparison of what changed:

| Aspect | Local Development | Vercel Serverless |
|--------|------------------|-------------------|
| **Package** | `duckdb` | `@duckdb/node-api` |
| **Connection** | Direct to `md:database` | In-memory + ATTACH |
| **API Style** | Callbacks | Promises |
| **Directory Config** | Automatic | Manual (`/tmp`) |
| **Token Setup** | Connection string | Environment variable |
| **Extension Install** | Cached locally | Auto-installed to `/tmp` |

### Before (Local Only)

```typescript
import * as duckdb from 'duckdb'

const db = new duckdb.Database('/path/to/db.db', (err) => {
  db.run(`USE md:${database}?motherduck_token=${token}`, callback)
})
```

### After (Works Everywhere)

```typescript
import { DuckDBInstance } from '@duckdb/node-api'

const instance = await DuckDBInstance.create(':memory:')
const connection = await instance.connect()
await connection.run("SET home_directory='/tmp'")
await connection.run("SET extension_directory='/tmp/.duckdb/extensions'")
await connection.run("SET temp_directory='/tmp'")
process.env.motherduck_token = token
await connection.run(`ATTACH 'md:${database}' AS md`)
await connection.run(`USE md`)
```

---

## Troubleshooting

### Build Error: "Cannot find module 'duckdb'"

Make sure you've added `@duckdb/node-api` to `serverExternalPackages` in `next.config.ts`.

### Runtime Error: "GLIBCXX_3.4.30 not found"

You're using an older version. Update to `@duckdb/node-api@1.4.1-r.4` or later.

### Error: "Extension motherduck.duckdb_extension not found"

The Motherduck extension is trying to initialize before directories are set. Make sure you:
1. Create `:memory:` database first
2. Set all three directories (`home_directory`, `extension_directory`, `temp_directory`)
3. THEN do the ATTACH

### Query Works Locally But Times Out on Vercel

Vercel functions have a 10-second timeout on the hobby plan (60s on Pro). For heavy queries:
- Use Vercel Pro for longer timeouts
- Optimize your queries
- Consider caching results
- Use incremental static regeneration where possible

---

## Performance Notes

**Cold Start Impact:**
- First request: ~2-3 seconds (extension installation)
- Subsequent requests: ~200-500ms (extension cached in `/tmp`)
- `/tmp` persists across warm invocations

**Best Practices:**
- Close connections after use to free resources
- Use connection pooling if making multiple queries
- Consider Edge Functions for geo-distribution (if they support native modules)

---

## Conclusion

Getting DuckDB/Motherduck working on Vercel isn't straightforward, but it's absolutely possible. The key insights:

1. **Use the new package**: `@duckdb/node-api` instead of `duckdb`
2. **In-memory first**: Create `:memory:` database before attaching to Motherduck
3. **Configure early**: Set all directory paths before loading the Motherduck extension
4. **Environment variables**: Use `process.env.motherduck_token` instead of connection string parameters

This approach works not just on Vercel, but on any serverless platform with similar constraints (AWS Lambda, Cloudflare Workers, etc.).

**Total journey:** 11 commits, several hours of debugging, one lucky Google search, and a lot of reading DuckDB source code. Hopefully this saves you some time!

---

## Resources

- [DuckDB Node Neo Client Docs](https://duckdb.org/docs/stable/clients/node_neo/overview)
- [Motherduck Documentation](https://motherduck.com/docs)
- [Original Japanese Blog Post](https://zenn.dev/terrierscript/articles/2025-09-10-duckdb-node-api-next-js-vercel) (the hero we needed)
- [@duckdb/node-api on npm](https://www.npmjs.com/package/@duckdb/node-api)

---

*Have questions or run into issues? Feel free to open an issue or reach out. We've been through the pain so you don't have to!* 🦆
