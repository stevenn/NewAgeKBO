# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

NewAgeKBO is a **production Next.js application** for managing Belgian KBO (Crossroads Bank for Enterprises) Open Data. The system provides:

- **Admin dashboard** for monitoring database status and import jobs
- **Enterprise browser** with search, detail views, and temporal navigation
- **Automated data imports** via Restate durable workflows
- **Export functionality** for VAT-liable entities and sector analysis
- **Multi-language support** (NL/FR/DE) for all data

### Tech Stack

| Layer | Technology |
|-------|------------|
| Framework | Next.js + TypeScript |
| Database | Motherduck (hosted DuckDB) |
| Auth | Clerk (admin role required) |
| Workflows | Restate SDK (durable execution) |
| UI | TailwindCSS + shadcn/ui |
| Deployment | Vercel |

## Project Structure

```
NewAgeKBO/
├── app/                          # Next.js App Router
│   ├── admin/                    # Admin pages (protected)
│   │   ├── browse/               # Enterprise search & detail
│   │   ├── dashboard/            # Database statistics
│   │   ├── exports/              # Export management
│   │   ├── imports/              # Import job management
│   │   ├── settings/             # App settings
│   │   └── workflows/            # Restate workflow details
│   ├── api/                      # API routes
│   │   ├── admin/                # Admin APIs (imports, exports, workflows)
│   │   ├── enterprises/          # Enterprise search & detail
│   │   └── restate/              # Restate webhook handler
│   └── (auth)/                   # Clerk auth pages
├── lib/
│   ├── auth/                     # Auth helpers (checkAdminAccess)
│   ├── cache/                    # Code description caching
│   ├── config/                   # App configuration
│   ├── export/                   # Export generation logic
│   ├── import/                   # Data import processing
│   ├── kbo-client/               # KBO portal HTTP client
│   ├── motherduck/               # Database queries & connection
│   │   ├── index.ts              # Connection management
│   │   ├── enterprise-detail.ts  # Enterprise detail queries
│   │   ├── temporal-query.ts     # Point-in-time query builders
│   │   └── stats.ts              # Database statistics
│   ├── restate/                  # Durable workflow definitions
│   ├── sql/schema/               # Database DDL (11 tables)
│   ├── types/                    # TypeScript type definitions
│   ├── utils/                    # Utilities (dates, column mapping)
│   └── validation/               # Enterprise number validation
├── components/                   # React components
├── scripts/                      # CLI utilities
└── docs/                         # Reference documentation
```

## Database Architecture

### Tables (11 total)

| Table | Rows | Purpose |
|-------|------|---------|
| enterprises | 1.9M | Core entities (denormalized primary name) |
| establishments | 1.7M | Physical locations per enterprise |
| activities | 36M | NACE codes (link to nace_codes) |
| denominations | 3.3M | All business names |
| addresses | 2.8M | Locations (60% of enterprises have one) |
| contacts | 0.7M | Phone/email/web |
| nace_codes | 7.3K | NACE code descriptions |
| codes | 21.5K | Multilingual code descriptions |
| branches | ~K | Foreign entity branches |
| import_jobs | - | Import tracking |
| export_jobs | - | Export tracking |

### Temporal Tracking

All data tables use composite primary keys for temporal versioning:
- `_snapshot_date` - Date of the KBO extract
- `_extract_number` - Sequential extract number
- `_is_current` - Boolean flag for current records
- `_deleted_at_extract` - When record was deleted (if applicable)

Point-in-time queries are supported via `buildTemporalFilter()` and related helpers in `lib/motherduck/temporal-query.ts`.

### Key Data Characteristics

- **40% of enterprises have NO address** (natural persons)
- **35% have NO MAIN activity** (schema allows NULL)
- **Daily updates are tiny** (~156 changes/day, 0.0008% of dataset)
- **Entity numbers have dots**: `0588.926.194` (enterprise), `2.092.820.431` (establishment)

## Development Patterns

### Running Database Queries

When investigating data in Motherduck:

1. **Use the template script**:
   ```bash
   cp scripts/_template-query.ts scripts/investigate-xyz.ts
   # Edit the query logic
   npx tsx scripts/investigate-xyz.ts [args]
   # Delete when done
   ```

2. **Why**: The `lib/motherduck` functions require proper environment loading that doesn't work in inline scripts.

3. **Column names**: Use underscores (e.g., `_extract_number`, `enterprise_number`, `_is_current`)

### API Route Pattern

```typescript
// All admin routes follow this pattern
export async function GET(request: Request) {
  // 1. Check auth
  const authError = await checkAdminAccess()
  if (authError) return authError

  // 2. Connect to database
  const connection = await connectMotherduck()

  try {
    // 3. Execute query
    const result = await executeQuery(connection, sql)
    return NextResponse.json(result)
  } finally {
    // 4. Always close connection
    await closeMotherduck(connection)
  }
}
```

### Temporal Query Pattern

```typescript
// Build filter for current or point-in-time
const filter = extractNumber
  ? { type: 'point-in-time', extractNumber, snapshotDate }
  : { type: 'current' }

// Use with temporal query builders
const whereClause = buildTemporalFilter(filter, 'e') // 'e' is table alias
```

### Import Workflow

Imports use Restate durable workflows (`lib/restate/kbo-import-service.ts`):

1. **Download** - Fetch ZIP from KBO portal with auth
2. **Prepare** - Parse metadata, populate staging tables with `row_sequence` tracking
3. **Process** - Transform CSV → Motherduck with batched inserts (deduplication applied)
4. **Finalize** - Resolve primary names, cleanup staging tables

Progress is tracked in the `import_jobs` table and visible in the admin UI.

#### Deduplication Strategy

KBO CSV files occasionally contain duplicate rows for the same entity. The import system handles this using:

- **`row_sequence` column** in staging tables tracks original CSV row order (1-based)
- **ROW_NUMBER windowing** during INSERT selects only the last occurrence per entity
- **`ON CONFLICT DO NOTHING`** as a safety net for any remaining edge cases

This ensures that when an entity appears multiple times in an update file, the **last row wins** (highest `row_sequence`), preserving the most recent change.

Key files:
- `lib/import/batched-update.ts` - Core import logic with deduplication
- `lib/sql/schema/11_batched_import.sql` - Staging table schemas

## Environment Variables

```bash
# Required
MOTHERDUCK_TOKEN=your_token
MOTHERDUCK_DATABASE=newagekbo
CLERK_SECRET_KEY=sk_...
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_...

# KBO Portal (for imports)
KBO_USERNAME=your_username
KBO_PASSWORD=your_password

# Restate (durable workflows)
RESTATE_INGRESS_URL=http://localhost:8080
RESTATE_ADMIN_URL=http://localhost:9070
```

## Common Tasks

### Adding a new API endpoint

1. Create route in `app/api/` following the auth pattern above
2. Add types to `lib/types/` if needed
3. Add database query helpers to `lib/motherduck/` if complex

### Modifying enterprise detail page

- Data fetching: `lib/motherduck/enterprise-detail.ts`
- API route: `app/api/enterprises/[number]/route.ts`
- UI: `app/admin/browse/[number]/page.tsx`
- Types: `app/api/enterprises/[number]/route.ts` (interfaces)

### Adding new database queries

- Use `executeQuery<T>()` from `lib/motherduck/index.ts`
- For temporal queries, use helpers from `lib/motherduck/temporal-query.ts`
- Join with `codes` table for multilingual descriptions

## Reference Documentation

- **[docs/DATA_ANALYSIS.md](docs/DATA_ANALYSIS.md)** - Data analysis and design decisions
- **[docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)** - Technical implementation details
- **[docs/MOTHERDUCK_SETUP.md](docs/MOTHERDUCK_SETUP.md)** - Database setup
- **[specs/KBOCookbook_EN.md](specs/KBOCookbook_EN.md)** - Official KBO specification

## KBO Open Data Structure

The KBO Open Data is provided as monthly full files and daily update files from https://kbopub.economie.fgov.be/kbo-open-data.

### CSV Files in ZIP Archives

1. **meta.csv** - Metadata (snapshot date, extract number)
2. **code.csv** - Code descriptions (multi-language)
3. **enterprise.csv** - Enterprise records
4. **establishment.csv** - Establishment records
5. **denomination.csv** - Business names
6. **address.csv** - Physical addresses
7. **contact.csv** - Contact details
8. **activity.csv** - NACE activity codes
9. **branch.csv** - Foreign entity branches

### Update File Pattern

Daily updates use delete-then-insert:
- `*_delete.csv` - Entity numbers to mark as deleted
- `*_insert.csv` - Complete replacement data (not diffs)

### Identifiers

- **Enterprise**: `9999.999.999` (10 digits with dots)
- **Establishment**: `9.999.999.999` (10 digits, leading single digit)
