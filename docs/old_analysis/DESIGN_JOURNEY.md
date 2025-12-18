# Design Journey: Building NewAgeKBO

How an evening side project became a case study in AI-assisted design

---

## The Vision

Create a modern platform for Belgian KBO (company registry) Open Data. The challenge: 2M enterprises, temporal tracking, efficient storage, daily updates. Keep it simple and maintainable.

## The Journey: Analysis Before Implementation

This project went through multiple design iterations, with critical corrections at each step. No code was written until the architecture was solid.

---

## Act I: Initial Exploration

**Starting Point**: Specs document, data structure diagram, sample data, third-party API reference

**First Steps**:
- Read KBO specifications (English + Dutch)
- Analyze sample data with DuckDB
- Create `analysis-queries.sql` to explore patterns
- Test Parquet compression ratios

**Key Discoveries**:
- 1.9M enterprises, 46M total rows
- 40% of enterprises have NO address (natural persons)
- 35% have NO MAIN activity
- Daily updates tiny (~156 changes/day)
- Parquet ZSTD: 21x compression (1.5GB → 71MB)

**Initial Schema**: Proposed tables for enterprises, activities, addresses, denominations, with temporal tracking using `_is_current` flag.

---

## Act II: Reality Check

### "Check the Sample Data First"

**AI**: *was provided with a sample API spec for a similar service and proposed tables for financial data, NSSO info, board members*

**User**: "For assumptions on missing tables, first make sure these exist in the @sampledata folder - the 3rd party service probably uses additional data which cannot be found in the open data set."

**Lesson**: Verified against actual CSVs. Found only 9 files. Saved building infrastructure for non-existent data.

**Result**: Added "Data Scope & Limitations" section documenting what's in KBO Open Data (9 CSV files) vs what's not (financial data, NSSO, board members - all external sources).

---

## Act III: Critical Deep Dive

### "Think Super Hard"

**User**: "Make two critical assessments: 1) Make sure all SQL works on Motherduck and DuckDB. 2) Do thorough review of multi-linguality."

**SQL Compatibility Investigation**:
- PostgreSQL GIN indexes → Don't exist in DuckDB ❌
- `to_tsvector()` / `to_tsquery()` → Not in DuckDB ❌
- `text_pattern_ops` → PostgreSQL-specific ❌
- Motherduck doesn't use indexes for acceleration ⚠️

**Multi-Language Deep Dive**:
- Checked actual code.csv data
- **Found critical bug**: Documentation said `0=FR, 1=DE, 2=NL, 3=EN`
- **Reality**: `0=unknown, 1=FR, 2=NL, 3=DE, 4=EN`

**Fixes Applied**:
- Fixed ALL language code references throughout
- Replaced PostgreSQL syntax with DuckDB-compatible
- Added German language support (`primary_name_de`)
- Changed to store codes only, JOIN for descriptions
- Added comprehensive Multi-Language Strategy section

---

## Act IV: The Index Question

### "Why Do You Keep Insisting on Indexes?"

**User**: "Before we go off to the races, you keep on insisting to create indexes - does this make sense for local DuckDB prep? Once data gets moved to Motherduck - is there any index support?"

**Research**: Dug into Motherduck documentation

**Found**:
> "While the syntax is supported, indexes are not currently utilized for query acceleration in MotherDuck. Indexes can significantly slow down INSERT operations without any corresponding advantages."

**Decision**:
- Removed ALL CREATE INDEX statements from schema
- Indexes don't help queries (columnar storage already optimized)
- Indexes slow down daily UPDATE/INSERT operations
- Local DuckDB doesn't need them either (one-pass ETL)

**Replacement**: Wrote comprehensive "Query Performance Strategy" section:
- Why no indexes needed
- Efficient query patterns for columnar storage
- When to use external search services (Typesense/Elasticsearch)
- Alternative approaches for full-text search

---

## Act V: Workflow Clarity

### "Daily Updates MODIFY Existing Data"

**User**: "Reviewing the implementation plan phases, it reads as if we would upload the daily update files into Motherduck. The goal is to use these daily updates to UPDATE the database prefilled with the full dataset (delete/insert) and maintain temporal tracking."

**Problem**: Documentation was ambiguous about:
- When initial full import happens
- That daily updates execute SQL (not upload files)
- The relationship between monthly and daily operations

**Additional Corrections**:
- "Datasets aren't always available on first Sunday of month" → Check meta.csv
- "Midday (Belgian time) is better than 8am" → Changed to 12:00 CET
- "Meta.csv is inside ZIP" → Manual detection, then validate

**Solution**: Added "Implementation Workflow" section with three clear modes:

**1. Initial Setup (One-Time)**
- Download full dataset → Transform locally → Upload to Motherduck
- All data marked `_is_current = true`

**2. Daily Operations (Automated, 12:00 CET)**
- Download update ZIP
- Execute SQL UPDATE (mark deleted rows as historical)
- Execute SQL INSERT (add new/updated rows)
- Modifies existing Motherduck data

**3. Monthly Full Import (Manual/Triggered)**
- When new dataset available on portal
- Mark current as historical
- Process new snapshot locally
- Upload to Motherduck as new current

---

## Act VI: Security & Cron Jobs

### "What About CRON_SECRET?"

**User**: "What is the goal of CRON_SECRET and how does this fit with Vercel crons?"

**Research**: Vercel cron job security patterns

**How It Works**:
1. Set `CRON_SECRET` as environment variable (generate with `openssl rand -hex 32`)
2. Vercel automatically sends it as: `Authorization: Bearer {CRON_SECRET}`
3. Endpoint validates header before processing
4. Prevents unauthorized execution

**Example**:
```typescript
export async function POST(request: Request) {
  const authHeader = request.headers.get('authorization');
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return new Response('Unauthorized', { status: 401 });
  }
  // Process cron job...
}
```

---

## Act VII: Component Architecture

### "Think Super Hard About What We Need"

**Initial Proposal**: KBO portal client, local CLI scripts, cron jobs, admin UI, data viewer, shares management, auth system, TypeScript utilities

**User's Constraints**:
- "No public API or public data browser for now"
- "Codes table handled as is, not using caching layer"
- "Keep external service dependencies as limited as possible"

**Final Architecture**: Minimal external dependencies
- ✅ Clerk (authentication)
- ✅ Motherduck (database)
- ✅ Vercel (hosting + cron)
- ❌ No public API
- ❌ No caching layer
- ❌ No external logging
- ❌ No CDN

**10 Component Areas Documented**:
1. KBO Data Access Layer (`lib/kbo-client/`)
2. Local ETL Scripts (`scripts/`, `lib/transform/`)
3. Daily Update Cron (`app/api/cron/`)
4. Admin Web UI (6 pages: jobs, triggers, status, data viewer, shares, users)
5. Authentication (Clerk middleware)
6. Shared TypeScript Components (`lib/types/`, `lib/validation/`, `lib/utils/`)
7. Configuration (environment variables)
8. Data Quality & Monitoring (`lib/quality/`, `lib/logger/`)
9. Testing (Jest, unit tests)
10. Documentation (admin guide, deployment checklist)

Each component documented with:
- Purpose statement
- Directory structure
- Function signatures
- TypeScript interfaces

---

## Key Technical Decisions

### 1. Parquet with ZSTD Compression
**Why**: Tested on real data, achieved 21x compression (1.5GB → 71MB)
**Impact**: 2-year retention = 2.5GB (vs 50GB for CSV)

### 2. Link Tables for Activities
**Why**: Denormalized = 18GB/snapshot, link table = 1.8GB/snapshot
**Impact**: 90% storage reduction

### 3. No Indexes
**Why**: Motherduck doesn't use them, slows down INSERTs
**Impact**: Faster daily updates, cleaner schema

### 4. Temporal Tracking with `_is_current`
**Why**: Simple boolean flag + snapshot date
**Impact**: Monthly granularity for historical queries

### 5. Primary Name Denormalization
**Why**: 100% of enterprises have denomination, need fast search
**Impact**: Search without JOINs, 3 languages (NL/FR/DE)

### 6. Hybrid Local/Cloud Architecture
**Why**: 46M rows monthly vs 156 changes/day
**Impact**: Efficient resource usage, predictable costs

### 7. Store Codes Only, JOIN for Descriptions
**Why**: Flexible multi-language support, no duplicate data
**Impact**: User can switch languages without data changes

### 8. Daily Cron at 12:00 CET
**Why**: Better API reliability than early morning
**Impact**: Runs during business hours when KBO portal stable

---

## Critical Corrections Log

| Issue | Before | After | Impact |
|-------|--------|-------|--------|
| Language codes | 0=FR, 1=DE, 2=NL | 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN | Fixed throughout schema |
| SQL syntax | PostgreSQL GIN, to_tsvector | DuckDB compatible | Queries will work |
| Indexes | CREATE INDEX everywhere | Zero indexes | Faster imports |
| Descriptions | Denormalized in enterprises | Store codes, JOIN runtime | Multi-lang flexible |
| Monthly timing | "First Sunday of month" | "When available" + meta.csv | Operational reality |
| Daily timing | 8am | 12:00 CET/CEST | Better reliability |
| Dependencies | API, cache, logging | Minimal (3 services) | Simpler maintenance |
| Workflow | Ambiguous upload | Clear 3-mode operation | Implementation clarity |

---

## What We Built (Before Code)

### Documentation Created

**DATA_ANALYSIS.md** (25KB)
- 7 query groups on sample data
- Row counts, distributions, patterns
- Primary selection rules
- Storage estimates

**IMPLEMENTATION_GUIDE.md** (67KB)
- Multi-language strategy (corrected codes)
- Schema design (10 tables, no indexes)
- Implementation workflow (3 modes)
- Component architecture (10 areas)
- Query performance strategy
- Transformation logic (SQL + TypeScript)
- Example queries

**README.md**
- Architecture summary
- Roadmap (5 phases)
- Quick start guide
- Key findings

**analysis-queries.sql**
- 7 query categories
- Reproducible analysis
- Compression tests

### Metrics

**Time**: Several evening sessions
**Code written**: 0 lines (intentionally)
**Documentation**: ~90KB of specs
**Bugs caught**: 8 critical issues fixed in design
**Rewrites needed**: 0

---

## The "Vibe Coding" Method

### What Worked

**1. Extended Planning Mode**
- Multiple sessions without coding
- Question every assumption
- Research thoroughly
- Document extensively

**2. Iterative Correction**
- First draft never final
- Each question improves design
- Bugs caught in planning

**3. Human + AI Collaboration**
- Human: domain knowledge, critical questions, strategic decisions
- AI: research, data analysis, documentation, patterns

**4. Documentation as Architecture**
- Writing forces clarity
- Captures rationale
- Creates implementation blueprint

**5. Embrace Constraints**
- "Minimal dependencies" → simpler system
- "No public API" → focused scope
- "Check sample data" → realistic design

### The Pattern

1. AI proposes based on initial understanding
2. User identifies gap or incorrect assumption
3. AI researches and corrects
4. User validates correction
5. Repeat until solid

Not linear. Not perfect first try. But thorough.

---

## Lessons Learned

### Plan Mode is Powerful
Catching bugs before implementation is cheap. Fixing them in production is expensive.

### Research Beats Assumptions
When uncertain, look it up. Motherduck docs saved us from wrong infrastructure.

### Questions Improve Design
Every "why?" led to better decisions:
- "Why indexes?" → Removed complexity
- "What about timing?" → Fixed assumptions
- "Why these dependencies?" → Simplified scope

### Domain Knowledge is Irreplaceable
AI can research and propose. Only humans know:
- Operational reality ("datasets aren't on fixed schedule")
- Business constraints ("no public API for now")
- Strategic tradeoffs ("keep dependencies minimal")

### Documentation First
Our 67KB implementation guide is the blueprint. Clear architecture before coding prevents rewrites.

---

## Implementation Roadmap

### Phase 1: Foundation & Initial Import
- Motherduck setup, schema creation
- Transform logic (primary selection, link tables)
- Initial full import to populate database

### Phase 2: Monthly Pipeline
- Local DuckDB → Parquet transformation
- Snapshot rotation logic
- Benchmark with full dataset

### Phase 3: Daily Updates & Admin UI
- Vercel cron (12:00 CET) with CRON_SECRET
- Next.js admin interface
- Job monitoring dashboard

### Phase 4-5: Features & Launch
- Data browser, temporal navigation
- Testing, optimization
- Production deployment

We have the complete blueprint.

---

## Conclusion

**Great software starts with great design.**

For NewAgeKBO, several evening sessions of collaborative planning created:
- ✅ Validated schema (no PostgreSQL assumptions)
- ✅ Correct multi-language handling (fixed codes)
- ✅ Efficient storage strategy (no unnecessary indexes)
- ✅ Clear operational workflows (3 modes documented)
- ✅ Minimal dependencies (3 external services)
- ✅ Complete component architecture (10 areas)

**Zero lines of code. Zero architectural debt. One clear path forward.**

The vibe coding method: Think deeply, question assumptions, document thoroughly, and only code when the design is solid.

That's an evening side project done right.

---

**Status**: Design Complete → Ready for Implementation
**Next**: Phase 1 (Foundation & Initial Import)
