# Git History - NewAgeKBO

**Repository:** NewAgeKBO
**Description:** Belgian KBO (Crossroads Bank for Enterprises) data import and management system
**Generated:** 2025-12-26

---

## Summary

- **Total Commits:** 95
- **First Commit:** 2025-10-12
- **Latest Commit:** 2025-12-18
- **Primary Author:** Steven Noels

---

## Timeline by Month

### December 2025

| Date | Commit | Description |
|------|--------|-------------|
| 2025-12-18 | b1c99f2 | Comprehensive documentation update for current project state |
| 2025-12-18 | 2afb4e6 | Add activity group descriptions and establishment activities to detail page |
| 2025-12-17 | c5482c4 | Remove Hermes exclusion from sector adoption export |
| 2025-12-16 | d106ece | Simplify import UI to use only durable imports |
| 2025-12-16 | 94952af | Fix BigInt serialization error in Restate retry path |
| 2025-12-12 | 1a8adf1 | Merge pull request #10 - Fix React Server Components CVE |
| 2025-12-12 | c9851f8 | Fix React Server Components CVE vulnerabilities |
| 2025-12-12 | f7d4b21 | Clean up delete confirmation UI to reduce layout shift |
| 2025-12-12 | 62d1db6 | Replace browser confirm popup with inline delete confirmation |
| 2025-12-12 | be59c24 | Add Restate Cloud authentication support |
| 2025-12-11 | fecbec6 | Add request signature validation for Restate Cloud |
| 2025-12-11 | 4fc03aa | Fix batch processing: add operation parameter to processBatch |
| 2025-12-11 | c150eef | Make processBatch idempotent for Restate retries |
| 2025-12-11 | dffbb95 | Fix route conflict: move workflow routes to /api/admin/workflows |
| 2025-12-11 | 988e0b3 | Add admin UI for Restate durable imports |
| 2025-12-11 | 8ccb901 | Fix Restate endpoint routing for Next.js App Router |
| 2025-12-11 | 0173b13 | Add Restate SDK and KBO import workflow |
| 2025-12-11 | 452632d | Remove DBOS SDK - going with Restate instead |
| 2025-12-11 | 7883671 | Add Restate migration plan as alternative to DBOS |
| 2025-12-11 | 686fed6 | Add DBOS migration plan for durable workflow execution |
| 2025-12-10 | 7445e41 | Add DBOS SDK dependency for durable workflow execution |
| 2025-12-07 | 8bfd028 | Add Peppol analysis scripts |
| 2025-12-06 | 268abfd | Add sector adoption JSON export script |
| 2025-12-05 | f5ad381 | Remove stale compiled index.js (let Next.js compile TS directly) |
| 2025-12-05 | 4d559e5 | Fix export download memory issue with streaming + update Next.js |
| 2025-12-02 | 91977bf | Add individual delete buttons to exports page |

### November 2025

| Date | Commit | Description |
|------|--------|-------------|
| 2025-11-30 | 17f0b3a | Deduplicate entity export to one row per enterprise |
| 2025-11-29 | 96ccf9c | Add activity group columns to entity export (v2 schema) |
| 2025-11-29 | 315a3c4 | Remove outdated session notes |
| 2025-11-25 | fb312b9 | adjust download format to accommodate peppol-bulk-processor |
| 2025-11-24 | 86c35da | feat: VAT entity export system with MotherDuck table storage |
| 2025-11-05 | c1f0572 | file naming |
| 2025-11-05 | fd60b65 | chore: Ignore KBO technical PDF (content extracted to markdown) |
| 2025-11-05 | 12736c7 | docs: Add XML VAT specifications and implementation plan |
| 2025-11-05 | 0c8bc37 | Merge branch 'feature/november-full-dump-validation' |
| 2025-11-05 | bde485d | feat: Complete validation implementation and docs |
| 2025-11-03 | c60b591 | more investigations on the source data layer |
| 2025-11-03 | 14de51a | refactor: Remove unused manual file upload from imports page |
| 2025-11-02 | 7a555dc | docs: Add comprehensive schema consistency review of daily import flows |
| 2025-11-02 | dd65c13 | docs: Add November 2025 strategy and session notes |
| 2025-11-02 | fa5aca6 | feat: Add comprehensive November full dump validation and migration system |
| 2025-11-02 | 3b70627 | refactor: Replace finalize confirm dialog with inline UI |
| 2025-11-01 | cfda36c | refactor: Replace browser popups with inline error displays |
| 2025-11-01 | f2c50e4 | feat: Add batched import system with real-time progress tracking |
| 2025-11-01 | fde57b4 | fix: Resolve Vercel build errors in batched-update.ts |
| 2025-11-01 | d9904f9 | fix: Update batched import for NULL columns, BigInt handling, duplicate keys |
| 2025-11-01 | 75c9ade | feat: Implement complete batched import core library |
| 2025-11-01 | e8a6c56 | feat: Add batched import core library skeleton |
| 2025-11-01 | ddc21a9 | fix: Align staging table schemas with KBO CSV structure |
| 2025-11-01 | 4acf5d2 | feat: Add batched import system design and database schema |
| 2025-11-01 | 62bbc87 | fix: Remove redundant database USE statement in export script |

### October 2025

| Date | Commit | Description |
|------|--------|-------------|
| 2025-10-31 | e0a0fc4 | ci: Use GitHub Secrets for build environment variables |
| 2025-10-31 | 16e8ad2 | perf: Eliminate duplicate API requests on page load and navigation |
| 2025-10-31 | 7bd3e4f | fix: Remove automatic file browser refresh on imports page load |
| 2025-10-30 | 25ee0cc | Merge pull request #9 - feature/codes-i18n |
| 2025-10-30 | 9fa7349 | feat: Add language selection and cessation status tracking |
| 2025-10-30 | aeec22b | fix: Comment out old duckdb import blocking Vercel build |
| 2025-10-30 | 2698356 | fix: Add type assertions for CSV parse results |
| 2025-10-30 | edc4cd7 | fix: Replace 'any' types with proper types for ESLint compliance |
| 2025-10-30 | c3c481d | fix: Comprehensive pre-production consistency fixes |
| 2025-10-30 | 8c8db55 | chore: Remove temporary fix and migration scripts |
| 2025-10-30 | ec0a408 | feat: Add import statistics recalculation and fix extract 140 |
| 2025-10-30 | 2316878 | fix: Critical fixes for web-based KBO import functionality |
| 2025-10-29 | d6c8184 | refactor: Improve admin imports UI organization |
| 2025-10-29 | 9649547 | feat: Add browsable file list to admin imports page |
| 2025-10-29 | ab0533e | feat: Add automated KBO data ingestion from web portal |
| 2025-10-29 | 1ad2832 | feat: Add 'backfill' worker type to import_jobs schema |
| 2025-10-29 | 638e744 | feat: Comprehensive import job tracking with affected enterprises (#8) |
| 2025-10-28 | 1a0c560 | chore: Upgrade @duckdb/node-api from 1.3.2-alpha.25 to 1.4.1-r.4 |
| 2025-10-28 | ac306e0 | refactor: Clean up Motherduck connection code |
| 2025-10-28 | 59df498 | fix: Set all directory configs before Motherduck extension loads |
| 2025-10-28 | 648470c | fix: Use in-memory database with ATTACH for Motherduck connection |
| 2025-10-28 | 560a4ec | fix: Set home_directory via SQL after connection instead of config option |
| 2025-10-28 | 7f1f51e | fix: Set home_directory in DuckDB config options instead of after connect |
| 2025-10-27 | 4fa520e | feat: Migrate from duckdb to @duckdb/node-api for Vercel compatibility |
| 2025-10-27 | 9f4228c | fix: Explicitly install and load Motherduck extension |
| 2025-10-27 | 0c2c2ec | fix: Configure DuckDB directories before Motherduck attachment |
| 2025-10-27 | a61abd0 | fix: Use in-memory DuckDB with Motherduck attachment for serverless |
| 2025-10-27 | c93a94e | fix: Configure DuckDB home directory for serverless environments |
| 2025-10-27 | 6ad5b5c | fix: Force dynamic rendering for admin dashboard page |
| 2025-10-27 | 2deebca | fix: Use dynamic import for DuckDB to support Vercel builds |
| 2025-10-27 | ca21cf7 | docs: Add comprehensive Vercel deployment guide |
| 2025-10-27 | c5b8174 | perf: Suppress webpack cache warning for runtime-only codes cache |
| 2025-10-27 | 58187b3 | feat: Phase 3 - Admin Web UI with Authentication and Data Browsing (#7) |
| 2025-10-18 | 84f0df3 | feat: Complete Phase 2 - Shared import library and refactored pipeline (#6) |
| 2025-10-18 | b86c0ff | small script to export current denoms from MD and remove sample data |
| 2025-10-17 | 1ba6114 | feat: Daily updates pipeline and script utilities (#5) |
| 2025-10-16 | 2455e01 | Initial Implementation - Motherduck Integration & Data Import (#4) |
| 2025-10-15 | e663f03 | Add Claude Code GitHub Workflow (#2) |
| 2025-10-15 | bb7f2b4 | feat: Initialize Next.js foundation with TypeScript types and validation (#1) |
| 2025-10-15 | f58c881 | finalising analysis and tech design |
| 2025-10-15 | 91cabbf | improved implementation guis (schema, no indexes, multilinguality) |
| 2025-10-13 | 305276c | Add .gitkeep and README for full dataset directory |
| 2025-10-13 | de1fb34 | Design phase complete: Comprehensive analysis and architecture |
| 2025-10-12 | bfc59d3 | first, the specs - extracted and translated from KBO cookbook |

---

## Key Milestones

- **2025-12-11:** Migrated to Restate for durable workflow execution (replaced DBOS)
- **2025-11-24:** VAT entity export system with MotherDuck table storage
- **2025-11-05:** November full dump validation system completed
- **2025-11-01:** Batched import system with real-time progress tracking
- **2025-10-30:** Language selection and i18n support for codes
- **2025-10-29:** Automated KBO data ingestion from web portal
- **2025-10-27:** Phase 3 - Admin Web UI with Authentication
- **2025-10-18:** Phase 2 - Shared import library and refactored pipeline
- **2025-10-16:** Initial Motherduck integration and data import
- **2025-10-12:** Project inception with KBO specs analysis

---

## Development Phases

### Phase 1: Design & Analysis (Oct 12-15)
- KBO specifications extraction and translation
- Comprehensive architecture design
- Next.js foundation with TypeScript

### Phase 2: Core Infrastructure (Oct 16-18)
- MotherDuck integration
- Daily updates pipeline
- Shared import library

### Phase 3: Web UI (Oct 27)
- Admin dashboard with Clerk authentication
- Data browsing and search
- Vercel deployment with DuckDB compatibility

### Phase 4: Production Hardening (Oct 28 - Nov 5)
- Batched import system
- November full dump validation
- Import job tracking

### Phase 5: Export & Integration (Nov 24 - Dec)
- VAT entity export for Peppol integration
- Sector adoption analysis
- Restate durable workflows

---

## Merge History

| PR | Date | Description |
|----|------|-------------|
| #10 | 2025-12-12 | Fix React Server Components CVE vulnerabilities |
| #9 | 2025-10-30 | feature/codes-i18n - Language selection support |
| #8 | 2025-10-29 | Comprehensive import job tracking |
| #7 | 2025-10-27 | Phase 3 - Admin Web UI |
| #6 | 2025-10-18 | Phase 2 - Shared import library |
| #5 | 2025-10-17 | Daily updates pipeline |
| #4 | 2025-10-16 | Motherduck Integration |
| #2 | 2025-10-15 | Claude Code GitHub Workflow |
| #1 | 2025-10-15 | Next.js foundation |

---

## Technology Decisions

| Decision | Date | Choice |
|----------|------|--------|
| Durable Workflows | 2025-12-11 | Restate (over DBOS) |
| Database | 2025-10-16 | MotherDuck (DuckDB cloud) |
| DuckDB Node API | 2025-10-27 | @duckdb/node-api (for Vercel) |
| Authentication | 2025-10-27 | Clerk |
| Framework | 2025-10-15 | Next.js 15 with App Router |

---

*This file is auto-generated. Run `git log` for complete history.*
