# Implementation Plan: NewAgeKBO

## Overview
Build a modern Next.js web application with TypeScript and Motherduck (DuckDB) backend for managing and querying Belgian KBO Open Data with temporal capabilities.

## Phase 1: Project Foundation

### 1.1 Initialize Next.js Application (TypeScript)
- Create Next.js 15 app with TypeScript in `webapp/` directory
- Configure Vercel deployment settings
- Set up environment variables for Motherduck connection
- Configure strict TypeScript settings

### 1.2 DuckDB/Motherduck Schema Design

!! Evaluate the most cost-effective way to transfer towards and manage data in Motherduck, CSV is bulky while Parquet files compress very well and are natively supported by DuckDB

**Simplified, Denormalized Schema for Easy Querying:**

```sql
-- Main enterprise table (denormalized for easy queries)
CREATE TABLE enterprises (
  enterprise_number VARCHAR PRIMARY KEY,

  -- Basic info
  status VARCHAR,
  juridical_situation VARCHAR,
  juridical_situation_desc_nl VARCHAR,
  juridical_situation_desc_fr VARCHAR,
  type_of_enterprise VARCHAR,
  juridical_form VARCHAR,
  juridical_form_desc_nl VARCHAR,
  juridical_form_desc_fr VARCHAR,
  start_date DATE,

  -- Primary denomination (most common query)
  name_nl VARCHAR,
  name_fr VARCHAR,
  commercial_name VARCHAR,
  abbreviation VARCHAR,

  -- Primary address (denormalized for convenience)
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,
  country_nl VARCHAR,
  country_fr VARCHAR,

  -- Primary contact (most used)
  phone VARCHAR,
  email VARCHAR,
  website VARCHAR,

  -- Main activity (denormalized)
  main_nace_code VARCHAR,
  main_nace_desc_nl VARCHAR,
  main_nace_desc_fr VARCHAR,
  nace_version VARCHAR,

  -- Temporal tracking
  _valid_from DATE,
  _valid_to DATE,
  _extract_number INTEGER
);

-- Establishments (linked to enterprises, also denormalized)
CREATE TABLE establishments (
  establishment_number VARCHAR PRIMARY KEY,
  enterprise_number VARCHAR,
  start_date DATE,

  -- Denormalized establishment details
  commercial_name VARCHAR,
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,
  phone VARCHAR,
  email VARCHAR,
  website VARCHAR,

  -- Main activity at establishment level
  main_nace_code VARCHAR,
  main_nace_desc_nl VARCHAR,
  main_nace_desc_fr VARCHAR,

  -- Temporal tracking
  _valid_from DATE,
  _valid_to DATE,
  _extract_number INTEGER
);

-- Additional denominations (only if multiple names exist)
CREATE TABLE additional_names (
  id INTEGER PRIMARY KEY,
  entity_number VARCHAR,
  entity_type VARCHAR, -- 'enterprise' or 'establishment'
  type_of_denomination VARCHAR,
  language VARCHAR,
  denomination VARCHAR,
  _valid_from DATE,
  _valid_to DATE
);

-- All activities (secondary/auxiliary activities)
CREATE TABLE activities (
  id INTEGER PRIMARY KEY,
  entity_number VARCHAR,
  entity_type VARCHAR, -- 'enterprise' or 'establishment'
  activity_group VARCHAR,
  nace_version VARCHAR,
  nace_code VARCHAR,
  nace_desc_nl VARCHAR,
  nace_desc_fr VARCHAR,
  classification VARCHAR, -- 'MAIN', 'SECO', 'ANCI'
  _valid_from DATE,
  _valid_to DATE
);

-- Branch offices (foreign entities with Belgian presence)
CREATE TABLE branches (
  id VARCHAR PRIMARY KEY,
  enterprise_number VARCHAR,
  start_date DATE,

  -- Branch details (denormalized)
  branch_name VARCHAR,
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,

  -- Temporal tracking
  _valid_from DATE,
  _valid_to DATE
);

-- Code lookup table (for any codes not denormalized)
CREATE TABLE codes (
  category VARCHAR,
  code VARCHAR,
  language VARCHAR,
  description VARCHAR,
  PRIMARY KEY (category, code, language)
);

-- Import job tracking
CREATE TABLE import_jobs (
  id INTEGER PRIMARY KEY,
  extract_number INTEGER,
  extract_type VARCHAR, -- 'full' or 'update'
  snapshot_date DATE,
  extract_timestamp TIMESTAMP,
  status VARCHAR, -- 'pending', 'running', 'completed', 'failed'
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message VARCHAR,
  records_processed INTEGER,
  records_inserted INTEGER,
  records_deleted INTEGER
);

-- Materialized views for common queries
CREATE VIEW enterprises_current AS
SELECT * FROM enterprises WHERE _valid_to IS NULL;

CREATE VIEW establishments_current AS
SELECT * FROM establishments WHERE _valid_to IS NULL;
```

**Benefits of this schema:**
- Single-table queries for most common use cases (no joins needed)
- Multi-language support built into columns (NL/FR)
- Easy filtering by location, activity, legal form
- Additional tables only for less-common data (multiple names, secondary activities)
- Temporal queries remain simple with _valid_from/_valid_to
- Denormalization happens at import time, not query time

### 1.3 Sample Data Validation
- Create TypeScript utilities to validate CSV structure against specs
- Test import with existing sample data (1.9M enterprises, 36M activity records)
- Verify temporal logic with full (extract 140) and update (extract 147) datasets

## Phase 2: Data Ingestion Backend (TypeScript)

### 2.1 KBO Data Fetcher Service
- TypeScript service for authenticated HTTP client to KBO API
- Type definitions for KBO dataset metadata
- API routes for listing available datasets
- Download and save ZIP files to Motherduck landing zone
- Parse meta.csv to extract snapshot date and extract number

### 2.2 CSV Parser & Importer (TypeScript)
- Build robust TypeScript CSV parser with proper typing
- Implement streaming parser for large files (activity.csv has 36M lines)
- **Transform normalized KBO CSVs into denormalized schema:**
  - Read code.csv first to build lookup maps
  - For each enterprise: join primary denomination, primary address, primary contact, main activity
  - Resolve code descriptions from code.csv during import
  - Handle missing data gracefully
- Handle update files with delete-then-insert pattern
- Create SQL COPY statements for bulk insertion into Motherduck

### 2.3 Temporal Update Logic (TypeScript)
- For full imports:
  - Close previous records: `UPDATE enterprises SET _valid_to = :snapshot_date - 1 WHERE _valid_to IS NULL`
  - Insert new records with _valid_from = snapshot_date, _valid_to = NULL
- For incremental updates:
  - Process deletes: close records in _delete.csv files
  - Process inserts: add new records from _insert.csv files with new valid_from date
- Maintain current view (WHERE _valid_to IS NULL) for active records

### 2.4 Data Transformation Pipeline
```typescript
// Pseudo-code for transformation
interface KBOEnterprise {
  enterpriseNumber: string;
  status: string;
  juridicalSituation: string;
  // ... from enterprise.csv
}

interface DenormalizedEnterprise extends KBOEnterprise {
  juridicalSituationDescNl: string;  // joined from code.csv
  juridicalSituationDescFr: string;  // joined from code.csv
  nameNl: string;                    // joined from denomination.csv
  nameFr: string;                    // joined from denomination.csv
  streetNl: string;                  // joined from address.csv
  // ...
}

// Pipeline: CSV → Parse → Join → Transform → Load to Motherduck
```

## Phase 3: Admin Web Interface (TypeScript + React)

### 3.1 Dataset Management UI
- List available datasets from KBO service with metadata
- Download trigger with progress tracking
- View import job history with status (pending, running, completed, failed)
- Display import statistics (records processed, inserted, deleted)

### 3.2 Data Browser
- **Enterprise search** by number, name, city, NACE code, legal form
- Display enterprise details with establishment hierarchy
- Multi-language toggle (NL/FR)
- Temporal navigation: view enterprise state at specific dates
- Export search results to CSV/JSON

### 3.3 Job Management
- View running/completed import jobs
- Trigger manual imports
- Retry failed imports
- Cancel running jobs

### 3.4 Analytics Dashboard
- Total enterprises/establishments over time
- Top municipalities by enterprise count
- NACE code distribution
- Legal form breakdown
- Daily update metrics

## Phase 4: API & Automation (TypeScript)

### 4.1 Programmatic API
- TypeScript API routes with Zod validation
- **Endpoints:**
  - `GET /api/enterprises/:number` - Get enterprise by number
  - `GET /api/enterprises/search` - Search enterprises
  - `GET /api/enterprises/:number/history` - Get temporal history
  - `GET /api/establishments/:number` - Get establishment details
  - `GET /api/datasets` - List available KBO datasets
  - `POST /api/import` - Trigger import job
  - `GET /api/jobs/:id` - Get job status
- Full TypeScript SDK for programmatic access

### 4.2 Vercel Cron Jobs
- **Daily (8am):** Check for new update files and auto-import
- **Weekly (Sunday):** Validate data integrity
- **Monthly (first Sunday):** Download full dataset for backup

## Phase 5: Testing & Documentation

### 5.1 Testing
- Unit tests for CSV parser and transformation logic
- Integration tests for import pipeline
- API endpoint tests
- Temporal query tests

### 5.2 Documentation
- API documentation (OpenAPI/Swagger)
- Database schema documentation
- User guide for admin interface
- Developer guide for programmatic access

## Key Technical Decisions

**Tech Stack:**
- **Next.js 15** (App Router) with **TypeScript**
- **Motherduck** (hosted DuckDB) - cloud-native analytics database
- **Vercel** (hosting + cron)
- **Zod** for runtime type validation
- **Papaparse** or custom CSV streaming parser
- **TailwindCSS** for styling
- **Radix UI** or **shadcn/ui** for components

**TypeScript Types:**
- Strict types for all KBO entities
- Generated types from database schema
- API contract types shared between frontend/backend
- Type-safe SQL query builders

**Performance Considerations:**
- Denormalized schema = faster queries, no joins for common cases
- Use Motherduck's SQL for heavy processing (avoid local data movement)
- Stream large CSV files during import (don't load entire files into memory)
- Index on enterprise_number, zipcode, nace_code, _valid_from, _valid_to
- Partition by extract_number for time-range queries
- Compress old temporal data (extracts older than 1 year)

**Data Quality:**
- Validate CSV structure before import
- Log transformation errors without failing entire import
- Track data quality metrics per import job
- Alert on anomalies (e.g., sudden drop in enterprise count)

## Implementation Order

1. **Week 1-2:** Setup (Next.js app, Motherduck connection, basic schema)
2. **Week 3-4:** CSV parser and transformation pipeline
3. **Week 5-6:** Import job system and temporal logic
4. **Week 7-8:** Admin UI (dataset management, job monitoring)
5. **Week 9-10:** Data browser and search
6. **Week 11-12:** API endpoints and Vercel cron jobs
7. **Week 13-14:** Testing, documentation, polish

## Success Metrics

- Successfully import full dataset (1.9M enterprises) in < 30 minutes
- Handle daily updates (typically 1000-5000 records) in < 2 minutes
- Sub-second query performance for enterprise lookup
- Support temporal queries spanning 2+ years of history
- Zero data loss during incremental updates
- 99.9% uptime for API endpoints
