# NewAgeKBO - Modern KBO Open Data Platform

A modern Next.js platform for managing and querying Belgian KBO (Crossroads Bank for Enterprises) Open Data with temporal tracking and efficient storage.

---

## 🎯 Project Status

**Phase**: Design & Analysis Complete ✅
**Next**: Implementation (Week 1 - Setup & POC)

---

## 📋 Quick Start

### Prerequisites
- Node.js 18+
- DuckDB CLI (`brew install duckdb`)
- Motherduck account (for cloud deployment)
- KBO Open Data access credentials

### Local Development
```bash
# Install dependencies
npm install

# Run data analysis (requires sample data)
duckdb :memory: < analysis-queries.sql

# Test Parquet compression
./test-parquet-compression.sh
```

---

## 📚 Documentation

### Core Documents
- **[docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)** - Complete implementation guide with schema, pipeline, and query patterns
- **[docs/DATA_ANALYSIS.md](docs/DATA_ANALYSIS.md)** - Data analysis findings and design decisions
- **[CLAUDE.md](CLAUDE.md)** - Instructions for Claude Code when working on this project

### Reference
- **[specs/KBOCookbook_EN.md](specs/KBOCookbook_EN.md)** - Official KBO Open Data specification (English)
- **[specs/KBOCookbook_NL.md](specs/KBOCookbook_NL.md)** - Official KBO Open Data specification (Dutch)

---

## 🏗️ Architecture

### Hybrid Approach: Local + Cloud

**Monthly Full Imports** (46M rows):
```
Download ZIP → DuckDB (local) → Transform → Parquet (ZSTD) → Motherduck
Duration: ~5 minutes
```

**Daily Updates** (~156 changes/day):
```
Vercel Cron (12:00 CET) → Download ZIP → Parse CSV → UPDATE/INSERT (Motherduck)
Duration: <1 minute
```

### Storage Strategy
- **Format**: Parquet with ZSTD compression (21x compression)
- **Retention**: Current snapshot + 24 monthly snapshots (2.5 GB total)
- **Cost**: ~$0.05/month (Motherduck storage)

---

## 💾 Database Schema

### Core Tables
- **enterprises** - 1.9M companies (denormalized: number, name, status, juridical form)
- **activities** - 36M activity links → **nace_codes** lookup table
- **addresses** - 2.8M addresses (60% have, 40% NULL for natural persons)
- **denominations** - 3.3M names (all languages, all types)
- **establishments** - 1.7M establishment units
- **contacts** - 0.7M contact details

### Key Design Decisions
✅ **Link tables for activities** → 90% storage reduction
✅ **Parquet with ZSTD** → 21x compression (1.5GB → 71MB measured)
✅ **Denormalize primary name only** → Fast search, all enterprises have one
❌ **Don't denormalize addresses** → 40% would be NULL
❌ **Don't denormalize activities** → Too large (36M rows)

### Temporal Tracking
- `_is_current` boolean flag for live data
- `_snapshot_date` for historical queries
- Monthly snapshots when new full dataset available
- Point-in-time queries with monthly granularity

---

## 📊 Data Characteristics

### Sample Data Analysis (Extract 140 + 147)

| Metric | Value |
|--------|-------|
| Total enterprises | 1,938,238 |
| Total rows per snapshot | 46,788,754 |
| CSV size (full dataset) | ~2.1 GB |
| Parquet size (ZSTD) | ~100 MB |
| Compression ratio | **21x** |
| Daily update rate | ~156 changes/day |
| Enterprises with address | 60% (40% are natural persons) |
| Enterprises with MAIN activity | 65% (35% have none) |
| Enterprises with denomination | **100%** ✓ |

### Activity Distribution
- **Average activities per enterprise**: 12.4
- **Median**: 6
- **Maximum**: 957 activities
- **Unique NACE codes**: 7,265 across 3 versions (2003, 2008, 2025)

---

## 🔍 Example Queries

### Search by name
```sql
SELECT enterprise_number, primary_name_nl, status
FROM enterprises
WHERE _is_current = true
  AND primary_name_nl ILIKE '%veneco%'
LIMIT 100;
```

### Get enterprise with address and activity
```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  a.municipality_nl,
  n.description_nl as activity
FROM enterprises e
LEFT JOIN addresses a
  ON e.enterprise_number = a.entity_number
  AND a.type_of_address = 'REGO'
  AND a._is_current = true
LEFT JOIN activities act
  ON e.enterprise_number = act.entity_number
  AND act.classification = 'MAIN'
  AND act._is_current = true
LEFT JOIN nace_codes n
  ON act.nace_code = n.nace_code
WHERE e.enterprise_number = '0200.065.765'
  AND e._is_current = true;
```

### Historical query
```sql
-- Show enterprise changes over time
SELECT _snapshot_date, primary_name_nl, status
FROM enterprises
WHERE enterprise_number = '0200.065.765'
ORDER BY _snapshot_date DESC;
```

---

## 🛠️ Tech Stack

- **Database**: Motherduck (hosted DuckDB)
- **Web Framework**: Next.js 15 + TypeScript
- **Deployment**: Vercel (functions + cron)
- **Storage Format**: Parquet with ZSTD compression
- **Processing**: DuckDB (local for monthly, Motherduck for daily)
- **UI**: TailwindCSS + shadcn/ui

---

## 📁 Project Structure

```
NewAgeKBO/
├── docs/
│   ├── IMPLEMENTATION_GUIDE.md    # Complete implementation reference
│   ├── DATA_ANALYSIS.md           # Data analysis findings
│   └── old_analysis/              # Historical analysis artifacts
├── specs/
│   ├── KBOCookbook_EN.md          # Official KBO specification
│   └── KBOCookbook_NL.md          # Official KBO specification (Dutch)
├── sampledata/
│   ├── KboOpenData_0140_*/        # Full dataset sample (NOT in git)
│   └── KboOpenData_0147_*/        # Update dataset sample (in git)
├── analysis-queries.sql           # DuckDB queries for data analysis
├── test-parquet-compression.sh    # Compression benchmark script
├── CLAUDE.md                      # Instructions for Claude Code
└── README.md                      # This file
```

---

## 🚀 Implementation Roadmap

### ✅ Phase 0: Design & Analysis (Complete)
- [x] Data analysis with DuckDB
- [x] Schema design with link tables
- [x] Parquet compression testing (21x confirmed)
- [x] Storage strategy (2.5 GB for 2 years)
- [x] Primary selection rules defined

### 🔄 Phase 1: Foundation & Initial Import (Week 1-2)
- [ ] Set up Motherduck account and connection
- [ ] Create schema (all tables) in Motherduck
- [ ] Implement primary selection logic (SQL + TypeScript)
- [ ] **Perform initial full import** to populate Motherduck
- [ ] Verify temporal tracking and data integrity

### 📅 Phase 2: Monthly Full Import Pipeline (Week 3-4)
- [ ] Build CSV → DuckDB → Parquet transformation pipeline (local)
- [ ] Implement link table transformations
- [ ] Implement snapshot rotation logic (`_is_current` updates)
- [ ] Test full re-import with sample data
- [ ] Benchmark with full dataset

### 🌐 Phase 3: Daily Update Pipeline & Web App (Week 5-7)
- [ ] Initialize Next.js app
- [ ] Build daily update cron job (Vercel, 12:00 CET)
- [ ] Implement delete/insert logic for incremental updates
- [ ] API routes for Motherduck queries
- [ ] Admin UI for job monitoring

### 🎨 Phase 4: Features (Week 8-10)
- [ ] Data browser with search
- [ ] Temporal navigation
- [ ] REST API endpoints
- [ ] TypeScript SDK

### ✨ Phase 5: Launch (Week 11-14)
- [ ] Performance optimization
- [ ] Testing & documentation
- [ ] Deploy to production
- [ ] First monthly import

---

## 🔬 Analysis & Testing

### Run Data Analysis
```bash
# Full analysis (requires sample data)
duckdb :memory: < analysis-queries.sql > analysis-results.txt

# View results
cat analysis-results.txt
```

### Test Parquet Compression
```bash
# Test compression on activity.csv (1.5 GB)
./test-parquet-compression.sh

# Results: ZSTD = 21.6x compression (1.5GB → 71MB)
```

### Sample Data
- **Full dataset**: `sampledata/KboOpenData_0140_2025_10_05_Full/` (NOT in git, 2.1 GB)
- **Update dataset**: `sampledata/KboOpenData_0147_2025_10_12_Update/` (in git, <1 MB)

See `sampledata/KboOpenData_0140_2025_10_05_Full/README.md` for instructions.

---

## 📖 Key Findings

### Critical Discoveries
1. **40% of enterprises have NO address** - Natural persons (TypeOfEnterprise=1) don't have enterprise-level addresses
2. **35% have NO MAIN activity** - Schema must allow NULL
3. **Daily updates are tiny** - Only ~156 changes/day (0.0008% of dataset)
4. **Monthly snapshots drive storage** - 46M rows/month vs 156 rows/day
5. **Parquet compression is excellent** - 21x compression measured on real data
6. **Link tables save 90% storage** - Activities: 18GB → 1.8GB per snapshot

### Primary Selection Rules

**Denomination** (always exists):
1. Type 001 (Legal Name) in Dutch
2. Type 001 (Legal Name) in French
3. Type 003 (Commercial Name) in Dutch
4. Fallback: Any denomination

**Address** (60% have):
1. Type REGO (Registered Office) for legal persons
2. NULL for natural persons

**Main Activity** (65% have):
1. ActivityGroup=003, NaceVersion=2025, Classification=MAIN
2. Fallback: Any MAIN activity with latest NACE version
3. NULL if none exists

---

## 🤝 Contributing

This is an open-source project. Contributions welcome!

### Development Workflow
1. Read `docs/IMPLEMENTATION_GUIDE.md` for technical details
2. Check `CLAUDE.md` for AI-assisted development guidelines
3. Follow the schema design in `docs/IMPLEMENTATION_GUIDE.md`
4. Test with sample data before full imports

---

## 📝 License

MIT License - See LICENSE file for details

---

## 🙏 Acknowledgments

- Belgian Federal Public Service Economy for providing KBO Open Data
- DuckDB team for the excellent analytics database
- Motherduck for hosted DuckDB infrastructure

---

**Questions?** See `docs/IMPLEMENTATION_GUIDE.md` for detailed technical documentation.
