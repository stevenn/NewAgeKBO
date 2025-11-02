# Monthly Full Dump Process

This document provides a step-by-step runbook for processing monthly KBO full dumps while preserving temporal history.

## Overview

KBO publishes monthly full dumps (first Sunday of each month) that contain the complete current state of the database. We use these dumps primarily for **validation** rather than replacement, allowing us to:

- Verify our database is synchronized with KBO
- Detect any missed daily updates
- Identify data corruption or inconsistencies
- Make informed decisions about database maintenance

## Strategy

**Primary approach**: Daily incremental updates + monthly validation

- **Daily**: Process daily update files to maintain current state
- **Monthly**: Validate against full dump, take action based on discrepancy level

## Process Steps

### Phase 1: Pre-Flight Checks

#### 1.1 Capture Current Database State

```bash
npx tsx scripts/database-state-snapshot.ts > snapshots/pre-$(date +%Y%m).json
```

This creates a baseline snapshot showing:
- Extract numbers present
- Row counts per table (total, current, historical)
- Date ranges
- Sample entities with version history
- Anomalies (duplicate current records, missing flags, extract gaps)

**Review the output carefully**. Note:
- Number of extracts loaded
- Total current vs historical records
- Any anomalies reported

#### 1.2 Check for Available Full Dump

Visit https://kbopub.economie.fgov.be/kbo-open-data (requires login)

Expected filename format:
```
KboOpenData_0145_YYYY_MM_DD_Full.zip
```

Download to a working directory and extract:
```bash
unzip KboOpenData_0145_YYYY_MM_DD_Full.zip -d /tmp/kbo-full-nov2025
```

### Phase 2: Validation Import

#### 2.1 Run Validation Script

```bash
npx tsx scripts/validate-with-full-dump.ts /tmp/kbo-full-nov2025 > validation-reports/nov2025.json
```

This will:
1. Import full dump to temporary `nov_*` tables in MotherDuck
2. Compare against current database state
3. Calculate discrepancy percentages
4. Sample discrepancies for investigation
5. Generate automatic recommendation
6. Clean up temporary tables

**Time estimate**: 30-45 minutes for full import + comparison

#### 2.2 Review Validation Report

The script outputs:
- Overall discrepancy percentage
- Per-table comparison statistics
- Missing entities in both directions
- Sample discrepancies with details
- **Automatic recommendation**

**Recommendation thresholds**:
- **< 1% discrepancy**: `keep_history` - Safe to continue with daily updates
- **1-5% discrepancy**: `review_details` - Investigate samples, decide case-by-case
- **> 5% discrepancy**: `start_fresh` - Consider database reset

### Phase 3: Decision Tree

Based on validation results, choose one of three paths:

#### Path A: Keep History (< 1% discrepancy)

✅ **Recommended when**: Database is synchronized, no major issues

**Action**: Continue with daily updates, no import needed

```bash
# Optional: Apply monthly snapshot for long-term archival
# (This will mark current records as historical and import the full dump)
npx tsx scripts/apply-monthly-snapshot.ts /tmp/kbo-full-nov2025
```

**Result**: Temporal history preserved, new snapshot added to 24-month history

#### Path B: Review Details (1-5% discrepancy)

⚠️ **Recommended when**: Moderate discrepancies, investigation needed

**Steps**:

1. **Examine sample discrepancies** from validation report:
   ```json
   {
     "sample_discrepancies": [
       {
         "entity_id": "0123.456.789",
         "issue_type": "missing_in_db",
         "db_value": "null",
         "dump_value": "active"
       }
     ]
   }
   ```

2. **Investigate root causes**:
   - Check `scripts/list-extracts.ts` to verify all daily updates were imported
   - Review import logs for failed daily updates
   - Query specific entities to understand discrepancies

3. **Decide**:
   - If discrepancies are explainable (e.g., missed 1-2 daily updates): → Path A (catch up with missing dailies)
   - If discrepancies are unexplainable (data corruption): → Path C (reset)

#### Path C: Start Fresh (> 5% discrepancy)

🚨 **Recommended when**: Major discrepancies, data corruption, or lost synchronization

**⚠️ WARNING**: This destroys all temporal history!

**Before proceeding**:
1. ✅ Backup created automatically (stored in `/tmp/kbo-backups/`)
2. ✅ Validation report reviewed and saved
3. ✅ Decision documented (why reset was necessary)

**Execute reset**:
```bash
npx tsx scripts/reset-from-full-dump.ts /tmp/kbo-full-nov2025 --confirm
```

Optionally skip backup (NOT RECOMMENDED):
```bash
npx tsx scripts/reset-from-full-dump.ts /tmp/kbo-full-nov2025 --confirm --no-backup
```

**Result**: Clean database with single extract, temporal history starts fresh

### Phase 4: Post-Import Verification

#### 4.1 Capture Post-Import State

```bash
npx tsx scripts/database-state-snapshot.ts > snapshots/post-$(date +%Y%m).json
```

#### 4.2 Verify Import Success

Check:
- ✅ Extract number incremented (or reset to 1 if fresh start)
- ✅ Row counts reasonable (enterprises ~2M, establishments ~1M)
- ✅ No anomalies reported (duplicate current records, etc.)
- ✅ Sample entities have correct status

#### 4.3 Test Application

1. Navigate to admin imports page
2. Verify latest extract is displayed
3. Test browse functionality
4. Verify primary names resolved correctly

### Phase 5: _deleted_at_extract Fix (One-Time)

**NOTE**: This phase is only needed once to backfill historical data. After the fix is applied, monthly snapshots automatically populate this field.

#### 5.1 Check if Fix Needed

```bash
npx tsx scripts/migrate-deleted-at-extract.ts
```

If output shows "Missing _deleted_at_extract: 0" for all tables, skip this phase.

#### 5.2 Dry Run Migration

```bash
npx tsx scripts/migrate-deleted-at-extract.ts
```

Review:
- Sample validations (should be ~10/10 valid)
- Records that would be updated

#### 5.3 Execute Migration

```bash
npx tsx scripts/migrate-deleted-at-extract.ts --execute
```

**Time estimate**: 5-10 minutes for all tables

#### 5.4 Verify Migration

```bash
# Single table check
npx tsx scripts/migrate-deleted-at-extract.ts --table=enterprises
```

Should show: "Missing _deleted_at_extract: 0"

### Phase 6: Documentation

#### 6.1 Update Import Log

Create entry in `docs/import-log.md`:

```markdown
## November 2025 Full Dump

**Date**: 2025-11-03
**Extract Number**: 145
**Snapshot Date**: 2025-11-01

**Validation Results**:
- Overall discrepancy: 0.3%
- Recommendation: keep_history
- Action taken: Applied monthly snapshot

**Issues**:
- None

**Notes**:
- All temporal history preserved
- 24-month retention policy applied
- _deleted_at_extract populated correctly
```

#### 6.2 Archive Validation Report

```bash
mkdir -p validation-reports
mv validation-nov2025.json validation-reports/
```

## Reference: Script Summary

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `database-state-snapshot.ts` | Capture baseline metrics | Before and after validation |
| `validate-with-full-dump.ts` | Compare full dump vs database | Monthly validation |
| `apply-monthly-snapshot.ts` | Import monthly dump preserving history | When discrepancy < 1% |
| `reset-from-full-dump.ts` | Clean slate import (destroys history) | When discrepancy > 5% |
| `migrate-deleted-at-extract.ts` | Backfill temporal tracking field | One-time fix |

## Troubleshooting

### Issue: Validation script fails with "temp table already exists"

**Solution**: Temporary tables weren't cleaned up from previous run

```bash
npx tsx scripts/validate-with-full-dump.ts /tmp/kbo-full --cleanup-only
```

Or manually:
```sql
-- Connect to MotherDuck
DROP TABLE IF EXISTS nov_enterprises;
DROP TABLE IF EXISTS nov_establishments;
-- ... etc for all tables
```

### Issue: Import fails with "extract number already exists"

**Solution**: The extract was already imported

Check current extracts:
```bash
npx tsx scripts/list-extracts.ts
```

Either:
1. Skip import (already done)
2. Use `reset-from-full-dump.ts` to start fresh

### Issue: High discrepancy (> 5%) but no obvious cause

**Investigation steps**:

1. Check for missed daily updates:
   ```bash
   npx tsx scripts/list-extracts.ts
   # Look for gaps in extract sequence
   ```

2. Query sample discrepant entities:
   ```sql
   -- Find specific entity in both current and historical
   SELECT * FROM enterprises
   WHERE enterprise_number = '0123.456.789'
   ORDER BY _snapshot_date DESC, _extract_number DESC
   ```

3. Review import logs from admin UI
   - Check for failed imports
   - Look for error patterns

4. Check KBO for known issues:
   - Visit https://kbopub.economie.fgov.be/kbo-open-data
   - Review announcements/known issues

### Issue: Backup creation fails (disk space)

**Solution**: Use `--no-backup` flag if absolutely necessary

```bash
npx tsx scripts/reset-from-full-dump.ts /tmp/kbo-full --confirm --no-backup
```

**⚠️ WARNING**: Only use if you have another backup strategy!

## Best Practices

### 1. Monthly Ritual Checklist

- [ ] Download latest full dump (first Sunday of month)
- [ ] Extract ZIP to temporary directory
- [ ] Run `database-state-snapshot.ts` (pre-validation)
- [ ] Run `validate-with-full-dump.ts`
- [ ] Review validation report
- [ ] Make decision (keep/review/reset)
- [ ] Execute chosen path
- [ ] Run `database-state-snapshot.ts` (post-validation)
- [ ] Test application
- [ ] Update import log
- [ ] Archive validation report
- [ ] Clean up temporary files

### 2. Keep Historical Records

Maintain a log of:
- Validation reports (JSON)
- Database snapshots (JSON)
- Import logs (Markdown)
- Decision rationale (why reset vs keep)

Suggested structure:
```
docs/
  import-log.md
  validation-reports/
    2025-10.json
    2025-11.json
    2025-12.json
  snapshots/
    pre-2025-10.json
    post-2025-10.json
    pre-2025-11.json
    post-2025-11.json
```

### 3. Automation Considerations

**DO NOT** fully automate this process. Monthly full dumps require human judgment:

- Review validation discrepancies
- Investigate anomalies
- Make reset/keep decision
- Document reasoning

**CAN automate**:
- Download notification (Slack/email when new dump available)
- Validation script execution (but review results manually)
- Snapshot archival

### 4. Disaster Recovery

**Backup strategy**:
1. **Automatic**: `reset-from-full-dump.ts` creates Parquet backups
2. **Manual**: Export critical tables before risky operations
3. **MotherDuck**: Native backups (check MotherDuck documentation)

**Recovery procedures**:

From Parquet backup:
```sql
-- Restore from backup
COPY enterprises FROM '/tmp/kbo-backups/backup-2025-11-03/enterprises.parquet';
```

From monthly full dump:
```bash
# If all else fails, reset from latest full dump
npx tsx scripts/reset-from-full-dump.ts /path/to/latest/full/dump --confirm
```

## Timeline Expectations

| Task | Duration |
|------|----------|
| Download full dump | 10-15 min (depends on connection) |
| Extract ZIP | 2-3 min |
| Database snapshot | 30-60 sec |
| Validation import | 30-45 min |
| Apply monthly snapshot | 30-45 min |
| Reset from full dump | 25-35 min |
| _deleted_at_extract migration | 5-10 min |
| **Total (validation path)** | **~1 hour** |
| **Total (reset path)** | **~45 min** |

## Future Improvements

Potential enhancements to consider:

1. **Parallel validation**: Import temp tables in parallel
2. **Incremental comparison**: Compare only changed entities
3. **Automated notifications**: Slack/email when validation complete
4. **Web UI**: Trigger validation from admin panel
5. **Scheduled validation**: Cron job to run validation automatically
6. **Discrepancy analysis**: ML-based anomaly detection
7. **Rollback automation**: One-click rollback from backup

## Questions & Support

For issues or questions:
1. Check this documentation first
2. Review validation reports for clues
3. Check import logs in admin UI
4. Review KBO announcements: https://kbopub.economie.fgov.be/kbo-open-data
5. Consult `docs/UPDATE_STRATEGY.md` for temporal design
6. Check `CLAUDE.md` for development guidance
