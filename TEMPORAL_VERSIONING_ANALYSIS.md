# Temporal Versioning Analysis

## Investigation Summary

### Company: 0878.689.049

This company has 3 temporal snapshots in the database:

| Extract | Date | Is Current | Status | Denominations |
|---------|------|------------|--------|---------------|
| 157 | 2025-10-21 | ✓ (current) | AC | **NONE** (empty) |
| 150 | 2025-10-14 | ✗ | AC | **NONE** (empty) |
| 140 | 2025-10-04 | ✗ | AC | 1 denomination: "A.S.B.L. Villers 2000" |

### The Problem

**Before the fix**: When viewing Extract #150 in the UI, all sections (denominations, addresses, activities, contacts, establishments) would be empty/hidden because Extract #150 has no data for those tables.

This created a confusing user experience where:
1. The temporal navigation showed "Viewing historical snapshot"
2. But NO data sections were displayed
3. Users couldn't tell what happened between snapshots

### The Solution

**After the fix (Option 2 - Inverted Comparison)**:
- **Display Logic**: Always show the **previous snapshot's data** as the baseline when comparing
- **Change Indicators**: Show what changed in the **selected snapshot** relative to the previous one

#### Example for 0878.689.049:

When viewing **Extract #150** (2025-10-14):
1. **Baseline displayed**: Extract #140's data (includes "A.S.B.L. Villers 2000")
2. **Change indicators**: Red background with "Removed in selected" badge on the denomination
3. **Result**: User can see that the denomination existed before and was removed in Extract #150

When viewing **Extract #157** (current):
1. **Baseline displayed**: Extract #150's data (which was already empty)
2. **Change indicators**: No changes (both empty)
3. **Result**: Clean view showing the enterprise has no denominations currently

### Display Logic for Current Version

When viewing the **current snapshot** (Extract #157, `_is_current = true`):
- **No comparison mode**: Just displays the current data normally
- **No previous snapshot fetched**: Only loads current data from API
- **Clean presentation**: Standard view without change indicators

The comparison feature only activates when:
1. User selects a **historical snapshot** from the dropdown
2. The selected snapshot is NOT marked as `_is_current`
3. A previous snapshot exists to compare against

### Code Implementation

**Display Detail Selection** (`page.tsx:28`):
```typescript
const displayDetail = (comparison && previousDetail ? previousDetail : detail)!
```

**Comparison Trigger** (`page.tsx:67-109`):
```typescript
// Only fetch for comparison if NOT viewing current snapshot
if (!selectedSnapshot.isCurrent) {
  // Fetch selected snapshot detail
  // Find and fetch previous snapshot detail
  // compareEnterprises(current, previous)
}
```

**Visual Indicators**:
- 🟢 Green background + "New in selected" = Added in the selected snapshot
- 🔴 Red background + "Removed in selected" = Was in previous, removed in selected
- 🟡 Yellow dot + "Changed to: X" = Field value changed

### Companies with Multiple Snapshots for Testing

These companies all have 3 snapshots (extracts 140, 150, 157) and can be used for testing:

| Enterprise Number | Snapshot Count | Date Range |
|-------------------|----------------|------------|
| 0670.994.431 | 3 | 140 → 157 |
| 0878.689.049 | 3 | 140 → 157 |
| 0733.584.769 | 3 | 140 → 157 |
| 0739.743.576 | 3 | 140 → 157 |
| 0505.699.996 | 3 | 140 → 157 |

### Verification Checklist

✅ **Temporal versioning correctly applied**:
- Each enterprise can have multiple snapshots
- Only one snapshot marked as `_is_current = true`
- Older snapshots have `_is_current = false`

✅ **Display logic for current version**:
- Shows data from `_is_current = true` snapshot
- No comparison mode active
- No "Viewing historical snapshot" indicator

✅ **Display logic for historical versions**:
- Shows **previous snapshot** data as baseline
- Indicates changes relative to **selected snapshot**
- Clear visual indicators for added/removed/changed items

✅ **Empty data handling**:
- If selected snapshot has empty sections, shows previous snapshot's data
- Marks items as "Removed in selected" appropriately
- Prevents blank/confusing views
