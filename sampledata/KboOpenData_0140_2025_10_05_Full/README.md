# KBO Open Data - Full Dataset Sample

This directory should contain a **full monthly KBO Open Data extract** for testing and analysis.

## What belongs here?

Download a full monthly extract from the KBO Open Data service:
- Visit: https://kbopub.economie.fgov.be/kbo-open-data (requires registration)
- Download a "Full" extract (e.g., `KboOpenData_0140_2025_10_05_Full.zip`)
- Extract the ZIP file to this directory

## Expected file structure

After extraction, this directory should contain:

```
KboOpenData_0140_2025_10_05_Full/
├── meta.csv                  # Extract metadata
├── code.csv                  # Code lookup table (~2 MB)
├── enterprise.csv            # Enterprises (~86 MB)
├── establishment.csv         # Establishments (~67 MB)
├── denomination.csv          # Names (~146 MB)
├── address.csv               # Addresses (~286 MB)
├── contact.csv               # Contact details (~32 MB)
├── activity.csv              # Activities (~1.5 GB)
└── branch.csv                # Branches (~300 KB)
```

## Dataset characteristics

| File | Rows | Size | Description |
|------|------|------|-------------|
| enterprise.csv | ~1.9M | 86 MB | Enterprise entities |
| activity.csv | ~36M | 1.5 GB | Economic activities (largest) |
| address.csv | ~2.8M | 286 MB | Addresses |
| denomination.csv | ~3.3M | 146 MB | Names (all languages) |
| establishment.csv | ~1.7M | 67 MB | Establishment units |
| contact.csv | ~0.7M | 32 MB | Contact details |
| code.csv | ~21K | 2 MB | Code descriptions |
| branch.csv | ~7K | 300 KB | Branch offices |
| meta.csv | 6 | 4 KB | Extract metadata |
| **TOTAL** | **~47M** | **~2.1 GB** | Full dataset |

## Why not in Git?

The full dataset is **~2.1 GB** in CSV format, which is too large for Git repositories. This folder contains only a `.gitkeep` file to preserve the directory structure.

**Note**: After Parquet conversion with ZSTD compression, the dataset compresses to ~100 MB, but we still exclude it from Git to keep the repository lightweight.

## For analysis and testing

To run the analysis queries on this dataset:

```bash
# Navigate to project root
cd ../..

# Run DuckDB analysis
duckdb :memory: < analysis-queries.sql > analysis-results.txt

# Test Parquet compression
./test-parquet-compression.sh
```

See the main [README.md](../../README.md) for more information.

## Update dataset

For testing incremental updates, use the smaller update dataset in:
`sampledata/KboOpenData_0147_2025_10_12_Update/`

This update dataset IS included in Git (< 1 MB).

---

**Need help?** See the [KBO Open Data specification](../../specs/KBOCookbook_EN.md) for details.
