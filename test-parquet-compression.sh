#!/bin/bash
# Test Parquet compression on KBO data
# Compares CSV size vs different Parquet compression methods

set -e

echo "=== Parquet Compression Test ==="
echo ""

# Test on activity.csv (largest file - 1.5GB)
INPUT_CSV="sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv"

echo "Input file: $INPUT_CSV"
echo "Original CSV size:"
ls -lh "$INPUT_CSV" | awk '{print $5}'
echo ""

# Create test directory
mkdir -p parquet-test
cd parquet-test

echo "Converting to Parquet with different compression methods..."
echo ""

# Test 1: Uncompressed
echo "[1/4] Uncompressed Parquet..."
duckdb :memory: <<EOF
.timer on
COPY (SELECT * FROM read_csv('../$INPUT_CSV', AUTO_DETECT=TRUE))
TO 'activity_uncompressed.parquet' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED);
EOF

# Test 2: Snappy (default, fast)
echo ""
echo "[2/4] Snappy compression..."
duckdb :memory: <<EOF
.timer on
COPY (SELECT * FROM read_csv('../$INPUT_CSV', AUTO_DETECT=TRUE))
TO 'activity_snappy.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);
EOF

# Test 3: ZSTD (best compression)
echo ""
echo "[3/4] ZSTD compression..."
duckdb :memory: <<EOF
.timer on
COPY (SELECT * FROM read_csv('../$INPUT_CSV', AUTO_DETECT=TRUE))
TO 'activity_zstd.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
EOF

# Test 4: GZIP (slower, good compression)
echo ""
echo "[4/4] GZIP compression..."
duckdb :memory: <<EOF
.timer on
COPY (SELECT * FROM read_csv('../$INPUT_CSV', AUTO_DETECT=TRUE))
TO 'activity_gzip.parquet' (FORMAT PARQUET, COMPRESSION GZIP);
EOF

echo ""
echo "=== Compression Results ==="
echo ""
echo "File sizes:"
ls -lh activity_*.parquet

echo ""
echo "Compression ratios (vs original CSV):"
CSV_SIZE=$(stat -f%z "../$INPUT_CSV")

for file in activity_*.parquet; do
    PARQUET_SIZE=$(stat -f%z "$file")
    RATIO=$(echo "scale=2; $CSV_SIZE / $PARQUET_SIZE" | bc)
    SAVINGS=$(echo "scale=1; (1 - $PARQUET_SIZE / $CSV_SIZE) * 100" | bc)
    echo "$file: ${RATIO}x compression (${SAVINGS}% savings)"
done

echo ""
echo "=== Read Performance Test ==="
echo ""

# Test read performance
echo "Reading each Parquet file and counting rows..."

for file in activity_*.parquet; do
    echo ""
    echo "Reading $file:"
    duckdb :memory: <<EOF
.timer on
SELECT COUNT(*) as row_count FROM read_parquet('$file');
EOF
done

echo ""
echo "=== Test Complete ==="
echo ""
echo "Recommendation:"
echo "- Use ZSTD for storage (best compression)"
echo "- Use Snappy for temporary/intermediate files (fast)"

# Cleanup
echo ""
read -p "Delete test Parquet files? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd ..
    rm -rf parquet-test
    echo "Cleaned up test files"
else
    echo "Test files kept in parquet-test/"
fi
