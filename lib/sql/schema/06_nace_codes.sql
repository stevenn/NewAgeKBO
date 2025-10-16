-- NACE Codes table (lookup table - STATIC)
-- 7,265 unique NACE codes across 3 versions
-- Descriptions are 150-200 chars each
-- Loaded once from code.csv, never changes per snapshot
-- Avoids storing descriptions 36M times in activities table

CREATE TABLE IF NOT EXISTS nace_codes (
  -- Composite primary key
  nace_version VARCHAR NOT NULL,              -- 2003, 2008, 2025
  nace_code VARCHAR NOT NULL,

  -- Multi-language descriptions (KBO only provides NL and FR)
  description_nl VARCHAR,
  description_fr VARCHAR,

  PRIMARY KEY (nace_version, nace_code),

  -- Constraints
  CHECK (nace_version IN ('2003', '2008', '2025'))
);

-- Comments for documentation
COMMENT ON TABLE nace_codes IS 'NACE economic activity codes (10K) - static lookup';
COMMENT ON COLUMN nace_codes.nace_version IS '2003, 2008, or 2025';
