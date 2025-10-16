-- Branches table
-- Branch offices of foreign entities in Belgium
-- Note: KBO dataset only provides minimal information (id, start_date, enterprise_number)

CREATE TABLE IF NOT EXISTS branches (
  -- Primary key
  id VARCHAR PRIMARY KEY,

  -- Foreign entity reference
  enterprise_number VARCHAR,                  -- May be NULL

  -- Basic info
  start_date DATE,

  -- Temporal tracking
  _snapshot_date DATE NOT NULL,
  _extract_number INTEGER NOT NULL,
  _is_current BOOLEAN NOT NULL
);

-- Comments for documentation
COMMENT ON TABLE branches IS 'Foreign entity branches (~7K) - minimal data in KBO';
COMMENT ON COLUMN branches.enterprise_number IS 'Parent enterprise (nullable)';
