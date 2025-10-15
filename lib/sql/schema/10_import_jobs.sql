-- Import Jobs table (metadata tracking)
-- Tracks all import operations for monitoring and debugging
-- Stores statistics and status of each import

CREATE TABLE IF NOT EXISTS import_jobs (
  -- Primary key
  id VARCHAR PRIMARY KEY,                     -- UUID

  -- Extract metadata
  extract_number INTEGER UNIQUE NOT NULL,     -- From meta.csv
  extract_type VARCHAR NOT NULL,              -- 'full' or 'update'
  snapshot_date DATE NOT NULL,                -- From meta.csv
  extract_timestamp TIMESTAMP NOT NULL,       -- From meta.csv

  -- Job status
  status VARCHAR NOT NULL,                    -- 'pending', 'running', 'completed', 'failed'
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message TEXT,

  -- Statistics
  records_processed BIGINT DEFAULT 0,
  records_inserted BIGINT DEFAULT 0,
  records_updated BIGINT DEFAULT 0,
  records_deleted BIGINT DEFAULT 0,

  -- Worker info
  worker_type VARCHAR NOT NULL,               -- 'local' or 'vercel'

  -- Constraints
  CHECK (extract_type IN ('full', 'update')),
  CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  CHECK (worker_type IN ('local', 'vercel'))
);

-- Comments for documentation
COMMENT ON TABLE import_jobs IS 'Import job metadata and statistics for monitoring';
COMMENT ON COLUMN import_jobs.extract_number IS 'Unique ID (e.g. 140)';
COMMENT ON COLUMN import_jobs.extract_type IS 'full=Monthly full dataset, update=Daily incremental';
COMMENT ON COLUMN import_jobs.status IS 'pending, running, success, failed';
COMMENT ON COLUMN import_jobs.worker_type IS 'local or vercel';
