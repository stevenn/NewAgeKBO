-- Addresses table (link table)
-- 40% of enterprises have NO address (natural persons)
-- Separate table avoids massive NULLs in enterprises table
-- Supports multi-language address components

CREATE TABLE IF NOT EXISTS addresses (
  -- Primary key
  id VARCHAR PRIMARY KEY,                     -- UUID generated during import

  -- Entity reference
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'
  type_of_address VARCHAR NOT NULL,           -- REGO, BAET, ABBR, OBAD

  -- Address components (multi-language)
  country_nl VARCHAR,
  country_fr VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  extra_address_info VARCHAR,
  date_striking_off DATE,                     -- Date address was removed

  -- Temporal tracking
  _snapshot_date DATE NOT NULL,
  _extract_number INTEGER NOT NULL,
  _is_current BOOLEAN NOT NULL,

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment')),
  CHECK (type_of_address IN ('REGO', 'BAET', 'ABBR', 'OBAD'))
);

-- Comments for documentation
COMMENT ON TABLE addresses IS 'Addresses (~2.8M) for enterprises and establishments';
COMMENT ON COLUMN addresses.type_of_address IS 'REGO=Registered office, BAET=Establishment, ABBR=Branch, OBAD=Oldest active';
COMMENT ON COLUMN addresses.date_striking_off IS 'Date struck off (rare)';
