-- Contacts table (link table)
-- Stores contact details (phone, email, web) for enterprises and establishments

CREATE TABLE IF NOT EXISTS contacts (
  -- Primary key (composite to support temporal tracking)
  id VARCHAR NOT NULL,                        -- Concatenated: entity_number_entity_contact_contact_type_value
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Entity reference
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'
  entity_contact VARCHAR NOT NULL,            -- ENT=Enterprise, ESTB=Establishment, BRANCH=Branch

  -- Contact details
  contact_type VARCHAR NOT NULL,              -- TEL, EMAIL, WEB, etc.
  contact_value VARCHAR NOT NULL,             -- The actual contact information

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number),

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment'))
);

-- Comments for documentation
COMMENT ON TABLE contacts IS 'Contact info (~690K) - phone, email, website. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN contacts.id IS 'Composite key: entity_number_entity_contact_type_hash(value). Ensures uniqueness across versions.';
COMMENT ON COLUMN contacts._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN contacts._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN contacts.entity_number IS 'Enterprise or establishment number (FK to either table)';
COMMENT ON COLUMN contacts.entity_contact IS 'ENT, ESTB, or BRANCH';
COMMENT ON COLUMN contacts.contact_type IS 'TEL, EMAIL, WEB';
COMMENT ON COLUMN contacts.contact_value IS 'Phone, email, or URL';
COMMENT ON COLUMN contacts._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN contacts._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation.';
