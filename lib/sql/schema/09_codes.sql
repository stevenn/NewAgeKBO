-- Codes table (lookup table - STATIC)
-- Multilingual descriptions for all code categories
-- 21,501 rows from code.csv
-- Loaded once, never changes per snapshot
-- Used for runtime JOIN to get human-readable descriptions

CREATE TABLE IF NOT EXISTS codes (
  -- Composite primary key
  category VARCHAR NOT NULL,                  -- JuridicalForm, JuridicalSituation, etc.
  code VARCHAR NOT NULL,
  language VARCHAR NOT NULL,                  -- NL, FR, DE (EN not available in code.csv)

  -- Description
  description VARCHAR NOT NULL,

  PRIMARY KEY (category, code, language),

  -- Constraints
  CHECK (language IN ('NL', 'FR', 'DE'))
);

-- Comments for documentation
COMMENT ON TABLE codes IS 'Multilingual lookup codes (21K) - juridical forms, statuses, etc.';
COMMENT ON COLUMN codes.category IS 'JuridicalForm, Status, ActivityGroup, etc.';
COMMENT ON COLUMN codes.language IS 'NL, FR, or DE';

-- Example categories:
-- JuridicalForm: Legal form codes (e.g., "030" = "Buitenlandse entiteit" in NL)
-- JuridicalSituation: Juridical situation codes
-- ActivityGroup: Activity group codes (001-007)
-- TypeOfAddress: Address type codes (REGO, BAET, ABBR, OBAD)
-- TypeOfDenomination: Denomination type codes (001-004)
-- ContactType: Contact type codes (TEL, EMAIL, WEB)
-- Classification: Activity classification (MAIN, SECO, ANCI)
-- Status: Enterprise status (AC, ST)
