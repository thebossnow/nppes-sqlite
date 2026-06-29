"""Normalized SQLite schema for NPPES NPI data.

Run: python -m nppes.schema --db path/to/nppes.db
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;

-- Main provider record (one row per NPI)
CREATE TABLE IF NOT EXISTS providers (
    npi TEXT PRIMARY KEY,
    entity_type TEXT,                 -- '1' individual, '2' organization
    replacement_npi TEXT,
    ein TEXT,
    org_name TEXT,
    last_name TEXT,
    first_name TEXT,
    middle_name TEXT,
    name_prefix TEXT,
    name_suffix TEXT,
    credential TEXT,
    sex TEXT,
    sole_proprietor TEXT,             -- 'Y'/'N'
    is_organization_subpart TEXT,
    parent_org_lbn TEXT,
    parent_org_tin TEXT,

    enumeration_date TEXT,            -- YYYY-MM-DD
    last_update_date TEXT,            -- YYYY-MM-DD
    deactivation_date TEXT,
    deactivation_reason TEXT,
    reactivation_date TEXT,
    certification_date TEXT,

    -- Mailing address
    mailing_line1 TEXT,
    mailing_line2 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_postal TEXT,
    mailing_country TEXT,
    mailing_phone TEXT,
    mailing_fax TEXT,

    -- Primary practice / business location (from main file)
    location_line1 TEXT,
    location_line2 TEXT,
    location_city TEXT,
    location_state TEXT,
    location_postal TEXT,
    location_country TEXT,
    location_phone TEXT,
    location_fax TEXT,

    -- Authorized official (Type 2)
    auth_official_last_name TEXT,
    auth_official_first_name TEXT,
    auth_official_middle_name TEXT,
    auth_official_title TEXT,
    auth_official_phone TEXT,
    auth_official_prefix TEXT,
    auth_official_suffix TEXT,
    auth_official_credential TEXT,

    status TEXT,                      -- derived: active/deactivated
    db_loaded_at TEXT DEFAULT (datetime('now'))
);

-- Repeating: taxonomies (up to 15 per NPI in raw, usually 1-3)
CREATE TABLE IF NOT EXISTS taxonomies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
    code TEXT NOT NULL,
    license TEXT,
    license_state TEXT,
    is_primary TEXT,                  -- 'Y'/'N'/'X' (X = not primary)
    taxonomy_group TEXT,
    UNIQUE(npi, code, license_state)
);

-- Repeating: other identifiers (up to 50)
CREATE TABLE IF NOT EXISTS other_identifiers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
    identifier TEXT NOT NULL,
    type_code TEXT,
    state TEXT,
    issuer TEXT,
    UNIQUE(npi, identifier, type_code)
);

-- Non-primary practice locations (from pl_pfile reference)
CREATE TABLE IF NOT EXISTS practice_locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
    line1 TEXT,
    line2 TEXT,
    city TEXT,
    state TEXT,
    postal TEXT,
    country TEXT,
    phone TEXT,
    phone_ext TEXT,
    fax TEXT
);

-- Endpoints (from endpoint_pfile)
CREATE TABLE IF NOT EXISTS endpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
    type TEXT,
    type_description TEXT,
    endpoint TEXT,
    affiliation TEXT,
    use_code TEXT,
    use_description TEXT,
    content_type TEXT,
    content_description TEXT,
    affiliation_lbn TEXT,
    affiliation_line1 TEXT,
    affiliation_line2 TEXT,
    affiliation_city TEXT,
    affiliation_state TEXT,
    affiliation_postal TEXT,
    affiliation_country TEXT
);

-- Other names (primarily for orgs / Type 2) from othername_pfile
CREATE TABLE IF NOT EXISTS other_names (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
    name TEXT,
    type_code TEXT,
    created_date TEXT
);

-- Indexes for the access patterns we actually need
CREATE INDEX IF NOT EXISTS idx_providers_npi ON providers(npi);
CREATE INDEX IF NOT EXISTS idx_providers_state_postal ON providers(location_state, location_postal);
CREATE INDEX IF NOT EXISTS idx_providers_last_update ON providers(last_update_date);

CREATE INDEX IF NOT EXISTS idx_tax_npi ON taxonomies(npi);
CREATE INDEX IF NOT EXISTS idx_tax_code ON taxonomies(code);
CREATE INDEX IF NOT EXISTS idx_tax_code_state ON taxonomies(code, license_state);

CREATE INDEX IF NOT EXISTS idx_ident_npi ON other_identifiers(npi);

CREATE INDEX IF NOT EXISTS idx_pl_npi ON practice_locations(npi);
CREATE INDEX IF NOT EXISTS idx_pl_state_postal ON practice_locations(state, postal);

CREATE INDEX IF NOT EXISTS idx_end_npi ON endpoints(npi);

CREATE INDEX IF NOT EXISTS idx_on_npi ON other_names(npi);

-- Helpful view for "active only"
CREATE VIEW IF NOT EXISTS active_providers AS
SELECT * FROM providers
WHERE (deactivation_date IS NULL OR deactivation_date = '')
  AND (status IS NULL OR status != 'deactivated');
"""


def get_db(path: str | Path = "nppes.db") -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def main(argv: Optional[list[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Initialize or inspect the NPPES SQLite DB")
    parser.add_argument("--db", default="nppes.db", help="Path to SQLite DB file")
    parser.add_argument("--info", action="store_true", help="Show table counts after init")
    args = parser.parse_args(argv)

    conn = get_db(args.db)
    init_db(conn)
    print(f"Schema initialized (or already present) at {args.db}")

    if args.info:
        for table in ("providers", "taxonomies", "other_identifiers", "practice_locations", "endpoints", "other_names"):
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {n:,}")
    conn.close()


if __name__ == "__main__":
    main()
