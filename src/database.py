"""SQLite database schema for Ciqual data"""

SCHEMA_SQL = """
-- Core tables
CREATE TABLE IF NOT EXISTS foods (
    alim_code INTEGER PRIMARY KEY,
    alim_nom_fr TEXT,
    alim_nom_eng TEXT,
    alim_grp_code TEXT,
    alim_nom_sci TEXT,
    alim_ssgrp_code TEXT,
    alim_ssssgrp_code TEXT,
    facteur_Jones REAL
);

CREATE TABLE IF NOT EXISTS nutrients (
    const_code INTEGER PRIMARY KEY,
    const_nom_fr TEXT,
    const_nom_eng TEXT,
    unit TEXT,
    code_infoods TEXT
);

CREATE TABLE IF NOT EXISTS composition (
    alim_code INTEGER,
    const_code INTEGER,
    teneur REAL,
    code_confiance TEXT,
    min REAL,
    max REAL,
    source_code INTEGER,
    PRIMARY KEY (alim_code, const_code),
    FOREIGN KEY (alim_code) REFERENCES foods(alim_code),
    FOREIGN KEY (const_code) REFERENCES nutrients(const_code)
);

CREATE TABLE IF NOT EXISTS food_groups (
    alim_grp_code TEXT,
    alim_grp_nom_fr TEXT,
    alim_grp_nom_eng TEXT,
    alim_ssgrp_code TEXT,
    alim_ssgrp_nom_fr TEXT,
    alim_ssgrp_nom_eng TEXT,
    alim_ssssgrp_code TEXT,
    alim_ssssgrp_nom_fr TEXT,
    alim_ssssgrp_nom_eng TEXT,
    PRIMARY KEY (alim_grp_code, alim_ssgrp_code, alim_ssssgrp_code)
);

CREATE TABLE IF NOT EXISTS sources (
    source_code INTEGER PRIMARY KEY,
    ref_citation TEXT
);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Full-text search for fuzzy matching
CREATE VIRTUAL TABLE IF NOT EXISTS foods_fts USING fts5(
    alim_code,
    alim_nom_fr,
    alim_nom_eng,
    alim_nom_sci,
    content=foods,
    tokenize='unicode61 remove_diacritics 1'
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_composition_nutrient ON composition(const_code);
CREATE INDEX IF NOT EXISTS idx_composition_source ON composition(source_code);
CREATE INDEX IF NOT EXISTS idx_foods_group ON foods(alim_grp_code);
CREATE INDEX IF NOT EXISTS idx_foods_name_fr ON foods(alim_nom_fr);
CREATE INDEX IF NOT EXISTS idx_foods_name_eng ON foods(alim_nom_eng);
"""
