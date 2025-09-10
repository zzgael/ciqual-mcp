"""SQLite database schema for Ciqual data"""

SCHEMA_SQL = """
-- Core tables
CREATE TABLE IF NOT EXISTS foods (
    alim_code INTEGER PRIMARY KEY,
    alim_nom_fr TEXT,
    alim_nom_eng TEXT,
    alim_grp_code TEXT
);

CREATE TABLE IF NOT EXISTS nutrients (
    const_code INTEGER PRIMARY KEY,
    const_nom_fr TEXT,
    const_nom_eng TEXT,
    unit TEXT
);

CREATE TABLE IF NOT EXISTS composition (
    alim_code INTEGER,
    const_code INTEGER,
    teneur REAL,
    code_confiance TEXT,
    PRIMARY KEY (alim_code, const_code),
    FOREIGN KEY (alim_code) REFERENCES foods(alim_code),
    FOREIGN KEY (const_code) REFERENCES nutrients(const_code)
);

CREATE TABLE IF NOT EXISTS food_groups (
    grp_code TEXT PRIMARY KEY,
    grp_nom_fr TEXT,
    grp_nom_eng TEXT
);

-- Full-text search for fuzzy matching
CREATE VIRTUAL TABLE IF NOT EXISTS foods_fts USING fts5(
    alim_code,
    alim_nom_fr,
    alim_nom_eng,
    content=foods,
    tokenize='unicode61 remove_diacritics 1'
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_composition_nutrient ON composition(const_code);
CREATE INDEX IF NOT EXISTS idx_foods_group ON foods(alim_grp_code);
CREATE INDEX IF NOT EXISTS idx_foods_name_fr ON foods(alim_nom_fr);
CREATE INDEX IF NOT EXISTS idx_foods_name_eng ON foods(alim_nom_eng);
"""