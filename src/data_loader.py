"""Data loader for importing Ciqual XML data into SQLite

Handles downloading, parsing, and importing CIQUAL data from Zenodo
into a local SQLite database with proper indexing and FTS support.
"""

import sqlite3
try:
    from lxml import etree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from pathlib import Path
import urllib.request
import io
import re
import json
import tempfile
import time
from database import SCHEMA_SQL

ZENODO_CONCEPT_RECORD = "17550132"
ZENODO_API_URL = f"https://zenodo.org/api/records/{ZENODO_CONCEPT_RECORD}/versions/latest"

# File prefix patterns for matching Zenodo files
FILE_PREFIXES = {
    "alim": "alim_",
    "const": "const_",
    "compo": "compo_",
    "alim_grp": "alim_grp_",
    "sources": "sources_",
}

def extract_unit(nutrient_name):
    """Extract unit from nutrient name

    Args:
        nutrient_name: Nutrient name possibly containing unit in parentheses

    Returns:
        Extracted unit string (e.g., 'mg/100g') or None

    Example:
        >>> extract_unit('Calcium (mg/100g)')
        'mg/100g'
    """
    if not nutrient_name:
        return None
    match = re.search(r'\(([^)]+/100\s?g)\)', nutrient_name)
    if match:
        return match.group(1).replace(' ', '')
    return None

def clean_text(text):
    """Clean and normalize text values

    Args:
        text: Raw text value from XML

    Returns:
        Cleaned text or None for empty/missing values
    """
    if text is None:
        return None
    text = text.strip()
    if text == "" or text == "missing":
        return None
    return text

def parse_number(value):
    """Parse a number from French format

    Args:
        value: Number string with comma as decimal separator

    Returns:
        Float value or None for invalid/missing values

    Note:
        Handles French decimal format (comma instead of dot)
        Returns None for special values like '-', 'traces'
    """
    if value is None or value.strip() in ["", "-", "traces"]:
        return None
    try:
        # Replace comma with dot for decimal
        return float(value.strip().replace(",", "."))
    except ValueError:
        return None

def _get_element_text(element, tag):
    """Get text content of a child element, or None if missing."""
    child = element.find(tag)
    if child is not None and child.text is not None:
        return child.text
    return None

def _fetch_zenodo_metadata():
    """Fetch latest version metadata from Zenodo API.

    Returns:
        dict with keys: record_id, version, files (list of {name, download_url})
    """
    req = urllib.request.Request(ZENODO_API_URL)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))

    files = []
    for f in data.get("files", []):
        name = f.get("key", "")
        download_url = f.get("links", {}).get("self", "")
        files.append({"name": name, "download_url": download_url})

    return {
        "record_id": str(data["id"]),
        "version": data.get("metadata", {}).get("version", "unknown"),
        "files": files,
    }

def _find_file(files, prefix):
    """Find a file matching the given prefix (e.g. 'alim_') among Zenodo files."""
    for f in files:
        name = f["name"]
        # Match prefix, ensure it's an XML file, and exclude alim_grp when looking for alim_
        if name.endswith(".xml") and name.startswith(prefix):
            if prefix == "alim_" and name.startswith("alim_grp_"):
                continue
            return f
    return None

def _download_xml(url):
    """Download and parse an XML file from a URL.

    Tries UTF-8 first, falls back to windows-1252.
    Returns the root element.
    """
    with urllib.request.urlopen(url) as response:
        raw = response.read()

    # Try UTF-8 first, fallback to windows-1252
    for encoding in ("utf-8", "windows-1252"):
        try:
            try:
                from lxml import etree
                parser = etree.XMLParser(encoding=encoding, recover=True)
                root = etree.fromstring(raw, parser)
                return root
            except ImportError:
                content = raw.decode(encoding, errors="ignore")
                # Remove control characters except tab, newline, carriage return
                content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
                return ET.fromstring(content)
        except Exception:
            continue

    raise ValueError("Failed to parse XML with both UTF-8 and windows-1252 encodings")

def should_update_database(db_path):
    """Check if database needs updating by comparing Zenodo record IDs.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        True if database needs update
    """
    if not db_path.exists():
        return True

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if metadata table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")
        if not cursor.fetchone():
            conn.close()
            return True

        # Check last update check timestamp (skip API call if checked recently)
        cursor.execute("SELECT value FROM metadata WHERE key = 'last_update_check'")
        row = cursor.fetchone()
        if row:
            last_check = float(row[0])
            days_since_check = (time.time() - last_check) / (24 * 3600)
            if days_since_check < 30:
                conn.close()
                return False

        # Get stored record ID
        cursor.execute("SELECT value FROM metadata WHERE key = 'zenodo_record_id'")
        row = cursor.fetchone()
        stored_record_id = row[0] if row else None
        conn.close()

        # Fetch latest from Zenodo API
        metadata = _fetch_zenodo_metadata()
        latest_record_id = metadata["record_id"]

        # Update last check timestamp
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_update_check', ?)",
            (str(time.time()),)
        )
        conn.commit()
        conn.close()

        return stored_record_id != latest_record_id

    except Exception:
        # If anything fails, trigger an update
        return True

def initialize_database(force_update=False):
    """Download and import Ciqual data from Zenodo into SQLite database

    Args:
        force_update: Force database update even if cache is valid

    Raises:
        Exception: If data download or import fails and no existing database

    Note:
        Creates database at ~/.ciqual/ciqual.db
        Downloads XML data from Zenodo (CIQUAL 2024 dataset)
    """

    # Setup paths
    db_path = Path.home() / ".ciqual" / "ciqual.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if update is needed
    if not force_update and db_path.exists() and not should_update_database(db_path):
        print("Database is up to date")
        return

    print("Fetching CIQUAL data metadata from Zenodo...")
    zenodo_meta = _fetch_zenodo_metadata()
    files = zenodo_meta["files"]

    # Find required XML files
    alim_file = _find_file(files, "alim_")
    const_file = _find_file(files, "const_")
    compo_file = _find_file(files, "compo_")
    grp_file = _find_file(files, "alim_grp_")
    sources_file = _find_file(files, "sources_")

    if not alim_file or not const_file or not compo_file:
        raise Exception("Required XML files not found in Zenodo record")

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Drop old tables for clean reimport
        print("Creating database schema...")
        conn.executescript("""
            DROP TABLE IF EXISTS foods_fts;
            DROP TABLE IF EXISTS composition;
            DROP TABLE IF EXISTS foods;
            DROP TABLE IF EXISTS nutrients;
            DROP TABLE IF EXISTS food_groups;
            DROP TABLE IF EXISTS sources;
        """)
        conn.executescript(SCHEMA_SQL)

        # Load nutrients
        print("Downloading and loading nutrients...")
        root = _download_xml(const_file["download_url"])

        for const in root.findall("CONST"):
            const_code = clean_text(_get_element_text(const, "const_code"))
            const_nom_fr = _get_element_text(const, "const_nom_fr")
            const_nom_eng = _get_element_text(const, "const_nom_eng")
            code_infoods = clean_text(_get_element_text(const, "code_INFOODS"))

            if const_code:
                unit = extract_unit(const_nom_fr) or extract_unit(const_nom_eng)
                cursor.execute(
                    "INSERT OR REPLACE INTO nutrients (const_code, const_nom_fr, const_nom_eng, unit, code_infoods) VALUES (?, ?, ?, ?, ?)",
                    (int(const_code), const_nom_fr, const_nom_eng, unit, code_infoods)
                )

        # Load food groups
        if grp_file:
            print("Downloading and loading food groups...")
            root = _download_xml(grp_file["download_url"])

            for grp in root.findall("ALIM_GRP"):
                alim_grp_code = clean_text(_get_element_text(grp, "alim_grp_code"))
                alim_grp_nom_fr = _get_element_text(grp, "alim_grp_nom_fr")
                alim_grp_nom_eng = _get_element_text(grp, "alim_grp_nom_eng")
                alim_ssgrp_code = clean_text(_get_element_text(grp, "alim_ssgrp_code"))
                alim_ssgrp_nom_fr = _get_element_text(grp, "alim_ssgrp_nom_fr")
                alim_ssgrp_nom_eng = _get_element_text(grp, "alim_ssgrp_nom_eng")
                alim_ssssgrp_code = clean_text(_get_element_text(grp, "alim_ssssgrp_code"))
                alim_ssssgrp_nom_fr = _get_element_text(grp, "alim_ssssgrp_nom_fr")
                alim_ssssgrp_nom_eng = _get_element_text(grp, "alim_ssssgrp_nom_eng")

                if alim_grp_code:
                    cursor.execute(
                        """INSERT OR REPLACE INTO food_groups
                        (alim_grp_code, alim_grp_nom_fr, alim_grp_nom_eng,
                         alim_ssgrp_code, alim_ssgrp_nom_fr, alim_ssgrp_nom_eng,
                         alim_ssssgrp_code, alim_ssssgrp_nom_fr, alim_ssssgrp_nom_eng)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (alim_grp_code, alim_grp_nom_fr, alim_grp_nom_eng,
                         alim_ssgrp_code or "", alim_ssgrp_nom_fr, alim_ssgrp_nom_eng,
                         alim_ssssgrp_code or "", alim_ssssgrp_nom_fr, alim_ssssgrp_nom_eng)
                    )

        # Load sources
        if sources_file:
            print("Downloading and loading sources...")
            root = _download_xml(sources_file["download_url"])

            for src in root.findall("SOURCE"):
                source_code = clean_text(_get_element_text(src, "source_code"))
                ref_citation = _get_element_text(src, "ref_citation")

                if source_code:
                    cursor.execute(
                        "INSERT OR REPLACE INTO sources (source_code, ref_citation) VALUES (?, ?)",
                        (int(source_code), ref_citation)
                    )

        # Load foods
        print("Downloading and loading foods...")
        root = _download_xml(alim_file["download_url"])

        food_count = 0
        for alim in root.findall("ALIM"):
            alim_code = clean_text(_get_element_text(alim, "alim_code"))
            alim_nom_fr = _get_element_text(alim, "alim_nom_fr")
            alim_nom_eng = _get_element_text(alim, "alim_nom_eng")
            alim_grp_code = clean_text(_get_element_text(alim, "alim_grp_code"))
            alim_nom_sci = _get_element_text(alim, "alim_nom_sci")
            alim_ssgrp_code = clean_text(_get_element_text(alim, "alim_ssgrp_code"))
            alim_ssssgrp_code = clean_text(_get_element_text(alim, "alim_ssssgrp_code"))
            facteur_jones = parse_number(_get_element_text(alim, "facteur_Jones"))

            if alim_code:
                cursor.execute(
                    """INSERT OR REPLACE INTO foods
                    (alim_code, alim_nom_fr, alim_nom_eng, alim_grp_code,
                     alim_nom_sci, alim_ssgrp_code, alim_ssssgrp_code, facteur_Jones)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (int(alim_code), alim_nom_fr, alim_nom_eng, alim_grp_code,
                     alim_nom_sci, alim_ssgrp_code, alim_ssssgrp_code, facteur_jones)
                )
                food_count += 1

        print(f"Loaded {food_count} foods")

        # Load composition data
        print("Downloading and loading nutritional composition data (this may take a minute)...")
        root = _download_xml(compo_file["download_url"])

        compo_count = 0
        batch = []
        for compo in root.findall("COMPO"):
            alim_code = clean_text(_get_element_text(compo, "alim_code"))
            const_code = clean_text(_get_element_text(compo, "const_code"))
            teneur = _get_element_text(compo, "teneur")
            code_confiance = clean_text(_get_element_text(compo, "code_confiance"))
            min_val = parse_number(_get_element_text(compo, "min"))
            max_val = parse_number(_get_element_text(compo, "max"))
            source_code_val = clean_text(_get_element_text(compo, "source_code"))

            if alim_code and const_code:
                teneur_value = parse_number(teneur)
                if teneur_value is not None:
                    sc = int(source_code_val) if source_code_val else None
                    batch.append((int(alim_code), int(const_code), teneur_value,
                                  code_confiance, min_val, max_val, sc))

                    if len(batch) >= 1000:
                        cursor.executemany(
                            "INSERT OR REPLACE INTO composition (alim_code, const_code, teneur, code_confiance, min, max, source_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            batch
                        )
                        compo_count += len(batch)
                        batch = []

        # Insert remaining batch
        if batch:
            cursor.executemany(
                "INSERT OR REPLACE INTO composition (alim_code, const_code, teneur, code_confiance, min, max, source_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
                batch
            )
            compo_count += len(batch)

        print(f"Loaded {compo_count} nutritional values")

        # Populate FTS table
        print("Building full-text search index...")
        try:
            conn.execute("DELETE FROM foods_fts")
        except Exception:
            pass  # Table might not exist or be empty
        conn.execute("INSERT INTO foods_fts SELECT alim_code, alim_nom_fr, alim_nom_eng, alim_nom_sci FROM foods")

        # Store version metadata
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('zenodo_record_id', ?)",
            (zenodo_meta["record_id"],)
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('zenodo_version', ?)",
            (zenodo_meta["version"],)
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('downloaded_at', ?)",
            (str(time.time()),)
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_update_check', ?)",
            (str(time.time()),)
        )

        # Commit and verify
        conn.commit()

        food_count = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
        nutrient_count = conn.execute("SELECT COUNT(*) FROM nutrients").fetchone()[0]
        comp_count = conn.execute("SELECT COUNT(*) FROM composition").fetchone()[0]

        print(f"Database initialization complete!")
        print(f"  - {food_count} foods")
        print(f"  - {nutrient_count} nutrients")
        print(f"  - {comp_count} composition entries")

        if food_count == 0:
            raise Exception("No data was loaded into the database")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    initialize_database()
