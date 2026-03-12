#!/usr/bin/env python3
"""ANSES Ciqual MCP Server with SQL query interface

Provides a Model Context Protocol (MCP) server for querying the ANSES Ciqual
French food composition database using SQL.
"""

from fastmcp import FastMCP
import sqlite3
import os
from pathlib import Path
import sys
import logging
import fcntl
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

mcp = FastMCP("ANSES Ciqual")

DB_PATH = Path.home() / ".ciqual" / "ciqual.db"

@mcp.tool()
async def query(sql: str) -> list[dict]:
    """Execute read-only SQL on ANSES Ciqual French food composition database (~3100 foods, 67 nutrients).

    ⚠️ USE MAX 2 QUERIES. Most tasks need only 1. NEVER query one nutrient at a time.

    === ONE-QUERY PATTERNS (preferred) ===

    SEARCH + ALL NUTRIENTS IN ONE QUERY (use this!):
    SELECT f.alim_code, f.alim_nom_fr, f.alim_nom_eng, f.alim_nom_sci,
           n.const_nom_fr, n.code_infoods, c.teneur, c.min, c.max, n.unit, c.code_confiance
    FROM foods_fts fts
    JOIN foods f ON f.alim_code = fts.alim_code
    JOIN composition c ON f.alim_code = c.alim_code
    JOIN nutrients n ON c.const_code = n.const_code
    WHERE fts MATCH 'pomme'
    ORDER BY f.alim_code, n.const_code LIMIT 200;

    SEARCH MULTIPLE FOODS + SPECIFIC NUTRIENTS AT ONCE:
    SELECT f.alim_code, f.alim_nom_fr, c.teneur, n.unit, n.const_nom_fr
    FROM foods_fts fts
    JOIN foods f ON f.alim_code = fts.alim_code
    JOIN composition c ON f.alim_code = c.alim_code
    JOIN nutrients n ON c.const_code = n.const_code
    WHERE fts MATCH 'steak OR boeuf OR bœuf' AND c.const_code IN (328,25000,40000,31000)
    ORDER BY f.alim_code;

    COMPARE FOODS ON KEY MACROS (single query):
    SELECT f.alim_nom_fr,
      MAX(CASE WHEN c.const_code=328 THEN c.teneur END) as kcal,
      MAX(CASE WHEN c.const_code=25000 THEN c.teneur END) as protein_g,
      MAX(CASE WHEN c.const_code=40000 THEN c.teneur END) as fat_g,
      MAX(CASE WHEN c.const_code=31000 THEN c.teneur END) as carbs_g
    FROM foods f JOIN composition c ON f.alim_code=c.alim_code
    WHERE f.alim_code IN (2028,2003,3001) GROUP BY f.alim_code;

    BROWSE BY FOOD GROUP/SUBGROUP:
    SELECT f.alim_code, f.alim_nom_fr, g.alim_grp_nom_fr, g.alim_ssgrp_nom_fr
    FROM foods f
    JOIN food_groups g ON f.alim_grp_code = g.alim_grp_code
      AND f.alim_ssgrp_code = g.alim_ssgrp_code
      AND f.alim_ssssgrp_code = g.alim_ssssgrp_code
    WHERE g.alim_grp_nom_fr LIKE '%fruit%';

    GET SOURCE/REFERENCE FOR A VALUE:
    SELECT s.ref_citation FROM composition c
    JOIN sources s ON c.source_code = s.source_code
    WHERE c.alim_code = 2028 AND c.const_code = 328;

    === FTS TIPS ===
    - FTS tokenizes on diacritics: "pâte" matches "pate". Use OR: 'steak OR boeuf'.
    - Prefix search: 'pomm*' matches pomme, pommeau, etc.
    - alim_nom_sci (scientific name) is also indexed.
    - Prefer French food names (alim_nom_fr) — they are more complete.

    === COMPOUND DISHES ===
    CIQUAL has ingredients, not recipes. Search each component separately and sum by portion weight (e.g. meat 150g, sauce 30g, bread 50g). You can search multiple components in one FTS query using OR.

    === KEY NUTRIENT const_code VALUES ===
    Energy: 328 (kcal), 327 (kJ)
    Macros: 25000 (protein), 40000 (fat), 31000 (carbs), 32000 (sugars), 34100 (fiber)
    Fat detail: 40400 (saturated FA), 40302 (monounsat FA), 40303 (polyunsat FA)
    Minerals: 10110 (Na), 10200 (Ca), 10260 (Fe), 10190 (K), 10120 (Mg), 10530 (Zn)
    Vitamins: 55100 (C), 52100 (D), 56600 (B12), 53100 (E), 56700 (folates/B9), 56100 (B6), 54100 (A retinol)
    Other: 400 (water), 60000 (cholesterol), 75100 (alcohol)

    === SCHEMA ===
    foods(alim_code PK, alim_nom_fr, alim_nom_eng, alim_grp_code, alim_nom_sci, alim_ssgrp_code, alim_ssssgrp_code, facteur_Jones)
    nutrients(const_code PK, const_nom_fr, const_nom_eng, unit, code_infoods)
    composition(alim_code, const_code, teneur, code_confiance [A-D quality], min, max, source_code) PK(alim_code,const_code)
    food_groups(alim_grp_code, alim_grp_nom_fr/eng, alim_ssgrp_code/nom, alim_ssssgrp_code/nom) PK(grp+ssgrp+ssssgrp)
    sources(source_code PK, ref_citation)
    foods_fts: FTS5(alim_code, alim_nom_fr, alim_nom_eng, alim_nom_sci) — use MATCH operator
    """

    # Ensure database exists
    if not DB_PATH.exists():
        logger.warning("Database not found at %s", DB_PATH)
        return [{"error": "Database not initialized. Please run the server first to download data."}]

    # Validate SQL query (basic safety check)
    sql_lower = sql.strip().lower()
    if not sql_lower.startswith(('select', 'with')):
        return [{"error": "Only SELECT queries are allowed for safety."}]

    # Connect with read-only mode
    try:
        logger.debug("Executing query: %s", sql[:100] + '...' if len(sql) > 100 else sql)
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row

        # Execute query with timeout
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA temp_store = MEMORY")
        cursor = conn.execute(sql)
        results = [dict(row) for row in cursor.fetchall()]
        logger.debug("Query returned %d rows", len(results))
        return results

    except sqlite3.OperationalError as e:
        logger.error("SQL operational error: %s", e)
        if "no such table" in str(e):
            return [{"error": f"Table not found. Available tables: foods, nutrients, composition, foods_fts, food_groups, sources, metadata"}]
        elif "read-only" in str(e) or "readonly" in str(e):
            return [{"error": "Database is read-only. Only SELECT queries are allowed."}]
        else:
            return [{"error": f"SQL error: {str(e)}"}]
    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
        return [{"error": f"Database error: {str(e)}"}]
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return [{"error": f"Unexpected error: {str(e)}"}]
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Main entry point for the MCP server

    Initializes the database if needed and starts the MCP server.
    """
    # Check and update database if needed
    from data_loader import initialize_database, should_update_database
    import sqlite3

    def repair_fts5_if_corrupted():
        """Repair FTS5 index if corrupted (e.g., from concurrent writes)

        Uses file locking to prevent multiple instances from repairing simultaneously.
        """
        lock_file = DB_PATH.parent / ".ciqual.lock"
        lock_fd = None

        try:
            conn = sqlite3.connect(DB_PATH)
            # Test FTS5 integrity
            conn.execute("SELECT * FROM foods_fts LIMIT 1").fetchone()
            conn.close()
            return False  # No corruption
        except sqlite3.OperationalError as e:
            if "fts5" in str(e).lower() or "missing row" in str(e).lower():
                logger.warning("FTS5 corruption detected, acquiring lock for repair...")
                print("FTS5 corruption detected, acquiring lock for repair...", file=sys.stderr)

                # Acquire exclusive lock
                try:
                    lock_fd = open(lock_file, 'w')
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
                    logger.info("Lock acquired, rebuilding FTS5 index...")
                    print("Lock acquired, rebuilding FTS5 index...", file=sys.stderr)

                    # Double-check corruption still exists (another instance might have fixed it)
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        conn.execute("SELECT * FROM foods_fts LIMIT 1").fetchone()
                        conn.close()
                        logger.info("FTS5 already repaired by another instance")
                        print("FTS5 already repaired by another instance", file=sys.stderr)
                        return False
                    except sqlite3.OperationalError:
                        pass  # Still corrupted, proceed with repair

                    # Rebuild FTS5 index
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute("DELETE FROM foods_fts")
                    conn.execute("INSERT INTO foods_fts SELECT alim_code, alim_nom_fr, alim_nom_eng, alim_nom_sci FROM foods")
                    conn.commit()
                    conn.close()

                    logger.info("FTS5 index rebuilt successfully")
                    print("FTS5 index rebuilt successfully", file=sys.stderr)
                    return True

                except Exception as repair_error:
                    logger.error("Failed to repair FTS5 index: %s", repair_error)
                    print(f"Failed to repair FTS5 index: {repair_error}", file=sys.stderr)
                    return False
                finally:
                    # Release lock
                    if lock_fd:
                        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                        lock_fd.close()
                        try:
                            lock_file.unlink()
                        except:
                            pass
            raise  # Re-raise if not FTS5-related

    try:
        if not DB_PATH.exists():
            logger.info("First run: Downloading Ciqual database...")
            print("First run: Downloading Ciqual database...", file=sys.stderr)
            initialize_database()
            logger.info("Database initialized successfully!")
            print("Database initialized successfully!", file=sys.stderr)
        elif should_update_database(DB_PATH):
            logger.info("New CIQUAL version available, updating database...")
            print("New CIQUAL version available, updating database...", file=sys.stderr)
            initialize_database()
            logger.info("Database updated successfully!")
            print("Database updated successfully!", file=sys.stderr)
        else:
            # Auto-repair FTS5 index if corrupted (e.g., from concurrent writes)
            repair_fts5_if_corrupted()
    except Exception as e:
        if not DB_PATH.exists():
            logger.error("Failed to initialize database: %s", e)
            print(f"Failed to initialize database: {e}", file=sys.stderr)
            sys.exit(1)
        else:
            logger.warning("Failed to update database, using existing version: %s", e)
            print(f"Failed to update database, using existing version: {e}", file=sys.stderr)

    logger.info("Starting Ciqual MCP server")
    print("Ciqual MCP server running", flush=True)
    try:
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Ciqual MCP server")
        print("\nShutting down Ciqual MCP server", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        logger.error("Server error: %s", e)
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
