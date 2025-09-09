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
    """Execute SQL query on ANSES Ciqual French food composition database.
    
    SCHEMA:
    • foods: thousands of foods with French/English names
      - alim_code (PK), alim_nom_fr, alim_nom_eng, alim_grp_code
    
    • nutrients: ~60+ nutrients with units
      - const_code (PK), const_nom_fr, const_nom_eng, unit
    
    • composition: nutritional values per 100g
      - alim_code, const_code, teneur (value), code_confiance (A/B/C/D)
    
    • foods_fts: full-text search for fuzzy matching
      - Use: WHERE foods_fts MATCH 'search term'
    
    KEY NUTRIENT CODES:
    Energy: 327 (kJ), 328 (kcal)
    Macros: 25000 (protein g), 31000 (carbs g), 40000 (fat g), 34100 (fiber g), 32000 (sugars g)
    Minerals: 10110 (sodium mg), 10200 (calcium mg), 10260 (iron mg), 10190 (potassium mg), 10120 (magnesium mg), 10300 (zinc mg)
    Vitamins: 55400 (vit C mg), 56400 (vit D µg), 51330 (vit B12 µg), 56310 (vit E mg), 56700 (vit K µg), 51200 (vit B6 mg)
    Lipids: 40302 (saturated g), 40303 (monounsaturated g), 40304 (polyunsaturated g), 40400 (cholesterol mg)
    Sugars: 18601 (lactose g), 18604 (sucrose g), 18605 (glucose g), 18606 (fructose g)
    
    The database is read-only. Use SELECT queries only.
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
            return [{"error": f"Table not found. Available tables: foods, nutrients, composition, foods_fts, food_groups"}]
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
    # Check and update database if needed (auto-updates yearly)
    from .data_loader import initialize_database, should_update_database
    
    try:
        if not DB_PATH.exists():
            logger.info("First run: Downloading Ciqual database...")
            print("First run: Downloading Ciqual database...", file=sys.stderr)
            initialize_database()
            logger.info("Database initialized successfully!")
            print("Database initialized successfully!", file=sys.stderr)
        elif should_update_database(DB_PATH):
            logger.info("Database is outdated, updating from ANSES...")
            print("Database is outdated, updating from ANSES...", file=sys.stderr)
            initialize_database(force_update=True)
            logger.info("Database updated successfully!")
            print("Database updated successfully!", file=sys.stderr)
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