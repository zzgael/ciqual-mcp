#!/usr/bin/env python3
"""ANSES Ciqual MCP Server with SQL query interface"""

from fastmcp import FastMCP
import sqlite3
import os
from pathlib import Path
import sys

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
        return [{"error": "Database not initialized. Please run the server first to download data."}]
    
    # Connect with read-only mode
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        
        # Execute query
        cursor = conn.execute(sql)
        results = [dict(row) for row in cursor.fetchall()]
        return results
        
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return [{"error": f"Table not found. Available tables: foods, nutrients, composition, foods_fts, food_groups"}]
        elif "read-only" in str(e):
            return [{"error": "Database is read-only. Only SELECT queries are allowed."}]
        else:
            return [{"error": f"SQL error: {str(e)}"}]
    except Exception as e:
        return [{"error": f"Error: {str(e)}"}]
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Main entry point"""
    # Check and update database if needed (auto-updates monthly)
    from .data_loader import initialize_database, should_update_database
    
    try:
        if not DB_PATH.exists():
            print("First run: Downloading Ciqual database...", file=sys.stderr)
            initialize_database()
            print("Database initialized successfully!", file=sys.stderr)
        elif should_update_database(DB_PATH):
            print("Database is outdated, updating from ANSES...", file=sys.stderr)
            initialize_database(force_update=True)
            print("Database updated successfully!", file=sys.stderr)
    except Exception as e:
        if not DB_PATH.exists():
            print(f"Failed to initialize database: {e}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Failed to update database, using existing version: {e}", file=sys.stderr)
    
    print("Ciqual MCP server running", flush=True)
    mcp.run()

if __name__ == "__main__":
    main()