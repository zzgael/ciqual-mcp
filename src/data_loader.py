"""Data loader for importing Ciqual XML data into SQLite

Handles downloading, parsing, and importing ANSES Ciqual data from XML files
into a local SQLite database with proper indexing and FTS support.
"""

import sqlite3
try:
    from lxml import etree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from pathlib import Path
import urllib.request
import zipfile
import io
import re
import tempfile
import shutil
from database import SCHEMA_SQL

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

def should_update_database(db_path):
    """Check if database needs updating
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        True if database needs update (older than 365 days or doesn't exist)
        
    Note:
        Uses 365-day cache since ANSES data hasn't been updated since 2020
    """
    if not db_path.exists():
        return True
    
    # Check last modified time
    import time
    db_age_days = (time.time() - db_path.stat().st_mtime) / (24 * 3600)
    return db_age_days > 365  # Update yearly (data hasn't changed since 2020)

def initialize_database(force_update=False):
    """Download and import Ciqual data into SQLite database
    
    Args:
        force_update: Force database update even if cache is valid
        
    Raises:
        Exception: If data download or import fails and no existing database
        
    Note:
        Creates database at ~/.ciqual/ciqual.db
        Downloads ~10MB of XML data from ANSES
        Results in ~10MB SQLite database with 3000+ foods
    """
    
    # Setup paths
    db_path = Path.home() / ".ciqual" / "ciqual.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if update is needed
    if not force_update and db_path.exists() and not should_update_database(db_path):
        print("Database is up to date (less than 30 days old)")
        return
    
    # Create temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        print("Downloading Ciqual data from ANSES...")
        
        # Download XML data directly from ANSES
        xml_url = "https://ciqual.anses.fr/cms/sites/default/files/inline-files/XML_2020_07_07.zip"
        
        zip_path = temp_path / "ciqual.zip"
        with urllib.request.urlopen(xml_url) as response:
            with open(zip_path, 'wb') as f:
                f.write(response.read())
        
        # Extract XML files
        print("Extracting data files...")
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(temp_path)
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Create schema
            print("Creating database schema...")
            conn.executescript(SCHEMA_SQL)
            
            # Load nutrients first
            print("Loading nutrients...")
            const_file = temp_path / "const_2020_07_07.xml"
            tree = ET.parse(const_file, parser=ET.XMLParser(encoding='windows-1252'))
            root = tree.getroot()
            
            for const in root.findall("CONST"):
                const_code = clean_text(const.find("const_code").text)
                const_nom_fr = const.find("const_nom_fr").text if const.find("const_nom_fr") is not None else None
                const_nom_eng = const.find("const_nom_eng").text if const.find("const_nom_eng") is not None else None
                
                if const_code:
                    unit = extract_unit(const_nom_fr) or extract_unit(const_nom_eng)
                    cursor.execute(
                        "INSERT OR REPLACE INTO nutrients (const_code, const_nom_fr, const_nom_eng, unit) VALUES (?, ?, ?, ?)",
                        (int(const_code), const_nom_fr, const_nom_eng, unit)
                    )
            
            # Load food groups
            print("Loading food groups...")
            grp_file = temp_path / "alim_grp_2020_07_07.xml"
            if grp_file.exists():
                tree = ET.parse(grp_file, parser=ET.XMLParser(encoding='windows-1252'))
                root = tree.getroot()
                
                for grp in root.findall("ALIM_GRP"):
                    grp_code = clean_text(grp.find("alim_grp_code").text) if grp.find("alim_grp_code") is not None else None
                    grp_nom_fr = grp.find("alim_grp_nom_fr").text if grp.find("alim_grp_nom_fr") is not None else None
                    grp_nom_eng = grp.find("alim_grp_nom_eng").text if grp.find("alim_grp_nom_eng") is not None else None
                    
                    if grp_code:
                        cursor.execute(
                            "INSERT OR REPLACE INTO food_groups (grp_code, grp_nom_fr, grp_nom_eng) VALUES (?, ?, ?)",
                            (grp_code, grp_nom_fr, grp_nom_eng)
                        )
            
            # Load foods
            print("Loading foods...")
            alim_file = temp_path / "alim_2020_07_07.xml"
            
            # Parse with recovery mode for broken XML
            try:
                # Try lxml with recovery mode
                from lxml import etree
                parser = etree.XMLParser(encoding='windows-1252', recover=True)
                tree = etree.parse(str(alim_file), parser)
                root = tree.getroot()
            except ImportError:
                # Fallback to standard parser with manual cleaning
                with open(alim_file, 'rb') as f:
                    content = f.read()
                
                # Decode and clean
                content = content.decode('windows-1252', errors='ignore')
                # Fix common XML issues
                content = content.replace('&', '&amp;').replace('<1°', '&lt;1°')
                import re
                # Remove control characters except tab, newline, carriage return
                content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
                
                root = ET.fromstring(content)
            
            food_count = 0
            for alim in root.findall("ALIM"):
                alim_code = clean_text(alim.find("alim_code").text) if alim.find("alim_code") is not None else None
                alim_nom_fr = alim.find("alim_nom_fr").text if alim.find("alim_nom_fr") is not None else None
                alim_nom_eng = alim.find("alim_nom_eng").text if alim.find("alim_nom_eng") is not None else None
                alim_grp_code = clean_text(alim.find("alim_grp_code").text) if alim.find("alim_grp_code") is not None else None
                
                if alim_code:
                    cursor.execute(
                        "INSERT OR REPLACE INTO foods (alim_code, alim_nom_fr, alim_nom_eng, alim_grp_code) VALUES (?, ?, ?, ?)",
                        (int(alim_code), alim_nom_fr, alim_nom_eng, alim_grp_code)
                    )
                    food_count += 1
            
            print(f"Loaded {food_count} foods")
            
            # Load composition data
            print("Loading nutritional composition data (this may take a minute)...")
            compo_file = temp_path / "compo_2020_07_07.xml"
            
            # Parse with recovery mode for broken XML
            try:
                # Try lxml with recovery mode
                from lxml import etree
                parser = etree.XMLParser(encoding='windows-1252', recover=True)
                tree = etree.parse(str(compo_file), parser)
                root = tree.getroot()
            except ImportError:
                # Fallback to standard parser with manual cleaning
                with open(compo_file, 'rb') as f:
                    content = f.read()
                
                # Decode and clean
                content = content.decode('windows-1252', errors='ignore')
                import re
                # Remove control characters except tab, newline, carriage return
                content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
                
                root = ET.fromstring(content)
            
            compo_count = 0
            batch = []
            for compo in root.findall("COMPO"):
                alim_code = clean_text(compo.find("alim_code").text) if compo.find("alim_code") is not None else None
                const_code = clean_text(compo.find("const_code").text) if compo.find("const_code") is not None else None
                teneur = compo.find("teneur").text if compo.find("teneur") is not None else None
                code_confiance = clean_text(compo.find("code_confiance").text) if compo.find("code_confiance") is not None else None
                
                if alim_code and const_code:
                    teneur_value = parse_number(teneur)
                    if teneur_value is not None:
                        batch.append((int(alim_code), int(const_code), teneur_value, code_confiance))
                        
                        if len(batch) >= 1000:
                            cursor.executemany(
                                "INSERT OR REPLACE INTO composition (alim_code, const_code, teneur, code_confiance) VALUES (?, ?, ?, ?)",
                                batch
                            )
                            compo_count += len(batch)
                            batch = []
            
            # Insert remaining batch
            if batch:
                cursor.executemany(
                    "INSERT OR REPLACE INTO composition (alim_code, const_code, teneur, code_confiance) VALUES (?, ?, ?, ?)",
                    batch
                )
                compo_count += len(batch)
            
            print(f"Loaded {compo_count} nutritional values")
            
            # Populate FTS table
            print("Building full-text search index...")
            try:
                conn.execute("DELETE FROM foods_fts")
            except:
                pass  # Table might not exist or be empty
            conn.execute("INSERT INTO foods_fts SELECT alim_code, alim_nom_fr, alim_nom_eng FROM foods")
            
            # Commit and optimize
            conn.commit()
            
            # Verify data was loaded
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