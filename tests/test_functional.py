"""Functional tests for Ciqual MCP server with real database"""

import unittest
import tempfile
import shutil
import sqlite3
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import SCHEMA_SQL
from server import query
import server

class TestCiqualFunctional(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Create a test database with sample data"""
        cls.test_dir = tempfile.mkdtemp()
        cls.test_db = Path(cls.test_dir) / "test.db"
        
        # Create database with schema
        conn = sqlite3.connect(cls.test_db)
        conn.executescript(SCHEMA_SQL)
        
        # Insert test data
        conn.execute("""
            INSERT INTO nutrients (const_code, const_nom_fr, const_nom_eng, unit)
            VALUES 
            (328, 'Energie', 'Energy', 'kcal/100g'),
            (25000, 'Protéines', 'Protein', 'g/100g'),
            (55400, 'Vitamine C', 'Vitamin C', 'mg/100g')
        """)
        
        conn.execute("""
            INSERT INTO foods (alim_code, alim_nom_fr, alim_nom_eng, alim_grp_code)
            VALUES
            (2028, 'Orange, pulpe, crue', 'Orange, raw', '13'),
            (2003, 'Pomme, pulpe, crue', 'Apple, raw', '13'),
            (3001, 'Boeuf, steak haché 15% MG, cru', 'Beef, minced, 15% fat, raw', '08')
        """)
        
        conn.execute("""
            INSERT INTO composition (alim_code, const_code, teneur, code_confiance)
            VALUES
            (2028, 328, 47, 'A'),
            (2028, 25000, 0.9, 'A'),
            (2028, 55400, 53.2, 'A'),
            (2003, 328, 52, 'A'),
            (2003, 25000, 0.3, 'A'),
            (2003, 55400, 4.6, 'B'),
            (3001, 328, 198, 'A'),
            (3001, 25000, 18.6, 'A')
        """)
        
        # Build FTS index
        conn.execute("INSERT INTO foods_fts SELECT alim_code, alim_nom_fr, alim_nom_eng FROM foods")
        
        conn.commit()
        conn.close()
        
        # Patch the DB_PATH in the server module
        cls.original_db_path = server.DB_PATH
        server.DB_PATH = cls.test_db
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database"""
        server.DB_PATH = cls.original_db_path
        shutil.rmtree(cls.test_dir)
    
    async def test_search_foods_by_name(self):
        """Test searching foods by name"""
        result = await query("SELECT * FROM foods WHERE alim_nom_eng LIKE '%orange%'")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['alim_code'], 2028)
        self.assertEqual(result[0]['alim_nom_eng'], 'Orange, raw')
    
    async def test_fuzzy_search(self):
        """Test fuzzy search using FTS"""
        result = await query("SELECT * FROM foods_fts WHERE foods_fts MATCH 'orang*'")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['alim_code'], 2028)
    
    async def test_get_nutrient_value(self):
        """Test getting specific nutrient value for a food"""
        result = await query("""
            SELECT f.alim_nom_eng, c.teneur as vitamin_c_mg
            FROM foods f
            JOIN composition c ON f.alim_code = c.alim_code
            WHERE f.alim_code = 2028 AND c.const_code = 55400
        """)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['alim_nom_eng'], 'Orange, raw')
        self.assertEqual(result[0]['vitamin_c_mg'], 53.2)
    
    async def test_compare_foods(self):
        """Test comparing nutritional values between foods"""
        result = await query("""
            SELECT f.alim_nom_eng, c.teneur as calories
            FROM foods f
            JOIN composition c ON f.alim_code = c.alim_code
            WHERE c.const_code = 328
            ORDER BY c.teneur DESC
        """)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['alim_nom_eng'], 'Beef, minced, 15% fat, raw')
        self.assertEqual(result[0]['calories'], 198)
        self.assertEqual(result[2]['alim_nom_eng'], 'Orange, raw')
        self.assertEqual(result[2]['calories'], 47)
    
    async def test_find_high_protein_foods(self):
        """Test finding foods high in protein"""
        result = await query("""
            SELECT f.alim_nom_eng, c.teneur as protein_g
            FROM foods f
            JOIN composition c ON f.alim_code = c.alim_code
            WHERE c.const_code = 25000
            ORDER BY c.teneur DESC
            LIMIT 1
        """)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['alim_nom_eng'], 'Beef, minced, 15% fat, raw')
        self.assertEqual(result[0]['protein_g'], 18.6)
    
    async def test_complex_query_with_aggregation(self):
        """Test complex query with aggregation"""
        result = await query("""
            SELECT 
                f.alim_nom_eng,
                COUNT(c.const_code) as nutrient_count,
                AVG(c.teneur) as avg_value
            FROM foods f
            JOIN composition c ON f.alim_code = c.alim_code
            GROUP BY f.alim_code, f.alim_nom_eng
            HAVING nutrient_count > 1
            ORDER BY nutrient_count DESC
        """)
        
        self.assertEqual(len(result), 3)
        # Orange has 3 nutrients
        self.assertEqual(result[0]['nutrient_count'], 3)
    
    async def test_write_protection(self):
        """Test that write operations are blocked"""
        result = await query("DELETE FROM foods WHERE alim_code = 2028")
        
        self.assertEqual(len(result), 1)
        self.assertIn('error', result[0])
        
        # Verify data wasn't deleted
        result = await query("SELECT COUNT(*) as count FROM foods")
        self.assertEqual(result[0]['count'], 3)

if __name__ == '__main__':
    import asyncio
    
    # Run async tests
    def async_test(coro):
        def wrapper(*args, **kwargs):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(coro(*args, **kwargs))
        return wrapper
    
    # Patch all async test methods
    for attr_name in dir(TestCiqualFunctional):
        if attr_name.startswith('test_'):
            attr = getattr(TestCiqualFunctional, attr_name)
            if asyncio.iscoroutinefunction(attr):
                setattr(TestCiqualFunctional, attr_name, async_test(attr))
    
    unittest.main()