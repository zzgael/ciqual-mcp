"""Unit tests for Ciqual MCP server"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from server import query
from data_loader import clean_text, parse_number, extract_unit

class TestCiqualServer(unittest.TestCase):
    
    @patch('server.DB_PATH')
    @patch('sqlite3.connect')
    async def test_query_success(self, mock_connect, mock_db_path):
        """Test successful SQL query execution"""
        # Setup
        mock_db_path.exists.return_value = True
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock row data
        mock_row = {'alim_code': 1234, 'alim_nom_eng': 'Apple, raw'}
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn.execute.return_value = mock_cursor
        mock_conn.row_factory = None
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = None
        
        # Execute
        result = await query("SELECT * FROM foods WHERE alim_code = 1234")
        
        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['alim_code'], 1234)
        mock_conn.execute.assert_called_once_with("SELECT * FROM foods WHERE alim_code = 1234")
    
    @patch('server.DB_PATH')
    async def test_query_no_database(self, mock_db_path):
        """Test query when database doesn't exist"""
        mock_db_path.exists.return_value = False
        
        result = await query("SELECT * FROM foods")
        
        self.assertEqual(len(result), 1)
        self.assertIn('error', result[0])
        self.assertIn('not initialized', result[0]['error'])
    
    @patch('server.DB_PATH')
    @patch('sqlite3.connect')
    async def test_query_read_only_protection(self, mock_connect, mock_db_path):
        """Test that write queries are blocked"""
        mock_db_path.exists.return_value = True
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = sqlite3.OperationalError("attempt to write a readonly database")
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = None
        
        result = await query("INSERT INTO foods VALUES (9999, 'Test', 'Test', '01')")
        
        self.assertEqual(len(result), 1)
        self.assertIn('error', result[0])
        self.assertIn('read-only', result[0]['error'])

class TestDataLoader(unittest.TestCase):
    
    def test_clean_text(self):
        """Test text cleaning function"""
        self.assertEqual(clean_text("  test  "), "test")
        self.assertEqual(clean_text(""), None)
        self.assertEqual(clean_text("missing"), None)
        self.assertEqual(clean_text(None), None)
    
    def test_parse_number(self):
        """Test number parsing with French format"""
        self.assertEqual(parse_number("12,5"), 12.5)
        self.assertEqual(parse_number("100"), 100.0)
        self.assertEqual(parse_number("-"), None)
        self.assertEqual(parse_number("traces"), None)
        self.assertEqual(parse_number(""), None)
    
    def test_extract_unit(self):
        """Test unit extraction from nutrient names"""
        self.assertEqual(extract_unit("Calcium (mg/100g)"), "mg/100g")
        self.assertEqual(extract_unit("Vitamin C (mg/100 g)"), "mg/100g")
        self.assertEqual(extract_unit("Energy"), None)
        self.assertEqual(extract_unit(None), None)

if __name__ == '__main__':
    # Import sqlite3 for the error type
    import sqlite3
    unittest.main()