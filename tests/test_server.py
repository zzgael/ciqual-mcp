"""Unit tests for Ciqual MCP server"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from server import query
from data_loader import clean_text, parse_number, extract_unit, _find_file, _get_element_text

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

    def test_find_file(self):
        """Test Zenodo file matching by prefix"""
        files = [
            {"name": "alim_2024_07_01.xml", "download_url": "https://example.com/alim"},
            {"name": "alim_grp_2024_07_01.xml", "download_url": "https://example.com/alim_grp"},
            {"name": "const_2024_07_01.xml", "download_url": "https://example.com/const"},
            {"name": "compo_2024_07_01.xml", "download_url": "https://example.com/compo"},
            {"name": "sources_2024_07_01.xml", "download_url": "https://example.com/sources"},
        ]

        # alim_ should NOT match alim_grp_
        result = _find_file(files, "alim_")
        self.assertEqual(result["name"], "alim_2024_07_01.xml")

        result = _find_file(files, "alim_grp_")
        self.assertEqual(result["name"], "alim_grp_2024_07_01.xml")

        result = _find_file(files, "const_")
        self.assertEqual(result["name"], "const_2024_07_01.xml")

        result = _find_file(files, "sources_")
        self.assertEqual(result["name"], "sources_2024_07_01.xml")

        result = _find_file(files, "nonexistent_")
        self.assertIsNone(result)

    def test_get_element_text(self):
        """Test XML element text extraction"""
        import xml.etree.ElementTree as ET
        root = ET.fromstring("<CONST><const_code>400</const_code><code_INFOODS>ENER</code_INFOODS></CONST>")
        self.assertEqual(_get_element_text(root, "const_code"), "400")
        self.assertEqual(_get_element_text(root, "code_INFOODS"), "ENER")
        self.assertIsNone(_get_element_text(root, "missing_tag"))

    def test_parse_number_new_fields(self):
        """Test number parsing for min/max/source_code values"""
        self.assertEqual(parse_number("0,5"), 0.5)
        self.assertEqual(parse_number("150"), 150.0)
        self.assertIsNone(parse_number(None))
        self.assertIsNone(parse_number("  "))

    @patch('data_loader._fetch_zenodo_metadata')
    @patch('data_loader._download_xml')
    @patch('sqlite3.connect')
    def test_initialize_database_calls_zenodo(self, mock_connect, mock_download, mock_fetch):
        """Test that initialize_database uses Zenodo API"""
        from data_loader import initialize_database

        mock_fetch.return_value = {
            "record_id": "123456",
            "version": "2024",
            "files": [
                {"name": "alim_2024.xml", "download_url": "https://zenodo.org/alim"},
                {"name": "const_2024.xml", "download_url": "https://zenodo.org/const"},
                {"name": "compo_2024.xml", "download_url": "https://zenodo.org/compo"},
                {"name": "alim_grp_2024.xml", "download_url": "https://zenodo.org/grp"},
                {"name": "sources_2024.xml", "download_url": "https://zenodo.org/sources"},
            ],
        }

        # Create mock XML roots for each file type
        import xml.etree.ElementTree as ET

        def make_xml_root(tag, children_xml):
            return ET.fromstring(f"<ROOT>{children_xml}</ROOT>")

        nutrients_root = make_xml_root("ROOT", "<CONST><const_code>328</const_code><const_nom_fr>Energie (kcal/100g)</const_nom_fr><code_INFOODS>ENER</code_INFOODS></CONST>")
        groups_root = make_xml_root("ROOT", "<ALIM_GRP><alim_grp_code>01</alim_grp_code><alim_grp_nom_fr>Entrées</alim_grp_nom_fr></ALIM_GRP>")
        sources_root = make_xml_root("ROOT", "<SOURCE><source_code>1</source_code><ref_citation>Test ref</ref_citation></SOURCE>")
        foods_root = make_xml_root("ROOT", "<ALIM><alim_code>1001</alim_code><alim_nom_fr>Pomme</alim_nom_fr><alim_grp_code>01</alim_grp_code></ALIM>")
        compo_root = make_xml_root("ROOT", "<COMPO><alim_code>1001</alim_code><const_code>328</const_code><teneur>52</teneur><source_code>1</source_code></COMPO>")

        mock_download.side_effect = [nutrients_root, groups_root, sources_root, foods_root, compo_root]

        # Mock DB connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.execute.return_value = MagicMock(fetchone=lambda: (1,))
        mock_connect.return_value = mock_conn

        # Run with force_update to skip cache check
        initialize_database(force_update=True)

        # Verify Zenodo API was called
        mock_fetch.assert_called_once()
        # Verify XMLs were downloaded (5 files)
        self.assertEqual(mock_download.call_count, 5)

if __name__ == '__main__':
    # Import sqlite3 for the error type
    import sqlite3
    unittest.main()
