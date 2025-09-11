# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2025-09-11

### Fixed
- Fixed relative import error in data_loader.py that prevented the package from running

## [0.1.1] - 2025-09-10

### Changed
- Updated minimum Python version to 3.10 (required by fastmcp dependency)
- Removed Python 3.9 from test matrix

## [0.1.0] - 2025-09-10

### Added
- Initial release
- SQL query interface for ANSES Ciqual database
- Automatic database initialization and updates
- Full-text search support with fuzzy matching
- Comprehensive test suite (unit and functional)
- Support for 3,185+ foods and 67 nutrients
- Bilingual support (French and English)
- Read-only database access for safety
- Auto-update mechanism (yearly checks)
- MCP server implementation with FastMCP