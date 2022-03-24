# ByteHouse JDBC Driver Change Log
All notable changes to this project will be documented in this file.

## [Unreleased] - yyyy-mm-dd

### Added

### Changed

### Fixed

## [1.1.1] - 2022-03-24

### Changed
- Enforce TLS v1.2 to be compatible with JDK 8

### Fixed
- Fix DataType LowCardinality throw exception for batch insertion
- Fix PreparedStatement throw exception when placeholders & values mixed

## [1.1.0] - 2022-01-28

### Added
- Add custom SQL expression parser for Tableau connector

### Fixed
- Fix getDate() with Calendar and getTimestamp() with Calendar
- Fix getResultSet() returning null before executing query

## [1.0.0] - 2022-01-17

### Added
- Initial release of ByteHouse JDBC Driver
