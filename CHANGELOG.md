# ByteHouse JDBC Driver Change Log
All notable changes to this project will be documented in this file.

## [Unreleased] - yyyy-mm-dd

### Added

### Changed

### Fixed

## [1.1.20] - 2022-06-22

### Added
- Support custom query ID
- Support local infile insertion

### Changed

### Fixed
- Fix index out of bound issue affecting LowCardinality data

## [1.1.12] - 2022-11-24

### Added
- Add settings dict_table_full_mode
- Enable generic settings in query

### Changed

### Fixed

## [1.1.10] - 2022-10-17

### Added
- Support API Key authentication
- Support Show ByteHouse table keys
- Add virtual warehouse name settings

### Changed
- Upgrade JWT version to v3.19.2
- Refactor serializable character set
- Update Volcano region name to CN-BEIJING
- Update License header to include claims

### Fixed
- Fix Datatype serialization issues
- Fix UInt64 lexer for parsing
- Fix insertion query for Nullable array datatype

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
