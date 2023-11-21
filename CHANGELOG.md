
# ByteHouse JDBC Driver Change Log
All notable changes to this project will be documented in this file.

## [Unreleased] - yyyy-mm-dd

### Added

### Changed

### Fixed

## [1.1.31] - 2023-11-22

### Added
- Support prepareStatement(String sqlString, int resultSetType, int resultSetConcurrency)

## [1.1.27] - 2023-09-20

### Added
- Supported uint128 and uint256 data types

### Changed

### Fixed

## [1.1.26] - 2023-09-15

### Added
- Supported null value insertion to non-nullable column, defaulting to zero-value

### Changed

### Fixed

## [1.1.24] - 2023-08-22

### Added
- Added Taco file for a tableau connector

### Changed

### Fixed

## [1.1.23] - 2023-07-18

### Added

### Changed

### Fixed
- Fix JDBC Url passing to not replace database if empty


## [1.1.22] - 2022-07-17

### Added
- Add region enum for AWS-US-EAST

### Changed

### Fixed
- Fix getPrimaryKeys order type to be short
- Fix DateTime64 enforcing space on parsing

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
