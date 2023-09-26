## 1.0.6

- New `CommandLog` and `WithVariables`.
- `Command` extends `CommandLog`.
- `DBCommand`:
  - Added field `properties`.
- New `DBCommandSet`.
- `SQL`:
  - Added field `executedSQL`, with the actual executed SQL `String`.

- apollovm: ^0.0.38

## 1.0.5

- `SQL`:
  - Added `parseDateTime` and `formatDateTime`.
  - Automatically calls `initializeDateFormatting` if needed.

## 1.0.4

- New `SQLDialect`:
  - Support for bytes string.
- `SQL`:
  - `toJson/fromJson`: support for `DateTime` and `UInt8List` object encoding and decoding. 

## 1.0.3

- Added `parseSQLType`.
- `SQL.fromJson`: fix enum resolution.

## 1.0.2

- Improve documentation.
- Fix library name for `sql_commander_mysql.dart` and `sql_commander_postgres.dart`.

## 1.0.1

- Implemente SQL DELETE support.

## 1.0.0

- Initial version.
