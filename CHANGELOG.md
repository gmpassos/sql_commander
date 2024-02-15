## 1.1.0

- sdk: '>=3.3.0 <4.0.0'
- swiss_knife: ^3.1.6
- apollovm: ^0.0.50
- intl: ^0.19.0
- postgres: ^2.6.3

- lints: ^3.0.0
- dependency_validator: ^3.2.3
- test: ^1.25.2
- coverage: ^1.7.2

## 1.0.9

- apollovm: ^0.0.49

## 1.0.8

- apollovm: ^0.0.45

## 1.0.7

- apollovm: ^0.0.43

## 1.0.6

- New `CommandLog` and `WithVariables`.
- `Command` extends `CommandLog`.
- `DBCommand`:
  - Added field `properties`.
- New `DBCommandSet`.
- `SQL`:
  - Added field `executedSQL`, with the actual executed SQL `String`.
- New `Procedure`: allows execution of dynamic Dart code loaded by `ApolloVM`.

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
