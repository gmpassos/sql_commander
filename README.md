# sql_commander

[![pub package](https://img.shields.io/pub/v/sql_commander.svg?logo=dart&logoColor=00b9fc)](https://pub.dartlang.org/packages/sql_commander)
[![Null Safety](https://img.shields.io/badge/null-safety-brightgreen)](https://dart.dev/null-safety)
[![Codecov](https://img.shields.io/codecov/c/github/gmpassos/sql_commander)](https://app.codecov.io/gh/gmpassos/sql_commander)
[![Dart CI](https://github.com/gmpassos/sql_commander/actions/workflows/dart.yml/badge.svg?branch=master)](https://github.com/gmpassos/sql_commander/actions/workflows/dart.yml)
[![GitHub Tag](https://img.shields.io/github/v/tag/gmpassos/sql_commander?logo=git&logoColor=white)](https://github.com/gmpassos/sql_commander/releases)
[![New Commits](https://img.shields.io/github/commits-since/gmpassos/sql_commander/latest?logo=git&logoColor=white)](https://github.com/gmpassos/sql_commander/network)
[![Last Commits](https://img.shields.io/github/last-commit/gmpassos/sql_commander?logo=git&logoColor=white)](https://github.com/gmpassos/sql_commander/commits/master)
[![Pull Requests](https://img.shields.io/github/issues-pr/gmpassos/sql_commander?logo=github&logoColor=white)](https://github.com/gmpassos/sql_commander/pulls)
[![Code size](https://img.shields.io/github/languages/code-size/gmpassos/sql_commander?logo=github&logoColor=white)](https://github.com/gmpassos/sql_commander)
[![License](https://img.shields.io/github/license/gmpassos/sql_commander?logo=open-source-initiative&logoColor=green)](https://github.com/gmpassos/sql_commander/blob/master/LICENSE)

A SQL command chain handler and executor that is database-agnostic, with built-in support for MySQL and PostgreSQL.

## Motivation

The primary motivation behind creating this package was to facilitate the execution of SQL queries chain across various
flavors of databases in remote locations.

Rather than deploying software with hardcoded database operations in remote
devices/servers, opting for a robust and easily updatable solution across many devices, involves deploying a
database-agnostic SQL chain command executor (`sql_commander`). This executor is designed to receive `DBCommands` and
perform their execution remotely, adapting them to the specific database dialect in use while also resolving SQL chain
references and IDs.

If any operation requires modification or updating for system compatibility, the generated `DBCommand` sent to the
remote `sql_commander` can be adjusted without the necessity of updating the software deployed on the remote devices.
This simplifies maintenance and minimizes issues in remote locations.

Empowered by Dart's multi-platform support, this package simplifies the creation of robust a solution with ease.

## Usage

```dart
import 'package:sql_commander/sql_commander_postgres.dart';
//import 'package:sql_commander/sql_commander_mysql.dart';

void main() async {
  // DBCommand SQLs chain:
  var dbCommandSQLs = [
    // Provide the parameter %SYS_USER% in the INSERT below:
    SQL(
      '%SYS_USER%',
      'user',
      SQLType.SELECT,
      where: SQLConditionValue('id', '>', 0),
      returnColumns: {'user_id': 'id'},
      orderBy: '>user_id',
      limit: 1,
    ),
    // Provide the parameter %TAB_NUMBER% in the INSERT below:
    SQL(
      '%TAB_NUMBER%',
      'tab',
      SQLType.SELECT,

      where: SQLConditionGroup.and([
        SQLConditionValue('serie', '=', 'tabs'),
        SQLConditionGroup.or([
          SQLConditionValue('status', '=', 'free'),
          SQLConditionValue('status', '=', null),
        ])
      ]),
      returnColumns: {'num': null},
      // ORDER BY num DESC:
      orderBy: '>num',
      // LIMIT 1:
      limit: 1,
    ),
    // INSERT into table `order` using `%SYS_USER%` and `%TAB_NUMBER%` as parameters:
    SQL(
      // The ID of this SQL for references in the command chain: `#order:1001#`
      '1001',
      'order',
      SQLType.INSERT,
      parameters: {
        'product': 123,
        'price': 10.20,
        'title': 'Water',
        'user': '%SYS_USER%',
        'tab': '%TAB_NUMBER%',
      },
      // Variables to resolve in this SQL:
      variables: {'SYS_USER': null, 'TAB_NUMBER': null},
      returnLastID: true,
    ),
    // Another INSERT, using the INSERT above: `#order:1001#`
    SQL(
      // The ID of this SQL for references:
      '1',
      'order_history',
      SQLType.INSERT,
      parameters: {
        // The order inserted above:
        'order': '#order:1001#',
        'date': DateTime.now(),
      },
      returnLastID: true,
    ),
  ];

  // A `DBCommand` as JSON:
  var commandJSON = {
    "host": 'localhost',
    "port": 5432,
    "user": 'root',
    "pass": 'abc123',
    "db": 'dev',
    "software": 'postgres',
    "sqls": dbCommandSQLs.map((e) => e.toJson()).toList(),
  };

  // Load a `DBCommand` from JSON:
  var dbCommand = DBCommand.fromJson(commandJSON);

  // Register the `PostgreSQL` connection implementation:
  DBConnectionPostgres.register();

  // Execute the SQL chain:
  var ok = await dbCommand.execute(
    logInfo: (m) => print('[INFO] $m'),
    logError: (m, [e, s]) => print('[ERROR] $m >> $e\n$s'),
  );

  print('SQL chain execution: $ok');
}
```
## Features and bugs

Please file feature requests and bugs at the [issue tracker][tracker].

[tracker]: https://github.com/gmpassos/sql_commander/issues

## Author

Graciliano M. Passos: [gmpassos@GitHub][github].

[github]: https://github.com/gmpassos

## Sponsor

Don't be shy, show some love, and become our [GitHub Sponsor][github_sponsors].
Your support means the world to us, and it keeps the code caffeinated! ☕✨

Thanks a million! 🚀😄

[github_sponsors]: https://github.com/sponsors/gmpassos

## License

Dart free & open-source [license](https://github.com/dart-lang/stagehand/blob/master/LICENSE).
