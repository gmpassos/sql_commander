import 'package:sql_commander/sql_commander_postgres.dart';

//
// For MySQL support:
// import 'package:sql_commander/sql_commander_mysql.dart';
//

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
  // For MySQL:
  //DBConnectionMySQL.register();

  // Execute the SQL chain:
  var ok = await dbCommand.execute(
    logInfo: (m) => print('[INFO] $m'),
    logError: (m, [e, s]) => print('[ERROR] $m >> $e\n$s'),
  );

  print('SQL chain execution: $ok');
}
