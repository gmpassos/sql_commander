import 'dart:async';
import 'dart:typed_data';

import 'package:sql_commander/sql_commander.dart';
import 'package:test/test.dart';

void main() {
  _MyDBConnection.register();

  group('ProcedureJava', () {
    test('basic', () async {
      var code = r'''
        class Foo {
      
          int sumAB(int a, int b) {
            var r = a + b;
            return r;
          }
        
        }
      ''';

      var f0 = ProcedureJava(
          name: 'sumAB', code: code, positionalParameters: [1000, 200]);

      var fJson = f0.toJson();

      print(fJson);

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = Procedure.fromJson(fJson)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();

        expect(r, 1200);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }

      {
        var r = await f.call(positionalParameters: [10, 20]);

        expect(r, 30);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }

      {
        var r = await f.call(positionalParameters: [100, 200]);
        expect(r, 300);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }
    });
  });

  group('ProcedureDart', () {
    test('basic', () async {
      var code = r'''
      
        int sumAB(int a, int b) {
          var r = a + b;
          return r;
        }
      
      ''';

      var f0 = ProcedureDart(
          name: 'sumAB', code: code, positionalParameters: [1000, 200]);

      var fJson = f0.toJson();

      print(fJson);

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = Procedure.fromJson(fJson)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();

        expect(r, 1200);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }

      {
        var r = await f.call(positionalParameters: [10, 20]);

        expect(r, 30);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }

      {
        var r = await f.call(positionalParameters: [100, 200]);
        expect(r, 300);
        expect(prints, equals([]));
        expect(loggedInfos, equals([]));
        expect(loggedErrors, equals([]));
      }
    });

    test("executeDBCommandByID (can't find DBCommand)", () async {
      var code = r'''
      
        bool do() {
          var cmdOK = executeDBCommandByID("cmd_1");
          
          if (cmdOK == false) {
            print('** Error executing DBCommand!');
            return false;
          }
          
          return true ;
        }
      
      ''';

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = ProcedureDart(name: 'do', code: code)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();
        expect(r, false);

        expect(prints, equals(['** Error executing DBCommand!']));
        expect(loggedInfos, equals(['Can\'t find `DBCommand`: cmd_1']));
        expect(loggedErrors, equals([]));
      }
    });

    test("executeDBCommandByID (can't find DBCommand)", () async {
      var code = r'''
      
        bool do() {
          var cmdOK = executeDBCommandByID("cmd_1");
          
          if (!cmdOK) {
            print('** Error executing DBCommand!');
            return false;
          }
          
          return true ;
        }
      
      ''';

      var f0 = ProcedureDart(name: 'do', code: code);

      var json = f0.toJson();

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = ProcedureDart.fromJson(json)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();
        expect(r, false);

        expect(prints, equals(['** Error executing DBCommand!']));
        expect(loggedInfos, equals(['Can\'t find `DBCommand`: cmd_1']));
        expect(loggedErrors, equals([]));
      }
    });

    test("executeDBCommandByID", () async {
      var dbPort = 3317;

      var sqls = [
        SQL(
          '%SYS_USER%',
          'user',
          SQLType.SELECT,
          where: SQLConditionValue('id', '>', 0),
          returnColumns: {'user_id': 'id'},
          orderBy: '>user_id',
          limit: 1,
        ),
        SQL(
          'sel_tab',
          'tab',
          SQLType.SELECT,
          where: SQLConditionGroup.and([
            SQLConditionValue('user', '==', '%SYS_USER%'),
            SQLConditionValue('tab_id', '>', '%MIN_TABLE_ID%'),
          ]),
          returnColumns: {'tab_id': 'id'},
          orderBy: '>tab_id',
          limit: 1,
        ),
      ];

      var dbCommands = [
        DBCommand(
            id: 'cmd_1',
            'localhost',
            dbPort,
            'root',
            'abc123',
            'pub',
            'any_db',
            sqls)
      ];

      var code = r'''
      
        class MyProcedure {
        
          bool do() {
            var cmdOK = executeDBCommandByID("cmd_1");
            
            if (!cmdOK) {
              print('** Error executing DBCommand!');
              return false;
            }
            
            print('DBCommand `cmd_1` executed.');
            
            var p1 = getProperty('p1');
            print('Property `p1`: $p1');
            
            var sqlResult = getSQLResult('sel_tab');
            print('sqlResult: $sqlResult');
            
            var sqlResultID = getSQLResultColumn('sel_tab','id');
            print('sqlResultID: $sqlResultID');
            
            return true ;
          }
        
        }
      
      ''';

      var f0 = ProcedureDart(
          className: 'MyProcedure',
          name: 'do',
          code: code,
          properties: {'p1': 'abc123', 'MIN_TABLE_ID': 10},
          dbCommands: dbCommands);

      var json = f0.toJson();

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = ProcedureDart.fromJson(json)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();
        expect(r, true);

        expect(
            prints,
            equals(
              [
                'DBCommand `cmd_1` executed.',
                'Property `p1`: abc123',
                'sqlResult: {id: 301}',
                'sqlResultID: 301'
              ],
            ));

        expect(
            loggedInfos,
            equals([
              'Started transaction',
              'Executed SQL for variable `SYS_USER`: SQL[%SYS_USER%]{type: SELECT, table: user, where: SQLConditionValue{id > 0}, returnLastID: false, orderBy: >user_id, executed: true, [{id: u10}]}<SELECT `user_id` as `id` FROM `user` WHERE `id` > 0 ORDER BY `user_id` DESC LIMIT 1>',
              'SQL executed: SQL[sel_tab]{type: SELECT, table: tab, where: SQLConditionGroup{or: false, conditions: [SQLConditionValue{user == %SYS_USER%}, SQLConditionValue{tab_id > %MIN_TABLE_ID%}]}, variables: {SYS_USER: u10, MIN_TABLE_ID: 10}, returnLastID: false, orderBy: >tab_id, executed: true, [{id: 301}]}<SELECT `tab_id` as `id` FROM `tab` WHERE ( `user` == \'u10\' AND `tab_id` > 10 ) ORDER BY `tab_id` DESC LIMIT 1>',
              'Commit transaction: OK'
            ]));

        expect(loggedErrors, equals([]));
      }
    });

    test("executeSQLByID (can't find SQL)", () async {
      var code = r'''
      
        bool do() {
          var sqlOK = executeSQLByID("sql_1");
          
          if (!sqlOK) {
            print('** Error executing SQL!');
            return false;
          }
          
          return true ;
        }
      
      ''';

      var f0 = ProcedureDart(name: 'do', code: code);

      var fJson = f0.toJson();

      print(fJson);

      var prints = [];
      var loggedInfos = [];
      var loggedErrors = [];

      var f = Procedure.fromJson(fJson)
        ..printFunction = ((o) => prints.add(o))
        ..logInfo = ((m) => loggedInfos.add(m))
        ..logError = ((m, [e, s]) => loggedErrors.add([m, e, s]));

      {
        var r = await f.execute();

        expect(r, false);
        expect(prints, equals(['** Error executing SQL!']));
        expect(loggedInfos, equals(["Can't find SQL: sql_1"]));
        expect(loggedErrors, equals([]));
      }
    });
  });
}

class _MyDialect extends SQLDialect {
  _MyDialect() : super('generic', q: '`');

  @override
  String toBytesString(Uint8List bytes) {
    var hex = SQLDialect.toHex(bytes);
    return "'\\x$hex'";
  }
}

class _MyDBConnection extends DBConnection<int> {
  static void register() =>
      DBConnectionProvider.registerProvider('any_db', _MyDBConnection.provider);

  static DBConnectionPoolProvider<_MyDBConnection> provider(
          DBConnectionCredential credential,
          {int? maxConnections,
          Duration? retryInterval,
          int? maxRetries}) =>
      DBConnectionPoolProvider<_MyDBConnection>(
        credential,
        open,
        maxConnections: maxConnections ?? 1,
        retryInterval: retryInterval ?? Duration(seconds: 1),
        maxRetries: maxRetries ?? 10,
      );

  /// Opens a [DBConnectionMySQL].
  static FutureOr<_MyDBConnection?> open(DBConnectionCredential credential,
          {Duration? retryInterval, int? maxRetries}) =>
      _MyDBConnection();

  static int _connectionIdCount = 0;

  _MyDBConnection() : super(++_connectionIdCount, _MyDialect());

  final List<(String, List?, Map?)> executedSQLs = [];

  int insertCount = 100;

  @override
  Future<({List<Map<String, dynamic>>? results, Object? lastID})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls}) async {
    var s = sql.build(dialect: dialect, executedSqls: executedSqls);

    sql.executedSQL = s.sql;

    List<Map<String, dynamic>>? results;
    Object? lastID;

    switch (sql.type) {
      case SQLType.INSERT:
        {
          if (sql.table == 'order_ref') {
            if (sql.returnLastID) {
              lastID = sql.resolveLastInsertID(0,
                  valuesNamed: s.valuesNamed, executedSqls: executedSqls);
            }

            results = [];
          } else {
            var id = ++insertCount;

            if (sql.returnLastID) {
              lastID = id;
            }

            results = [];
          }
        }
      case SQLType.UPDATE:
        {
          results = [];
        }
      case SQLType.SELECT:
        {
          if (sql.table == 'user') {
            results = [
              {'id': 'u10'}
            ];
          } else if (sql.table == 'tab') {
            results = [
              {'id': 301}
            ];
          }
        }
      case SQLType.DELETE:
        {
          if (sql.table == 'tab_use') {
            results = [];
          }
        }
      default:
        throw StateError("Can't execute SQL: $sql");
    }

    executedSQLs.add((s.sql, s.valuesOrdered, s.valuesNamed));

    return (results: results, lastID: lastID);
  }

  @override
  Future<bool> commitTransaction() async => true;

  @override
  Future<bool> rollbackTransaction() async => true;

  @override
  Future<bool> startTransaction() async => true;

  @override
  Future<void> close() async {}
}
