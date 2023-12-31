import 'dart:typed_data';

import 'package:sql_commander/sql_commander.dart';
import 'package:test/test.dart';

void main() {
  group('DBCommand', () {
    test('basic', () async {
      var dbPort = 3316;

      var commandSQLs = [
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
          orderBy: '>num',
          limit: 1,
        ),
        SQL(
          '11',
          'order',
          SQLType.INSERT,
          parameters: {
            'product': 123,
            'price': 10.20,
            'title': 'Water',
            'user': '%SYS_USER%',
            'tab': '%TAB_NUMBER%',
          },
          variables: {'SYS_USER': null, 'TAB_NUMBER': null},
          returnLastID: true,
        ),
        SQL(
          '12',
          'product',
          SQLType.UPDATE,
          parameters: {
            'last_date': DateTime.utc(2020, 10, 11),
            'count': ['count + 1']
          },
          where: SQLConditionGroup.and([
            SQLConditionValue('id', '=', 123),
            SQLConditionValue('type', '!=', 'x'),
          ]),
        ),
        SQL(
          '13',
          'order_ref',
          SQLType.INSERT,
          parameters: {
            'order': '#order:11#',
            "next_order": ['#order:11# + 10'],
            'ref': 1002
          },
          returnColumns: {'next_order': null},
          returnLastID: true,
        ),
        SQL(
          '14',
          'order_ref',
          SQLType.UPDATE,
          parameters: {
            'last_date': DateTime.utc(2020, 10, 11, 1, 2, 3),
            'qr': Uint8List.fromList([1, 2, 3, 4])
          },
          where: SQLConditionValue('id', '=', '#order_ref:13#'),
        ),
        SQL(
          '15',
          'tab_use',
          SQLType.DELETE,
          parameters: {'last_date': DateTime.utc(2020, 10, 11, 1, 2, 3)},
          where: SQLConditionValue('num', '=', '%TAB_NUMBER%'),
          variables: {'TAB_NUMBER': null},
        ),
      ];

      var json = {
        "ip": 'localhost',
        "port": dbPort,
        "user": 'root',
        "pass": 'abc123',
        "db": 'pub',
        "software": 'any_db',
        "sqls": commandSQLs.map((e) => e.toJson()).toList(),
      };

      var dbCommand = DBCommand.fromJson(json);

      expect(dbCommand.commandType, equals('DB'));
      expect(dbCommand.software, equals('any_db'));
      expect(dbCommand.host, equals('localhost'));
      expect(dbCommand.port, equals(dbPort));
      expect(dbCommand.sqls.length, equals(7));

      expect(dbCommand.sqls[0],
          isA<SQL>().having((e) => e.type, 'type', equals(SQLType.SELECT)));
      expect(dbCommand.sqls[2],
          isA<SQL>().having((e) => e.type, 'type', equals(SQLType.INSERT)));
      expect(dbCommand.sqls[3],
          isA<SQL>().having((e) => e.type, 'type', equals(SQLType.UPDATE)));
      expect(dbCommand.sqls[4],
          isA<SQL>().having((e) => e.type.name, 'type', equals('INSERT')));
      expect(dbCommand.sqls[5],
          isA<SQL>().having((e) => e.type.name, 'type', equals('UPDATE')));
      expect(dbCommand.sqls[6],
          isA<SQL>().having((e) => e.type.name, 'type', equals('DELETE')));

      expect(
          dbCommand.sqls[5].parameters,
          equals({
            'last_date': DateTime.utc(2020, 10, 11, 1, 2, 3),
            'qr': Uint8List.fromList([1, 2, 3, 4])
          }));

      var myDBConnection = _MyDBConnection();

      var loggedInfos = [];
      var loggedErrors = [];

      myLogInfo(m) => loggedInfos.add(m);
      myLogError(m, [e, s]) => loggedErrors.add([m, e, s]);

      dbCommand.logInfo = myLogInfo;
      dbCommand.logError = myLogError;

      var connectionProvider = DBSingleConnectionProvider(myDBConnection);
      var db = DB(connectionProvider);

      var sqlOK = await dbCommand.execute(prepared: db);

      expect(sqlOK, isTrue);

      expect(
          loggedInfos,
          equals([
            'Started transaction',
            'Executed SQL for variable `SYS_USER`: SQL[%SYS_USER%]{type: SELECT, table: user, where: SQLConditionValue{id > 0}, returnLastID: false, orderBy: >user_id, executed: true, [{id: u10}]}<SELECT `user_id` as `id` FROM `user` WHERE `id` > 0 ORDER BY `user_id` DESC LIMIT 1>',
            'Executed SQL for variable `TAB_NUMBER`: SQL[%TAB_NUMBER%]{type: SELECT, table: tab, where: SQLConditionGroup{or: false, conditions: [SQLConditionValue{serie = tabs}, SQLConditionGroup{or: true, conditions: [SQLConditionValue{status = free}, SQLConditionValue{status = null}]}]}, returnLastID: false, orderBy: >num, executed: true, [{id: 301}]}<SELECT `num` FROM `tab` WHERE ( `serie` = \'tabs\' AND ( `status` = \'free\' OR `status` IS NULL ) ) ORDER BY `num` DESC LIMIT 1>',
            'SQL executed: SQL[11]{type: INSERT, table: order, parameters: {product: 123, price: 10.2, title: Water, user: %SYS_USER%, tab: %TAB_NUMBER%}, variables: {SYS_USER: u10, TAB_NUMBER: 301}, returnLastID: true, lastID: 101, executed: true, []}<INSERT INTO `order` (`product` , `price` , `title` , `user` , `tab`) VALUES (123 , 10.2 , \'Water\' , \'u10\' , 301)>',
            'SQL executed: SQL[12]{type: UPDATE, table: product, where: SQLConditionGroup{or: false, conditions: [SQLConditionValue{id = 123}, SQLConditionValue{type != x}]}, parameters: {last_date: 2020-10-11 00:00:00.000Z, count: [count + 1]}, returnLastID: false, executed: true, []}<UPDATE `product` SET `last_date` = \'2020-10-11 00:00:00\' , `count` = count + 1 WHERE ( `id` = 123 AND `type` != \'x\' )>',
            'SQL executed: SQL[13]{type: INSERT, table: order_ref, parameters: {order: #order:11#, next_order: [#order:11# + 10], ref: 1002}, returnLastID: true, lastID: 111, executed: true, []}<INSERT INTO `order_ref` (`order` , `next_order` , `ref`) VALUES (101 , 101 + 10 , 1002)>',
            'SQL executed: SQL[14]{type: UPDATE, table: order_ref, where: SQLConditionValue{id = #order_ref:13#}, parameters: {last_date: 2020-10-11 01:02:03.000Z, qr: [1, 2, 3, 4]}, returnLastID: false, executed: true, []}<UPDATE `order_ref` SET `last_date` = \'2020-10-11 01:02:03\' , `qr` = \'\\x01020304\' WHERE `id` = 111>',
            'SQL executed: SQL[15]{type: DELETE, table: tab_use, where: SQLConditionValue{num = %TAB_NUMBER%}, parameters: {last_date: 2020-10-11 01:02:03.000Z}, variables: {TAB_NUMBER: 301}, returnLastID: false, executed: true, []}<DELETE FROM `tab_use` WHERE `num` = 301>',
            'Commit transaction: OK'
          ]));

      expect(loggedErrors, isEmpty);

      expect(myDBConnection.insertCount, equals(101));
      expect(myDBConnection.executedSQLs.length, equals(7));

      {
        var sql = myDBConnection.executedSQLs[0];
        expect(
            sql.$1,
            equals(
                'SELECT `user_id` as `id` FROM `user` WHERE `id` > 0 ORDER BY `user_id` DESC LIMIT 1'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
        expect(
            dbCommand.sqls[0].results,
            equals([
              {'id': 'u10'}
            ]));
        expect(dbCommand.sqls[0].lastID, isNull);
      }

      {
        var sql = myDBConnection.executedSQLs[1];
        expect(
            sql.$1,
            equals(
                'SELECT `num` FROM `tab` WHERE ( `serie` = \'tabs\' AND ( `status` = \'free\' OR `status` IS NULL ) ) ORDER BY `num` DESC LIMIT 1'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
        expect(
            dbCommand.sqls[1].results,
            equals([
              {'id': 301}
            ]));
        expect(dbCommand.sqls[1].lastID, isNull);
      }

      {
        var sql = myDBConnection.executedSQLs[2];
        expect(
            sql.$1,
            equals(
                'INSERT INTO `order` (`product` , `price` , `title` , `user` , `tab`) VALUES (123 , 10.2 , \'Water\' , \'u10\' , 301)'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
        expect(dbCommand.sqls[2].lastID, equals(101));
      }

      {
        var sql = myDBConnection.executedSQLs[3];
        expect(
            sql.$1,
            equals(
                'UPDATE `product` SET `last_date` = \'2020-10-11 00:00:00\' , `count` = count + 1 WHERE ( `id` = 123 AND `type` != \'x\' )'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
      }

      {
        var sql = myDBConnection.executedSQLs[4];
        expect(
            sql.$1,
            equals(
                'INSERT INTO `order_ref` (`order` , `next_order` , `ref`) VALUES (101 , 101 + 10 , 1002)'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
        expect(dbCommand.sqls[4].lastID, equals(111));
      }

      {
        var sql = myDBConnection.executedSQLs[5];
        expect(
            sql.$1,
            equals(
                'UPDATE `order_ref` SET `last_date` = \'2020-10-11 01:02:03\' , `qr` = \'\\x01020304\' WHERE `id` = 111'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
      }

      {
        var sql = myDBConnection.executedSQLs[6];
        expect(sql.$1, equals('DELETE FROM `tab_use` WHERE `num` = 301'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
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
