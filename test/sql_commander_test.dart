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
          parameters: {'last_date': DateTime.utc(2020, 10, 11, 1, 2, 3)},
          where: SQLConditionValue('id', '=', '#order_ref:13#'),
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
      expect(dbCommand.sqls.length, equals(6));

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

      var myDBConnection = _MyDBConnection();

      var loggedInfos = [];
      var loggedErrors = [];

      var connectionProvider = DBSingleConnectionProvider(myDBConnection);
      var db = DB(connectionProvider);

      var sqlOK = await dbCommand.execute(
          prepared: db,
          logInfo: (m) => loggedInfos.add(m),
          logError: (m, [e, s]) => loggedErrors.add([m, e, s]));

      expect(sqlOK, isTrue);
      expect(loggedInfos, isEmpty);
      expect(loggedErrors, isEmpty);

      expect(myDBConnection.insertCount, equals(101));
      expect(myDBConnection.executedSQLs.length, equals(6));

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
                'UPDATE `order_ref` SET `last_date` = \'2020-10-11 01:02:03\' WHERE `id` = 111'));
        expect(sql.$2, isNull);
        expect(sql.$3, isNull);
      }
    });
  });
}

class _MyDBConnection extends DBConnection<int> {
  static int _connectionIdCount = 0;

  _MyDBConnection() : super(++_connectionIdCount);

  final List<(String, List?, Map?)> executedSQLs = [];

  int insertCount = 100;

  @override
  Future<({List<Map<String, dynamic>>? results, Object? lastID})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls}) async {
    var s = sql.build(q: '`', executedSqls: executedSqls);

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
