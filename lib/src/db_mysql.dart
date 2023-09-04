import 'package:collection/collection.dart';
import 'package:logging/logging.dart' as logging;
import 'package:mysql1/mysql1.dart';

import 'db.dart';
import 'sql.dart';

final _log = logging.Logger('DBConnectionMysql');

class DBConnectionMySQL extends DBConnection<MySqlConnection> {
  static void register() => DBConnectionProvider.registerProvider(
      'mysql', DBConnectionMySQL.provider);

  static DBConnectionPoolProvider<DBConnectionMySQL> provider(
          DBConnectionCredential credential,
          {int? maxConnections,
          Duration? retryInterval,
          int? maxRetries}) =>
      DBConnectionPoolProvider<DBConnectionMySQL>(
        credential,
        open,
        maxConnections: maxConnections ?? 1,
        retryInterval: retryInterval ?? Duration(seconds: 1),
        maxRetries: maxRetries ?? 10,
      );

  static Future<DBConnectionMySQL?> open(DBConnectionCredential credential,
          {Duration? retryInterval, int? maxRetries}) =>
      DBConnection.openConnection(
        credential,
        retryInterval: retryInterval ?? const Duration(seconds: 1),
        maxRetries: maxRetries ?? 10,
        (credential) async {
          var settings = ConnectionSettings(
              host: credential.host,
              port: credential.port,
              user: credential.user,
              password: credential.pass,
              db: credential.db);

          var conn = await MySqlConnection.connect(settings);
          return DBConnectionMySQL(conn);
        },
      );

  DBConnectionMySQL(super.nativeConnection);

  @override
  Future<void> close() => nativeConnection.close();

  @override
  Future<bool> startTransaction() async {
    try {
      await nativeConnection.query('START TRANSACTION');
      return true;
    } catch (e, s) {
      _log.severe("Transaction start error", e, s);
      return false;
    }
  }

  @override
  Future<bool> rollbackTransaction() async {
    try {
      await nativeConnection.query('ROLLBACK');
      return true;
    } catch (e, s) {
      _log.severe("Transaction rollback error", e, s);
      return false;
    }
  }

  @override
  Future<bool> commitTransaction() async {
    try {
      await nativeConnection.query('COMMIT');
      return true;
    } catch (e, s) {
      _log.severe("Transaction commit error", e, s);
      return false;
    }
  }

  @override
  Future<({Object? lastID, List<Map<String, dynamic>>? results})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls}) async {
    var s = sql.build(q: '`', executedSqls: executedSqls);

    switch (sql.type) {
      case SQLType.INSERT:
        {
          var result = await nativeConnection.query(s.sql, s.valuesOrdered);
          var affectedRows = result.affectedRows ?? 0;

          var results = _resolveMyAQLResults(result);

          if (affectedRows > 0) {
            var lastId = sql.resolveLastInsertID(result.insertId,
                valuesNamed: s.valuesNamed, executedSqls: executedSqls);
            return (results: results, lastID: lastId);
          } else {
            return null;
          }
        }
      case SQLType.UPDATE:
        {
          var result = await nativeConnection.query(s.sql, s.valuesOrdered);
          var affectedRows = result.affectedRows ?? 0;
          return affectedRows > 0
              ? (
                  results: [
                    {'ok': true}
                  ],
                  lastID: null
                )
              : null;
        }
      case SQLType.SELECT:
        {
          var result = await nativeConnection.query(s.sql, s.valuesOrdered);

          var results = _resolveMyAQLResults(result);

          return (results: results, lastID: null);
        }
      default:
        throw StateError("Can't execute SQL: $sql");
    }
  }

  List<Map<String, dynamic>> _resolveMyAQLResults(Results result) {
    var fields = result.fields.map((f) => f.name).toList();

    var results = result
        .map((r) => Map.fromEntries(
            r.mapIndexed((i, e) => MapEntry(fields[i] ?? '$i', e))))
        .toList();

    return results;
  }
}

abstract class DBConnectionWrapper {
  Future<bool> startTransaction();

  Future<bool> rollbackTransaction();

  Future<bool> commitTransaction();

  Future<({List<Map<String, dynamic>>? results, Object? lastID})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls});
}
