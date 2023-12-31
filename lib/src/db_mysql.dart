import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:logging/logging.dart' as logging;
import 'package:mysql1/mysql1.dart';

import 'db.dart';
import 'sql.dart';

final _log = logging.Logger('DBConnectionMysql');

class MySQLDialect extends SQLDialect {
  MySQLDialect() : super('mysql', q: '`');

  @override
  String toBytesString(Uint8List bytes) {
    var hex = SQLDialect.toHex(bytes);
    return "X'$hex'";
  }
}

/// [DBConnection] for MySQL.
class DBConnectionMySQL extends DBConnection<MySqlConnection> {
  /// Registers a [DBConnectionProvider] for MySQL.
  static void register() => DBConnectionProvider.registerProvider(
      'mysql', DBConnectionMySQL.provider);

  /// Returns a [DBConnectionPoolProvider] for MySQL.
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

  /// Opens a [DBConnectionMySQL].
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

  DBConnectionMySQL(MySqlConnection nativeConnection)
      : super(nativeConnection, MySQLDialect());

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
    var s = sql.build(dialect: dialect, executedSqls: executedSqls);

    sql.executedSQL = s.sql;

    switch (sql.type) {
      case SQLType.INSERT:
        {
          var result = await nativeConnection.query(s.sql, s.valuesOrdered);
          var affectedRows = result.affectedRows ?? 0;

          var results = _resolveMySQLResults(result);

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
          var results = _resolveMySQLResults(result);
          return (results: results, lastID: null);
        }
      case SQLType.DELETE:
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
      default:
        throw StateError("Can't execute SQL: $sql");
    }
  }

  List<Map<String, dynamic>> _resolveMySQLResults(Results result) {
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
