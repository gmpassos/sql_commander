import 'dart:async';
import 'dart:typed_data';

import 'package:logging/logging.dart' as logging;
import 'package:postgres/postgres.dart';
import 'package:swiss_knife/swiss_knife.dart';

import 'db.dart';
import 'sql.dart';

final _log = logging.Logger('DBConnectionPostgres');

class PostgreSQLDialect extends SQLDialect {
  PostgreSQLDialect() : super('postgres', q: '"');

  @override
  String toBytesString(Uint8List bytes) {
    var hex = SQLDialect.toHex(bytes);
    return "'\\x$hex'";
  }
}

/// [DBConnection] for PostgreSQL.
class DBConnectionPostgres extends DBConnection<PostgreSQLConnection> {
  /// Registers a [DBConnectionProvider] for PostgreSQL.
  static void register() => DBConnectionProvider.registerProvider(
      'postgres', DBConnectionPostgres.provider);

  /// Returns a [DBConnectionPoolProvider] for PostgreSQL.
  static DBConnectionPoolProvider<DBConnectionPostgres> provider(
          DBConnectionCredential credential,
          {int? maxConnections,
          Duration? retryInterval,
          int? maxRetries}) =>
      DBConnectionPoolProvider<DBConnectionPostgres>(
        credential,
        open,
        maxConnections: maxConnections ?? 1,
        retryInterval: retryInterval ?? Duration(seconds: 1),
        maxRetries: maxRetries ?? 10,
      );

  /// Opens a [DBConnectionPostgres].
  static Future<DBConnectionPostgres?> open(DBConnectionCredential credential,
          {Duration? retryInterval, int? maxRetries}) =>
      DBConnection.openConnection(
        credential,
        retryInterval: retryInterval ?? Duration(seconds: 1),
        maxRetries: maxRetries ?? 10,
        (credential) async {
          var conn = PostgreSQLConnection(
              credential.host, credential.port, credential.db,
              username: credential.user, password: credential.pass);
          var ok = await conn.open();
          return parseBool(ok, false)! ? DBConnectionPostgres(conn) : null;
        },
      );

  DBConnectionPostgres(PostgreSQLConnection nativeConnection)
      : super(nativeConnection, PostgreSQLDialect());

  @override
  Future<void> close() async {
    await nativeConnection.close();
  }

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

    switch (sql.type) {
      case SQLType.INSERT:
        {
          var idFieldName = await resolveTableIDFieldName(sql.table);

          var sqlQuery = '${s.sql} RETURNING "${sql.table}"."$idFieldName" ';
          var result = await nativeConnection.mappedResultsQuery(sqlQuery,
              substitutionValues: s.valuesNamed);

          if (result.isNotEmpty) {
            var resultID =
                _resolveResultID(result, sql.table, idFieldName: idFieldName);
            var lastId = sql.resolveLastInsertID(resultID,
                valuesNamed: s.valuesNamed, executedSqls: executedSqls);
            return (results: result, lastID: lastId);
          } else {
            return null;
          }
        }
      case SQLType.UPDATE:
        {
          var result = await nativeConnection.query(s.sql,
              substitutionValues: s.valuesNamed);

          return result.affectedRowCount > 0
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
          var results = await nativeConnection.mappedResultsQuery(s.sql,
              substitutionValues: s.valuesNamed);

          return (results: results, lastID: null);
        }
      case SQLType.DELETE:
        {
          var result = await nativeConnection.query(s.sql,
              substitutionValues: s.valuesNamed);

          return result.affectedRowCount > 0
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

  FutureOr<String> resolveTableIDFieldName(String table) => 'id';

  Object? _resolveResultID(
      List<Map<String, Map<String, dynamic>>> results, String table,
      {String? idFieldName}) {
    if (results.isEmpty) {
      return null;
    }

    var returning = results.first[table];

    if (returning == null || returning.isEmpty) {
      return null;
    } else if (returning.length == 1) {
      var id = returning.values.first;
      return id;
    } else {
      if (idFieldName != null) {
        var id = returning[idFieldName];
        return id;
      } else {
        var id = returning.values.first;
        return id;
      }
    }
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
