import 'package:collection/collection.dart';
import 'package:swiss_knife/swiss_knife.dart';

import 'db.dart';
import 'sql.dart';

typedef CommandLogInfo = void Function(String message);
typedef CommandLogError = void Function(String message,
    [Object? error, StackTrace? stackTrace]);

/// A generic command.
abstract class Command<P> {
  /// The type of the command.
  String get commandType;

  /// The execution group of the command.
  String get executionGroup;

  /// Prepared the command to be executed.
  Future<P?> prepare();

  /// Executes the command and returns `true` if successful.
  Future<bool> execute(
      {P? prepared, CommandLogInfo? logInfo, CommandLogError? logError});

  /// Disposes the command resources.
  Future<void> dispose(P? prepared, {int executedCommands = 1});
}

/// A DB command.
class DBCommand extends Command<DB> {
  final String host;
  final int port;
  final String user;
  final String pass;
  final String db;
  final String software;
  final List<SQL> sqls;

  @override
  String get executionGroup => '$host:$port';

  @override
  String get commandType => 'DB';

  /// Returns a [List] of [SQL]s already executed.
  List<SQL> get executedSqls => sqls.where((e) => e.executed).toList();

  DBCommand(this.host, this.port, this.user, this.pass, this.db, this.software,
      this.sqls);

  /// Creates a [DBCommand] from a JSON [Map].
  factory DBCommand.fromJson(Map<String, dynamic> json) => DBCommand(
        (json["host"] ?? json["ip"]) as String,
        parseInt(json["port"])!,
        json["user"] as String,
        json["pass"] as String,
        json["db"] as String,
        json["software"] as String,
        (json["sqls"] as List)
            .whereType<Map>()
            .map((e) => e.map((k, v) => MapEntry('$k', v)))
            .map((e) => SQL.fromJson(e))
            .toList(),
      );

  /// Converts this [DBCommand] to JSON.
  Map<String, dynamic> toJson() => {
        "host": host,
        "port": port,
        "user": user,
        "pass": pass,
        "db": db,
        "software": software,
        "sqls": sqls.map((e) => e.toJson()).toList(),
      };

  /// Returns the [DBConnectionCredential].
  DBConnectionCredential get credential =>
      DBConnectionCredential(host, port, user, pass, db);

  Future<DB?> openDB(
      {Duration retryInterval = const Duration(seconds: 1),
      int maxRetries = 10,
      int maxConnections = 1}) async {
    var dbType = software.trim().toLowerCase();

    var connProvider = await DBConnectionProvider.getProvider(
      dbType,
      credential,
      retryInterval: retryInterval,
      maxRetries: maxRetries,
      maxConnections: maxConnections,
    );

    if (connProvider == null) return null;

    return DB(connProvider);
  }

  @override
  Future<DB?> prepare({int maxRetries = 10}) => openDB(maxRetries: maxRetries);

  @override
  Future<bool> execute(
      {DB? prepared,
      CommandLogInfo? logInfo,
      CommandLogError? logError}) async {
    if (sqls.isEmpty) return false;

    var db = prepared ?? (await openDB());

    if (db == null) {
      if (logError != null) {
        logError("Can't open DB: $this");
      }
      return false;
    }

    try {
      var started = await db.startTransaction();
      if (!started) return false;

      await resolveSQLs(db);

      var ok = await _executeImpl(db, logInfo: logInfo, logError: logError);

      if (ok) {
        ok = await db.commitTransaction();
        return ok;
      } else {
        await db.rollbackTransaction();
        return false;
      }
    } catch (e, s) {
      print(e);
      print(s);
      await db.rollbackTransaction();
      return false;
    }
  }

  Future<Object?> sqlVariableResolver(DB db, String variableName) async {
    var variableSQLs =
        sqls.where((sql) => sql.sqlID == '%$variableName%').toList();

    for (var sql in variableSQLs) {
      var r = await db.executeSQL(sql, executedSqls: executedSqls);
      if (r == null) continue;

      var results = r.results;
      if (results == null || results.isEmpty) continue;

      var value = results.first.values.firstOrNull;
      if (value != null) {
        return value;
      }
    }

    return null;
  }

  Future<void> resolveSQLs(DB db) async {
    var resolvedVariables = <String, dynamic>{};

    for (var sql in sqls) {
      await sql.resolveVariables(db, (v) => sqlVariableResolver(db, v),
          resolvedVariables: resolvedVariables);
    }
  }

  Future<bool> _executeImpl(DB db,
      {CommandLogInfo? logInfo, CommandLogError? logError}) async {
    if (sqls.isEmpty) return false;

    var sqlsToExecute = sqls.where((sql) => !sql.isVariableSQL).toList();

    for (var sql in sqlsToExecute) {
      var ok = await db.executeSQL(sql, executedSqls: executedSqls);
      if (ok == null) {
        return false;
      }
    }

    return true;
  }

  @override
  Future<void> dispose(DB? prepared, {int executedCommands = 1}) async {
    await prepared?.close();
  }
}
