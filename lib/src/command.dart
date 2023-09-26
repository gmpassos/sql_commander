import 'dart:async';

import 'package:collection/collection.dart';
import 'package:swiss_knife/swiss_knife.dart';

import 'db.dart';
import 'json_helper.dart' as json_helper;
import 'sql.dart';

typedef CommandLogInfo = void Function(String message);
typedef CommandLogError = void Function(String message,
    [Object? error, StackTrace? stackTrace]);

/// Base class for logging command.
abstract class CommandLog {
  /// Logs an INFO message.
  CommandLogInfo? logInfo;

  /// Logs an ERROR message.
  CommandLogError? logError;

  CommandLog({this.logInfo, this.logError});
}

/// Interface for classes with variables to resolve.
abstract class WithVariables {
  /// A list of required variables.
  List<String> get requiredVariables;

  static final RegExp _regexpVariable = RegExp(r'(%\w+%|#[^:#]+:[^:#]+#)');

  /// Returns `true` if [value] has a variable:
  /// - `%varName%`: A variable name.
  /// - `#table:sqlID#`: references to a table and sql ID.
  static bool isVariableValue(Object? value) =>
      (value is String && _regexpVariable.hasMatch(value)) ||
      (value is List && value.any(isVariableValue));

  /// Extracts the variables names of [value].
  static List<String> extractVariables(Object? value) {
    if (value == null) return [];

    if (value is String) {
      if (value.startsWith('%') && value.endsWith('%')) {
        return [value.substring(1, value.length - 1)];
      }
    } else if (value is WithVariables) {
      return value.requiredVariables;
    } else if (value is List) {
      return value.expand(extractVariables).toList();
    } else if (value is Map) {
      return value.values.expand(extractVariables).toList();
    }

    return [];
  }

  static String replaceVariables(
          String s, String Function(Match match) replace) =>
      s.replaceAllMapped(_regexpVariable, replace);

  static Future<Map<String, dynamic>> resolveVariables(
    List<String> requiredVariables,
    Future<Object?> Function(String name) variableResolver, {
    Map<String, dynamic>? variables,
    Map<String, dynamic>? resolvedVariables,
  }) async {
    resolvedVariables ??= {};

    if (requiredVariables.isEmpty) return resolvedVariables;

    variables ??= {};

    for (var key in requiredVariables) {
      var value = variables[key] ?? resolvedVariables[key];
      value ??= await variableResolver(key);

      variables[key] ??= value;
      resolvedVariables[key] ??= value;
    }

    return resolvedVariables;
  }
}

/// A generic command.
/// - [P] is the prepared instance shared between executions.
abstract class Command<P> extends CommandLog {
  Command({super.logInfo, super.logError});

  /// The type of the command.
  /// Disposes the command resources.
  String get commandType;

  /// The execution group of the command.
  String get executionGroup;

  /// Prepared the command to be executed.
  Future<P?> prepare();

  /// Executes the command and returns `true` if successful.
  Future<bool> execute({P? prepared});

  /// Dispose this command resources and [prepared] instance.
  Future<void> dispose(P? prepared);
}

/// A DB command.
class DBCommand extends Command<DB> implements WithVariables {
  final String id;

  final String host;
  final int port;
  final String user;
  final String pass;
  final String db;
  final String software;
  final List<SQL> sqls;
  final Map<String, dynamic> properties;

  @override
  String get executionGroup => '$host:$port';

  @override
  String get commandType => 'DB';

  /// Returns a [List] of [SQL]s already executed.
  List<SQL> get executedSqls => sqls.where((e) => e.executed).toList();

  DBCommand(this.host, this.port, this.user, this.pass, this.db, this.software,
      this.sqls,
      {Map<String, dynamic>? properties, String? id})
      : properties = properties ?? {},
        id = id?.trim() ?? '';

  /// Creates a [DBCommand] from a JSON [Map].
  factory DBCommand.fromJson(Map<String, dynamic> json) => DBCommand(
        (json["host"] ?? json["ip"]) as String,
        parseInt(json["port"])!,
        json["user"] as String,
        json["pass"] as String,
        json["db"] as String,
        json["software"] as String,
        (json["sqls"] as List).toListOfSQLFromJson(),
        properties:
            json_helper.fromJsonMap<String, dynamic>(json["properties"]),
        id: json["id"] as String?,
      );

  /// Converts this [DBCommand] to JSON.
  Map<String, dynamic> toJson() => {
        "id": id,
        "host": host,
        "port": port,
        "user": user,
        "pass": pass,
        "db": db,
        "software": software,
        "properties": json_helper.toJson(properties),
        "sqls": sqls.map((e) => e.toJson()).toList(),
      };

  @override
  List<String> get requiredVariables => WithVariables.extractVariables(sqls);

  /// Returns a property with [key] from [DBCommand.properties] or
  /// from the parameter [properties].
  dynamic getProperty(String key, {Map<String, dynamic>? properties}) {
    var value = this.properties[key] ?? properties?[key];
    return value;
  }

  /// Returns the [SQL] with [sqlID] from [sqls].
  SQL? getSQLByID(String sqlID) =>
      sqls.firstWhereOrNull((sql) => sql.sqlID == sqlID);

  /// Returns the [SQL]s with ids in [sqlIDs] from [sqls].
  List<SQL> getSQLsByIDs(List<String> sqlIDs) =>
      sqls.where((sql) => sqlIDs.contains(sql.sqlID)).toList();

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
  Future<bool> execute({DB? prepared, Map<String, dynamic>? properties}) {
    return executeSQLs(sqls, properties: properties, prepared: prepared);
  }

  Future<bool> executeSQLs(List<SQL> sqls,
      {DB? prepared, Map<String, dynamic>? properties}) async {
    if (sqls.isEmpty) return false;

    final logInfo = this.logInfo;
    final logError = this.logInfo;

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

      if (logInfo != null) {
        logInfo("Started transaction");
      }

      await resolveSQLs(db, properties: properties);

      var ok = await _executeImpl(db, sqls);

      if (ok) {
        ok = await db.commitTransaction();
        if (logInfo != null) {
          logInfo("Commit transaction: ${ok ? 'OK' : 'FAILED'}");
        }
        return ok;
      } else {
        if (logInfo != null) {
          logInfo("Rollback transaction");
        }
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

  Future<Object?> sqlVariableResolver(DB db, String variableName,
      {Map<String, dynamic>? properties}) async {
    final logInfo = this.logInfo;
    final logError = this.logError;

    var variableSQLs =
        sqls.where((sql) => sql.sqlID == '%$variableName%').toList();

    for (var sql in variableSQLs) {
      var r = await db.executeSQL(sql, executedSqls: executedSqls);
      if (r == null) {
        if (logError != null) {
          logError("SQL execution for variable `$variableName` failed: $sql");
        }
        continue;
      }

      if (logInfo != null) {
        logInfo("Executed SQL for variable `$variableName`: $sql");
      }

      var results = r.results;
      if (results == null || results.isEmpty) continue;

      var value = results.first.values.firstOrNull;
      if (value != null) {
        return value;
      }
    }

    return getProperty(variableName, properties: properties);
  }

  Future<void> resolveSQLs(DB db, {Map<String, dynamic>? properties}) async {
    var resolvedVariables = <String, dynamic>{};

    for (var sql in sqls) {
      await sql.resolveVariables(
          db, (v) => sqlVariableResolver(db, v, properties: properties),
          resolvedVariables: resolvedVariables);
    }
  }

  Future<bool> _executeImpl(DB db, List<SQL> sqls) async {
    if (sqls.isEmpty) return false;

    final logInfo = this.logInfo;
    final logError = this.logInfo;

    var sqlsToExecute = sqls.where((sql) => !sql.isVariableSQL).toList();

    for (var sql in sqlsToExecute) {
      var ok = await db.executeSQL(sql, executedSqls: executedSqls);
      if (ok == null) {
        if (logError != null) {
          logError("SQL execution failed: $sql");
        }
        return false;
      }

      if (logInfo != null) {
        logInfo("SQL executed: $sql");
      }
    }

    return true;
  }

  @override
  Future<void> dispose(DB? prepared) async {
    await prepared?.close();
  }
}

/// A set of [DBCommand]s.
class DBCommandSet extends CommandLog {
  final Set<DBCommand> dbCommands;

  DBCommandSet({Iterable<DBCommand>? dbCommands, super.logInfo, super.logError})
      : dbCommands = (dbCommands ?? []).toSet();

  /// Returns the [DBCommand] with [id] from [dbCommands].
  DBCommand? getDBCommandByID(String id) =>
      dbCommands.firstWhereOrNull((cmd) => cmd.id == id);

  /// Returns the [DBCommand] with an [SQL] with [sqlID] from [dbCommands].
  DBCommand? getDBCommandWithSQL(String sqlID) => dbCommands
      .firstWhereOrNull((cmd) => cmd.sqls.any((sql) => sql.sqlID == sqlID));

  /// Returns the [SQL] with [sqlID] from [dbCommands].
  SQL? getSQLByID(String sqlID) =>
      getDBCommandWithSQL(sqlID)?.getSQLByID(sqlID);

  FutureOr<bool> executeDBCommandByID(String id,
      {Map<String, dynamic>? properties}) {
    var dbCommand = getDBCommandByID(id);

    if (dbCommand == null) {
      final logInfo = this.logInfo;
      if (logInfo != null) {
        logInfo("Can't find `DBCommand`: $id");
      }
      return false;
    }

    dbCommand.logInfo ??= logInfo;
    dbCommand.logError ??= logError;

    return dbCommand.execute(properties: properties);
  }

  FutureOr<bool> executeSQLByID(String sqlID,
      {Map<String, dynamic>? properties}) {
    final logInfo = this.logInfo;
    final logError = this.logError;

    var dbCommand = getDBCommandWithSQL(sqlID);

    var sql = dbCommand?.getSQLByID(sqlID);
    if (sql == null) {
      if (logInfo != null) {
        logInfo("Can't find SQL: $sqlID");
      }
      return false;
    }

    dbCommand!.logInfo ??= logInfo;
    dbCommand.logError ??= logError;

    return dbCommand.executeSQLs([sql], properties: properties);
  }

  Future<bool> executeSQLsByIDs(List<String> sqlIDs,
      {Map<String, dynamic>? properties}) async {
    final logInfo = this.logInfo;
    final logError = this.logError;

    sqlIDs = sqlIDs.toSet().toList();

    var dbCommands = sqlIDs.map(getDBCommandWithSQL).whereNotNull().toList();
    if (dbCommands.isEmpty) {
      if (logInfo != null) {
        logInfo("Can't find SQLs: $sqlIDs");
      }
      return false;
    }

    if (dbCommands.length < sqlIDs.length) {
      if (logInfo != null) {
        logInfo("Can't find ALL SQLs: $sqlIDs");
      }
      return false;
    }

    dbCommands = dbCommands.toSet().toList();

    for (var dbCommand in dbCommands) {
      dbCommand.logInfo ??= logInfo;
      dbCommand.logError ??= logError;

      var sqls = dbCommand.getSQLsByIDs(sqlIDs);

      var ok = await dbCommand.executeSQLs(sqls, properties: properties);
      if (!ok) {
        if (logInfo != null) {
          logInfo(" SQLs execution failed: $sqls");
        }
        return false;
      }
    }

    return true;
  }

  List<Map<String, dynamic>>? getSQLResults(String sqlID) {
    var sql = getSQLByID(sqlID);
    if (sql == null) return null;
    return sql.results;
  }

  Map<String, dynamic>? getSQLResult(String sqlID) =>
      getSQLResults(sqlID)?.firstOrNull;

  List? getSQLResultsColumn(String sqlID, String column) {
    var sql = getSQLByID(sqlID);
    if (sql == null) return null;
    return sql.results?.map((e) => e[column]).toList();
  }

  dynamic getSQLResultColumn(String sqlID, String column) =>
      getSQLResultsColumn(sqlID, column)?.firstOrNull;

  Map<String, dynamic> toJson() => {
        "dbCommands": json_helper.toJson(dbCommands.toList()),
      };

  factory DBCommandSet.fromJson(Map<String, dynamic> json) {
    return DBCommandSet(
      dbCommands: (json["dbCommands"] as List).toListOfDBCommandFromJson(),
    );
  }
}

extension IterableDBCommandFromJsonExtension on Iterable {
  List<DBCommand> toListOfDBCommandFromJson() =>
      whereJsonMap().map(DBCommand.fromJson).toList();
}
