import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:swiss_knife/swiss_knife.dart';

import 'command.dart';
import 'db.dart';
import 'json_helper.dart' as json_helper;

enum SQLType {
  // ignore: constant_identifier_names
  INSERT,
  // ignore: constant_identifier_names
  UPDATE,
  // ignore: constant_identifier_names
  SELECT,
  // ignore: constant_identifier_names
  DELETE,
}

SQLType? parseSQLType(Object? o) {
  if (o == null) return null;
  if (o is SQLType) return o;

  var s = o.toString().trim().toUpperCase();

  switch (s) {
    case 'INSERT':
      return SQLType.INSERT;
    case 'UPDATE':
      return SQLType.UPDATE;
    case 'SELECT':
      return SQLType.SELECT;
    case 'DELETE':
      return SQLType.DELETE;
    default:
      return null;
  }
}

/// The SQL dialect.
abstract class SQLDialect {
  final String name;

  final String q;

  SQLDialect(this.name, {required this.q});

  static String toHex(List<int> data) {
    var hex = data.map((e) => e.toRadixString(16).padLeft(2, '0')).join();
    return hex;
  }

  String toBytesString(Uint8List bytes);
}

/// SQL declaration with agnostic dialect.
class SQL implements WithVariables {
  /// The SQL ID for chain references.
  /// - To reference to this SQL result use `#$table:$sqlID#`.
  /// - Example:
  ///   - Table SQL ID `1001` at table `order`: `#order:1001#`
  final String sqlID;

  /// The target table of the SQL.
  final String table;

  /// The type of SQL:
  final SQLType type;
  final SQLCondition? where;

  final Map<String, String?> returnColumns;
  final Map<String, dynamic> parameters;

  final Map<String, dynamic> variables;

  final bool returnLastID;
  final String? orderBy;
  final int? limit;

  List<Map<String, dynamic>>? results;
  Object? lastID;

  bool executed = false;
  String? executedSQL;

  SQL(
    this.sqlID,
    this.table,
    this.type, {
    Map<String, dynamic>? parameters,
    this.where,
    Map<String, String?>? returnColumns,
    this.returnLastID = false,
    this.orderBy,
    this.limit,
    Map<String, dynamic>? variables,
    this.results,
    this.lastID,
  })  : parameters = parameters ?? {},
        returnColumns = returnColumns ?? {},
        variables = variables ?? {};

  factory SQL.fromJson(Map<String, dynamic> json) {
    return SQL(
      json["sqlID"],
      json["table"],
      parseSQLType(json["type"])!,
      parameters: json_helper.fromJsonMap<String, dynamic>(json["parameters"]),
      where: SQLCondition.fromJson(json["where"]),
      returnColumns:
          json_helper.fromJsonMap<String, String?>(json["returnColumns"]),
      returnLastID: parseBool(json["returnLastID"], false)!,
      orderBy: json['orderBy'],
      limit: json['limit'],
      variables: json_helper.fromJsonMap<String, dynamic>(json["variables"]),
    );
  }

  Map<String, dynamic> toJson() {
    return {
      "sqlID": sqlID,
      "table": table,
      "type": type.name,
      "where": where?.toJson(),
      "returnColumns": json_helper.toJson(returnColumns),
      "returnLastID": returnLastID,
      "orderBy": orderBy,
      "limit": limit,
      "parameters": json_helper.toJson(parameters),
      "variables": json_helper.toJson(variables),
    };
  }

  bool get isVariableSQL => sqlID.startsWith('%') && sqlID.endsWith('%');

  @override
  List<String> get requiredVariables => <String>{
        ...variables.keys,
        ...?where?.requiredVariables,
        ...WithVariables.extractVariables(parameters),
      }.toList();

  Future<void> resolveVariables(
          DB db, Future<Object?> Function(String name) variableResolver,
          {Map<String, dynamic>? resolvedVariables}) =>
      WithVariables.resolveVariables(requiredVariables, variableResolver,
          variables: variables, resolvedVariables: resolvedVariables);

  static Object? resolveVariableValue(
      Object value, Map<String, dynamic>? variables, List<SQL>? executedSqls) {
    if (value is List) {
      return value.map((e) {
        if (WithVariables.isVariableValue(e)) {
          return resolveVariableValue(e, variables, executedSqls);
        } else {
          return e;
        }
      }).toList();
    }

    var s = value.toString();

    if (s.startsWith('%') && s.endsWith('%')) {
      var name = s.substring(1, s.length - 1);
      var v = variables?[name];
      return v;
    } else if (s.startsWith('#') && s.endsWith('#')) {
      var ref = s.substring(1, s.length - 1);
      var refParts = ref.split(':');
      var refTable = refParts[0];
      var refSqlID = refParts[1];

      var refSql = executedSqls?.firstWhereOrNull(
          (sql) => sql.table == refTable && sql.sqlID == refSqlID);

      if (refSql != null) {
        var v = refSql.lastID ?? refSql.results;
        return v;
      }
    } else {
      var v = WithVariables.replaceVariables(s, (m) {
        var variable = m.group(1)!;
        var val = resolveVariableValue(variable, variables, executedSqls);
        return val?.toString() ?? 'null';
      });
      return v;
    }

    return null;
  }

  Map<String, dynamic> resolveParameters(List<SQL> executedSqls) {
    var parameters2 = Map<String, dynamic>.from(parameters);

    var variableEntries = parameters.entries
        .where((e) => WithVariables.isVariableValue(e.value))
        .toList();
    if (variableEntries.isEmpty) return parameters2;

    for (var e in variableEntries) {
      var v = resolveVariableValue(e.value, variables, executedSqls);
      parameters2[e.key] = v;
    }

    return parameters2;
  }

  String resolveValueAsSQL(Object? value, {required SQLDialect dialect}) {
    if (value == null) return 'NULL';

    if (value is Uint8List) {
      var hex = SQLDialect.toHex(value);
      return "'\\x$hex'";
    }

    var valueSQL = switch (value) {
      num() => '$value',
      String() => "'$value'",
      DateTime() => "'${json_helper.formatDateTime(value)}'",
      List() => value.first.toString(),
      _ => value.toString(),
    };

    return valueSQL;
  }

  ({String sql, List? valuesOrdered, Map<String, dynamic>? valuesNamed}) build(
      {required SQLDialect dialect, List<SQL>? executedSqls}) {
    executedSqls ??= [];

    final q = dialect.q;

    String sql;
    List? valuesOrdered;
    Map<String, dynamic>? valuesNamed;

    var parameters = resolveParameters(executedSqls);

    switch (type) {
      case SQLType.INSERT:
        {
          if (parameters.isEmpty) {
            throw StateError(
                "Can't build INSERT SQL with empty parameters: $this");
          }

          var columns = parameters.keys.map((p) => '$q$p$q').join(' , ');
          var values = parameters.values
              .map((e) => resolveValueAsSQL(e, dialect: dialect))
              .join(' , ');

          sql = 'INSERT INTO $q$table$q ($columns) VALUES ($values)';
        }
      case SQLType.UPDATE:
        {
          if (parameters.isEmpty) {
            throw StateError(
                "Can't build UPDATE SQL with empty parameters: $this");
          }

          final where = this.where?.build(
              dialect: dialect,
              variables: variables,
              executedSqls: executedSqls);
          if (where == null || where.isEmpty) {
            throw StateError("Can't build UPDATE SQL with empty WHERE: $this");
          }

          var set = parameters.entries
              .map((e) =>
                  '$q${e.key}$q = ${resolveValueAsSQL(e.value, dialect: dialect)}')
              .join(' , ');

          sql = 'UPDATE $q$table$q SET $set WHERE $where';
        }
      case SQLType.SELECT:
        {
          final where = this.where?.build(
              dialect: dialect,
              variables: variables,
              executedSqls: executedSqls);

          var sqlColumns = '*';

          if (returnColumns.isNotEmpty) {
            sqlColumns = returnColumns.entries.map((e) {
              var column = e.key;
              var alias = e.value ?? '';
              return alias.isNotEmpty
                  ? '$q$column$q as $q$alias$q'
                  : '$q$column$q';
            }).join(' , ');
          }

          var sqlWhere =
              where != null && where.isNotEmpty ? ' WHERE $where' : '';

          var orderBy = this.orderBy?.trim();

          var sqlOrder = '';
          if (orderBy != null && orderBy.isNotEmpty) {
            if (orderBy.startsWith('>')) {
              orderBy = orderBy.substring(1).trim();
              sqlOrder = ' ORDER BY $q$orderBy$q DESC';
            } else if (orderBy.startsWith('<')) {
              orderBy = orderBy.substring(1).trim();
              sqlOrder = ' ORDER BY $q$orderBy$q';
            } else {
              sqlOrder = ' ORDER BY $q$orderBy$q';
            }
          }

          var limit = this.limit;

          var sqlLimit = limit != null && limit > 0 ? ' LIMIT $limit' : '';

          sql = 'SELECT $sqlColumns FROM $q$table$q$sqlWhere$sqlOrder$sqlLimit';
        }
      case SQLType.DELETE:
        {
          final where = this.where?.build(
              dialect: dialect,
              variables: variables,
              executedSqls: executedSqls);

          var sqlWhere =
              where != null && where.isNotEmpty ? ' WHERE $where' : '';

          var limit = this.limit;

          var sqlLimit = limit != null && limit > 0 ? ' LIMIT $limit' : '';

          sql = 'DELETE FROM $q$table$q$sqlWhere$sqlLimit';
        }
      default:
        throw StateError("Can't build SQL: $this");
    }

    return (sql: sql, valuesOrdered: valuesOrdered, valuesNamed: valuesNamed);
  }

  Object? resolveLastInsertID(Object? id,
      {Map<String, dynamic>? valuesNamed, List<SQL>? executedSqls}) {
    if (id != null) {
      if (id is num) {
        if (id != 0) return id;
      } else if (id is String) {
        if (id.isNotEmpty) return id;
      }
    }

    var returnColumn = returnColumns.keys.firstOrNull;
    var val = valuesNamed?[returnColumn] ?? parameters[returnColumn];

    if (val == null) {
      return null;
    }

    if (val is int) {
      return val;
    } else if (val is List) {
      var valSQL = val.firstOrNull ?? '';

      var n = parseInt(valSQL);
      if (n != null) return n;

      if (WithVariables.isVariableValue(valSQL)) {
        valSQL = SQL.resolveVariableValue(valSQL, variables, executedSqls);
      }

      var operation =
          RegExp(r'^\s*(-?\d+)\s*([+-])\s*(-?\d+)\s*$', dotAll: true)
              .firstMatch(valSQL);

      if (operation != null) {
        var a = parseInt(operation.group(1));
        var op = operation.group(2)!;
        var b = parseInt(operation.group(3));

        if (a != null && b != null) {
          if (op == '+') {
            return a + b;
          } else if (op == '-') {
            return a - b;
          }
        }
      }

      return null;
    } else {
      return parseInt(val);
    }
  }

  String get info {
    return [
      'type: ${type.name}',
      'table: $table',
      if (where != null) 'where: $where',
      if (parameters.isNotEmpty) 'parameters: $parameters',
      if (variables.isNotEmpty) 'variables: $variables',
      'returnLastID: $returnLastID',
      if (orderBy != null) 'orderBy: $orderBy',
      if (lastID != null) 'lastID: $lastID',
      'executed: $executed',
      if (results != null) '$results',
    ].join(', ');
  }

  @override
  String toString() {
    return 'SQL[$sqlID]{$info}${executedSQL != null ? '<$executedSQL>' : ''}';
  }
}

abstract class SQLCondition implements WithVariables {
  static SQLCondition? fromJson(dynamic json) {
    if (json == null) {
      return null;
    } else if (json is List) {
      return SQLConditionValue.fromJson(json);
    } else if (json is Map) {
      return SQLConditionGroup.fromJson(json.map((k, v) => MapEntry("$k", v)));
    } else {
      throw StateError("Unknown condition JSON: $json");
    }
  }

  SQLCondition();

  @override
  List<String> get requiredVariables;

  dynamic toJson();

  String build(
      {required SQLDialect dialect,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls});
}

class SQLConditionGroup extends SQLCondition {
  final bool or;
  final List<SQLCondition> conditions;

  SQLConditionGroup(this.or, this.conditions);

  SQLConditionGroup.and(this.conditions) : or = false;

  SQLConditionGroup.or(this.conditions) : or = true;

  @override
  List<String> get requiredVariables =>
      conditions.expand((c) => c.requiredVariables).toList();

  factory SQLConditionGroup.fromJson(Map<String, dynamic> json) =>
      SQLConditionGroup(
        json["or"].toString().toLowerCase() == 'true',
        (json["conditions"] as List)
            .map((j) => SQLCondition.fromJson(j))
            .whereNotNull()
            .toList(),
      );

  @override
  Map<String, dynamic> toJson() => {
        "or": or,
        "conditions": conditions.map((e) => e.toJson()).toList(),
      };

  @override
  String build(
      {required SQLDialect dialect,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls}) {
    if (conditions.length == 1) {
      return conditions.first.build(
        dialect: dialect,
        variables: variables,
        executedSqls: executedSqls,
      );
    }

    var op = or ? ' OR ' : ' AND ';
    var conditionLine = conditions
        .map((e) => e.build(
              dialect: dialect,
              variables: variables,
              executedSqls: executedSqls,
            ))
        .join(op);
    return '( $conditionLine )';
  }

  @override
  String toString() {
    return 'SQLConditionGroup{or: $or, conditions: $conditions}';
  }
}

class SQLConditionValue extends SQLCondition {
  final String field;
  final String op;
  final Object? value;

  SQLConditionValue(this.field, this.op, this.value);

  @override
  List<String> get requiredVariables => WithVariables.extractVariables(value);

  factory SQLConditionValue.fromJson(List json) => SQLConditionValue(
        json[0],
        json[1],
        json[2],
      );

  @override
  List toJson() => [field, op, json_helper.toJson(value)];

  @override
  String build(
      {required SQLDialect dialect,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls}) {
    Object? value = this.value;

    final q = dialect.q;

    if (value != null && WithVariables.isVariableValue(value)) {
      value = SQL.resolveVariableValue(value, variables, executedSqls);
    }

    var v = switch (value) {
      num() => '$value',
      String() => "'$value'",
      List() => value.first,
      _ => value?.toString() ?? 'null',
    };

    if (equalsIgnoreAsciiCase(v, 'null')) {
      if (op == '=') {
        return '$q$field$q IS NULL';
      } else if (op == '!=' || op == '<>') {
        return '$q$field$q IS NOT NULL';
      }
    }

    return '$q$field$q $op $v';
  }

  @override
  String toString() {
    return 'SQLConditionValue{$field $op $value}';
  }
}

extension IterableSQLFromJsonExtension on Iterable {
  List<SQL> toListOfSQLFromJson() => whereJsonMap().map(SQL.fromJson).toList();
}
