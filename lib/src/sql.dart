import 'package:collection/collection.dart';
import 'package:intl/intl.dart';
import 'package:swiss_knife/swiss_knife.dart';

import 'db.dart';

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

/// SQL declaration with agnostic dialect.
class SQL {
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
      json["type"],
      parameters: (json["parameters"] as Map).map((k, v) => MapEntry('$k', v)),
      where: SQLCondition.fromJson(json["where"]),
      returnColumns: (json["returnColumns"] as Map)
          .map((k, v) => MapEntry('$k', v?.toString())),
      returnLastID: parseBool(json["returnLastID"], false)!,
      orderBy: json['orderBy'],
      limit: json['limit'],
      variables: (json["variables"] as Map).map((k, v) => MapEntry('$k', v)),
    );
  }

  Map<String, dynamic> toJson() {
    return {
      "sqlID": sqlID,
      "table": table,
      "type": type,
      "where": where?.toJson(),
      "returnColumns": deepCopyMap(returnColumns),
      "returnLastID": returnLastID,
      "orderBy": orderBy,
      "limit": limit,
      "parameters": deepCopyMap(parameters),
      "variables": deepCopyMap(variables),
    };
  }

  bool get isVariableSQL => sqlID.startsWith('%') && sqlID.endsWith('%');

  Future<void> resolveVariables(
      DB db, Future<Object?> Function(String name) variableResolver,
      {Map<String, dynamic>? resolvedVariables}) async {
    if (variables.isEmpty) return;

    resolvedVariables ??= {};

    var keys = variables.keys.toList();

    for (var key in keys) {
      var value = variables[key] ?? resolvedVariables[key];
      value ??= await variableResolver(key);

      variables[key] ??= value;
      resolvedVariables[key] ??= value;
    }
  }

  static final RegExp _regexpVariable = RegExp(r'(%\w+%|#[^:#]+:[^:#]+#)');

  static bool isVariableValue(Object? value) =>
      (value is String && _regexpVariable.hasMatch(value)) ||
      (value is List && value.any(isVariableValue));

  static Object? resolveVariableValue(
      Object value, Map<String, dynamic>? variables, List<SQL>? executedSqls) {
    if (value is List) {
      return value.map((e) {
        if (isVariableValue(e)) {
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
      var v = s.replaceAllMapped(_regexpVariable, (m) {
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

    var variableEntries =
        parameters.entries.where((e) => isVariableValue(e.value)).toList();
    if (variableEntries.isEmpty) return parameters2;

    for (var e in variableEntries) {
      var v = resolveVariableValue(e.value, variables, executedSqls);
      parameters2[e.key] = v;
    }

    return parameters2;
  }

  static final sqlDateTimeFormat = DateFormat('yyyy-MM-dd HH:mm:ss');

  String resolveValueAsSQL(Object? value) {
    if (value == null) return 'NULL';

    var valueSQL = switch (value) {
      num() => '$value',
      String() => "'$value'",
      DateTime() => "'${sqlDateTimeFormat.format(value.toUtc())}'",
      List() => value.first,
      _ => value.toString(),
    };

    return valueSQL;
  }

  ({String sql, List? valuesOrdered, Map<String, dynamic>? valuesNamed}) build(
      {required String q, List<SQL>? executedSqls}) {
    executedSqls ??= [];

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
          var values = parameters.values.map(resolveValueAsSQL).join(' , ');

          sql = 'INSERT INTO $q$table$q ($columns) VALUES ($values)';
        }
      case SQLType.UPDATE:
        {
          if (parameters.isEmpty) {
            throw StateError(
                "Can't build UPDATE SQL with empty parameters: $this");
          }

          final where = this
              .where
              ?.build(q: q, variables: variables, executedSqls: executedSqls);
          if (where == null || where.isEmpty) {
            throw StateError("Can't build UPDATE SQL with empty WHERE: $this");
          }

          var set = parameters.entries
              .map((e) => '$q${e.key}$q = ${resolveValueAsSQL(e.value)}')
              .join(' , ');

          sql = 'UPDATE $q$table$q SET $set WHERE $where';
        }
      case SQLType.SELECT:
        {
          final where = this
              .where
              ?.build(q: q, variables: variables, executedSqls: executedSqls);

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

      if (SQL.isVariableValue(valSQL)) {
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

  @override
  String toString() {
    return 'SQL{sqlID: $sqlID, type: ${type.name}, table: $table, where: $where, parameters: $parameters, variables: $variables, returnLastID: $returnLastID, orderBy: $orderBy, lastID: $lastID}';
  }
}

abstract class SQLCondition {
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

  dynamic toJson();

  String build(
      {required String q,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls});
}

class SQLConditionGroup extends SQLCondition {
  final bool or;
  final List<SQLCondition> conditions;

  SQLConditionGroup(this.or, this.conditions);

  SQLConditionGroup.and(this.conditions) : or = false;

  SQLConditionGroup.or(this.conditions) : or = true;

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
      {required String q,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls}) {
    if (conditions.length == 1) {
      return conditions.first.build(
        q: q,
        variables: variables,
        executedSqls: executedSqls,
      );
    }

    var op = or ? ' OR ' : ' AND ';
    var conditionLine = conditions
        .map((e) => e.build(
              q: q,
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

  factory SQLConditionValue.fromJson(List json) => SQLConditionValue(
        json[0],
        json[1],
        json[2],
      );

  @override
  List toJson() => [field, op, value];

  @override
  String build(
      {required String q,
      Map<String, dynamic>? variables,
      List<SQL>? executedSqls}) {
    Object? value = this.value;

    if (value != null && SQL.isVariableValue(value)) {
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
