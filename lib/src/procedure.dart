import 'package:apollovm/apollovm.dart';
import 'package:sql_commander/src/sql.dart';

import 'command.dart';
import 'json_helper.dart' as json_helper;

/// Base class for procedures with [dbCommands].
/// See [ProcedureDart], [ProcedureJava] and [ProcedureApolloVM],
abstract class Procedure extends DBCommandSet {
  /// The language of the procedure code.
  final String language;

  /// The entrypoint class name.
  final String? className;

  /// The entrypoint function/method name.
  final String name;

  /// The procedure code.
  final String code;

  /// The positional parameters to be [call]ed when [execute] is invoked.
  final List<dynamic>? positionalParameters;

  /// The named parameters to be [call]ed when [execute] is invoked.
  final Map<String, dynamic>? namedParameters;

  /// The properties to use when executing.
  /// It's also passed to [DBCommand] or [SQL] when executing one of them.
  final Map<String, dynamic>? properties;

  Procedure.base({
    required this.language,
    this.className,
    required this.name,
    required this.code,
    this.positionalParameters,
    this.namedParameters,
    this.properties,
    super.dbCommands,
    super.logInfo,
    super.logError,
  });

  dynamic getProperty(String key) => properties?[key];

  CommandLogInfo? printFunction;

  FutureOr<bool> load();

  FutureOr<bool> ensureLoaded() {
    var loaded = load();

    if (loaded is Future<bool>) {
      return loaded.then(_checkLoaded);
    }

    return _checkLoaded(loaded);
  }

  bool _checkLoaded(bool loaded) {
    if (!loaded) {
      throw StateError("Error loading `CommandFunction`: $this");
    }
    return true;
  }

  FutureOr<R> call<R>(
      {List<dynamic>? positionalParameters,
      Map<String, dynamic>? namedParameters});

  FutureOr<R> execute<R>() {
    var loaded = ensureLoaded();

    if (loaded is Future<bool>) {
      return loaded.then((_) => _executeImpl<R>());
    }

    return _executeImpl<R>();
  }

  FutureOr<R> _executeImpl<R>() {
    var loaded = ensureLoaded();

    if (loaded is Future<bool>) {
      return loaded.then((value) {
        return call(
            positionalParameters: positionalParameters,
            namedParameters: namedParameters);
      });
    }

    return call(
        positionalParameters: positionalParameters,
        namedParameters: namedParameters);
  }

  @override
  Map<String, dynamic> toJson() => {
        "language": language,
        if (className != null) "className": className,
        "name": name,
        "code": code,
        "positionalParameters": json_helper.toJson(positionalParameters),
        "namedParameters": json_helper.toJson(namedParameters),
        "properties": json_helper.toJson(properties),
        "dbCommands": dbCommands.map((e) => e.toJson()).toList(),
      };

  factory Procedure.fromJson(Map<String, dynamic> json) {
    var language = json["language"] as String;

    if (language == 'dart') {
      return ProcedureDart.fromJson(json);
    } else if (language == 'java') {
      return ProcedureJava.fromJson(json);
    } else {
      return ProcedureApolloVM.fromJson(json);
    }
  }
}

class ProcedureApolloVM extends Procedure {
  ProcedureApolloVM({
    required super.language,
    super.className,
    required super.name,
    super.positionalParameters,
    super.namedParameters,
    super.properties,
    required super.code,
    super.dbCommands,
    super.logInfo,
    super.logError,
  }) : super.base();

  ApolloVM? _vm;

  @override
  Future<bool> load() async {
    _vm ??= await _loadVM();
    return true;
  }

  Future<ApolloVM> _loadVM() async {
    var vm = ApolloVM();

    var codeUnit = CodeUnit(
        language, code, 'sql_commander:ProcedureApolloVM/$language/$name');

    var loadOK = await vm.loadCodeUnit(codeUnit);
    if (!loadOK) {
      throw StateError('Error parsing `$language` code!');
    }

    return vm;
  }

  @override
  Future<R> call<R>({
    List<dynamic>? positionalParameters,
    Map<String, dynamic>? namedParameters,
    CommandLogInfo? logInfo,
    CommandLogError? logError,
  }) async {
    logInfo ??= this.logInfo;
    logError ??= this.logError;

    await ensureLoaded();

    var vm = _vm!;

    var runner = createRunner(vm);

    final className = this.className;

    ASTValue astValue;

    if (className != null) {
      astValue = await runner.executeClassMethod('', className, name,
          positionalParameters: positionalParameters,
          namedParameters: namedParameters);
    } else {
      astValue = await runner.executeFunction('', name,
          positionalParameters: positionalParameters,
          namedParameters: namedParameters,
          allowClassMethod: true);
    }

    var value = astValue.getValueNoContext();
    return value as R;
  }

  ApolloLanguageRunner createRunner(ApolloVM vm) {
    var runner = vm.createRunner(language)!;

    runner.externalPrintFunction = _printFunctionMapper;

    mapExternalFunctions(runner, runner.externalFunctionMapper!);
    return runner;
  }

  FutureOr<void> _printFunctionMapper(Object? o) {
    final printFunction = this.printFunction ?? (m) => print(m);

    String s;

    if (o == null) {
      s = 'null';
    } else if (o is ASTValue) {
      var val = o.getValueNoContext();

      if (val is Future) {
        return val.then((val) => printFunction(val?.toString() ?? 'null'));
      } else {
        s = val?.toString() ?? 'null';
      }
    } else {
      s = o.toString();
    }

    printFunction(s);
  }

  void mapExternalFunctions(ApolloLanguageRunner runner,
      ApolloExternalFunctionMapper externalFunctionMapper) {
    externalFunctionMapper.mapExternalFunction1(ASTTypeDynamic.instance,
        'getProperty', ASTTypeString.instance, 'key', getProperty);

    externalFunctionMapper.mapExternalFunction1(
        ASTTypeBool.instance,
        'executeDBCommandByID',
        ASTTypeString.instance,
        'id',
        (id) => executeDBCommandByID(id, properties: properties));

    externalFunctionMapper.mapExternalFunction1(
        ASTTypeBool.instance,
        'executeSQLByID',
        ASTTypeString.instance,
        'sqlID',
        (sqlID) => executeSQLByID(sqlID, properties: properties));

    externalFunctionMapper.mapExternalFunction1(
        ASTTypeBool.instance,
        'executeSQLsByIDs',
        ASTTypeArray<ASTTypeString, String>(ASTTypeString.instance),
        'sqlIDs',
        (sqlIDs) => executeSQLsByIDs(sqlIDs, properties: properties));

    externalFunctionMapper.mapExternalFunction1(ASTTypeArray.instanceOfDynamic,
        'getSQLResults', ASTTypeString.instance, 'sqlID', getSQLResults);

    externalFunctionMapper.mapExternalFunction1(ASTTypeDynamic.instance,
        'getSQLResult', ASTTypeString.instance, 'sqlID', getSQLResult);

    externalFunctionMapper.mapExternalFunction2(
        ASTTypeArray.instanceOfDynamic,
        'getSQLResultsColumn',
        ASTTypeString.instance,
        'sqlID',
        ASTTypeString.instance,
        'column',
        getSQLResultsColumn);

    externalFunctionMapper.mapExternalFunction2(
        ASTTypeDynamic.instance,
        'getSQLResultColumn',
        ASTTypeString.instance,
        'sqlID',
        ASTTypeString.instance,
        'column',
        getSQLResultColumn);
  }

  factory ProcedureApolloVM.fromJson(Map<String, dynamic> json) =>
      ProcedureApolloVM(
        language: json["language"] as String,
        className: json["className"] as String?,
        name: json["name"] as String,
        code: json["code"] as String,
        positionalParameters:
            json_helper.fromJsonList<dynamic>(json["positionalParameters"]),
        namedParameters:
            json_helper.fromJsonMap<String, dynamic>(json["namedParameters"]),
        properties:
            json_helper.fromJsonMap<String, dynamic>(json["properties"]),
        dbCommands: (json["dbCommands"] as List?)?.toListOfDBCommandFromJson(),
      );
}

class ProcedureDart extends ProcedureApolloVM {
  ProcedureDart({
    super.className,
    required super.name,
    super.positionalParameters,
    super.namedParameters,
    super.properties,
    required super.code,
    super.dbCommands,
    super.logInfo,
    super.logError,
  }) : super(language: 'dart');

  factory ProcedureDart.fromJson(Map<String, dynamic> json) => ProcedureDart(
        className: json["className"] as String?,
        name: json["name"] as String,
        code: json["code"] as String,
        positionalParameters:
            json_helper.fromJsonList<dynamic>(json["positionalParameters"]),
        namedParameters:
            json_helper.fromJsonMap<String, dynamic>(json["namedParameters"]),
        properties:
            json_helper.fromJsonMap<String, dynamic>(json["properties"]),
        dbCommands: (json["dbCommands"] as List?)?.toListOfDBCommandFromJson(),
      );
}

class ProcedureJava extends ProcedureApolloVM {
  ProcedureJava({
    super.className,
    required super.name,
    super.positionalParameters,
    super.namedParameters,
    super.properties,
    required super.code,
    super.dbCommands,
    super.logInfo,
    super.logError,
  }) : super(language: 'java');

  factory ProcedureJava.fromJson(Map<String, dynamic> json) => ProcedureJava(
        className: json["className"] as String?,
        name: json["name"] as String,
        code: json["code"] as String,
        positionalParameters:
            json_helper.fromJsonList<dynamic>(json["positionalParameters"]),
        namedParameters:
            json_helper.fromJsonMap<String, dynamic>(json["namedParameters"]),
        properties:
            json_helper.fromJsonMap<String, dynamic>(json["properties"]),
        dbCommands: (json["dbCommands"] as List?)?.toListOfDBCommandFromJson(),
      );
}

extension IterableProcedureFromJsonExtension on Iterable {
  List<Procedure> toListOfProcedureFromJson() =>
      whereJsonMap().map(Procedure.fromJson).toList();
}
