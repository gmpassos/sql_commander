import 'dart:async';
import 'dart:collection';

import 'sql.dart';

class DB<C extends DBConnection> {
  final DBConnectionProvider<C> connectionProvider;

  DB(this.connectionProvider);

  FutureOr<void> close() => connectionProvider.close();

  Future<bool> startTransaction({C? connection}) async {
    connection ??= await connectionProvider.getConnection();
    return connection.startTransaction();
  }

  Future<bool> rollbackTransaction({C? connection}) async {
    connection ??= await connectionProvider.getConnection();
    return connection.rollbackTransaction();
  }

  Future<bool> commitTransaction({C? connection}) async {
    connection ??= await connectionProvider.getConnection();
    return connection.commitTransaction();
  }

  Future<({List<Map<String?, dynamic>>? results, Object? lastID})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls,
      C? connection}) async {
    connection ??= await connectionProvider.getConnection();

    var r = await connection.executeSQL(sql, executedSqls: executedSqls);

    sql.executed = true;

    if (r != null) {
      sql.results = r.results;
      sql.lastID = r.lastID;
    }

    return r;
  }
}

abstract class DBConnection<T> {
  static Future<C?> openConnection<C extends DBConnection>(
      DBConnectionCredential credential,
      Future<C?> Function(DBConnectionCredential credential) opener,
      {Duration retryInterval = const Duration(seconds: 1),
      int maxRetries = 10}) async {
    var retryCount = 0;

    while (retryCount < maxRetries) {
      try {
        var conn = await opener(credential);
        return conn;
      } catch (_) {
        await Future.delayed(retryInterval);
      }

      ++retryCount;
    }

    return null;
  }

  final T nativeConnection;

  DBConnection(this.nativeConnection);

  Future<bool> startTransaction();

  Future<bool> rollbackTransaction();

  Future<bool> commitTransaction();

  Future<({List<Map<String, dynamic>>? results, Object? lastID})?> executeSQL(
      SQL sql,
      {List<SQL>? executedSqls});

  Future<void> close();
}

typedef DBConnectionProviderInstantiator
    = FutureOr<DBConnectionProvider?> Function(
        DBConnectionCredential credential,
        {Duration? retryInterval,
        int? maxRetries,
        int? maxConnections});

abstract class DBConnectionProvider<C extends DBConnection> {
  static final Map<String, DBConnectionProviderInstantiator>
      _registeredProviders = {};

  static void registerProvider(
      String type, DBConnectionProviderInstantiator providerInstantiator) {
    type = type.toLowerCase().trim();
    _registeredProviders[type] = providerInstantiator;
  }

  static FutureOr<DBConnectionProvider?> getProvider(
      String type, DBConnectionCredential credential,
      {Duration retryInterval = const Duration(seconds: 1),
      int maxRetries = 10,
      int maxConnections = 1}) {
    type = type.toLowerCase().trim();

    var providerInstantiator = _registeredProviders[type];
    if (providerInstantiator == null) return null;

    var provider = providerInstantiator(credential,
        retryInterval: retryInterval,
        maxRetries: maxRetries,
        maxConnections: maxConnections);

    return provider;
  }

  FutureOr<C> getConnection();

  FutureOr<bool> releaseConnection(C connection);

  FutureOr<int> close();
}

class DBConnectionCredential {
  final String host;
  final int port;
  final String user;
  final String pass;
  final String db;

  const DBConnectionCredential(
      this.host, this.port, this.user, this.pass, this.db);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DBConnectionCredential &&
          runtimeType == other.runtimeType &&
          host == other.host &&
          port == other.port &&
          user == other.user &&
          db == other.db;

  @override
  int get hashCode =>
      host.hashCode ^ port.hashCode ^ user.hashCode ^ db.hashCode;

  @override
  String toString() {
    return 'DBConnectionCredential{host: $host, port: $port, user: $user, db: $db}';
  }
}

typedef DBConnectionInstantiator<C extends DBConnection>
    = FutureOr<C?> Function(DBConnectionCredential credential,
        {Duration? retryInterval, int? maxRetries});

class DBConnectionPoolProvider<C extends DBConnection>
    extends DBConnectionProvider<C> {
  final DBConnectionCredential credential;
  final int maxConnections;

  final Duration retryInterval;
  final int maxRetries;

  final DBConnectionInstantiator<C> connectionInstantiator;

  DBConnectionPoolProvider(
    this.credential,
    this.connectionInstantiator, {
    int maxConnections = 1,
    this.retryInterval = const Duration(seconds: 1),
    this.maxRetries = 3,
  }) : maxConnections = maxConnections.clamp(1, 100);

  final ListQueue<C> _pool = ListQueue();

  @override
  FutureOr<int> close() async {
    var total = _pool.length;
    for (var c in _pool) {
      await c.close();
    }
    _pool.clear();
    return total;
  }

  @override
  FutureOr<C> getConnection() {
    if (_pool.isEmpty) {
      return createConnection();
    }
    var c = _pool.removeFirst();
    return c;
  }

  @override
  FutureOr<bool> releaseConnection(C connection) {
    if (_pool.length < maxConnections) {
      _pool.add(connection);
      return true;
    }
    return false;
  }

  FutureOr<C> createConnection() async {
    var conn = await connectionInstantiator(credential,
        maxRetries: maxRetries, retryInterval: retryInterval);

    if (conn == null) {
      throw StateError("Can't create connection for $credential");
    }

    return conn;
  }
}

class DBSingleConnectionProvider<C extends DBConnection>
    extends DBConnectionProvider<C> {
  final C connection;

  DBSingleConnectionProvider(this.connection);

  @override
  FutureOr<C> getConnection() => connection;

  @override
  FutureOr<bool> releaseConnection(C connection) {
    if (connection != this.connection) {
      return false;
    }
    return true;
  }

  @override
  FutureOr<int> close() => connection.close().then((_) => 1);
}
