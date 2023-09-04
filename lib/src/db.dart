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

  /// The native connection.
  final T nativeConnection;

  DBConnection(this.nativeConnection);

  /// Starts a transaction.
  Future<bool> startTransaction();

  /// Rollback the transaction.
  Future<bool> rollbackTransaction();

  /// Commit the transaction.
  Future<bool> commitTransaction();

  /// Executes a [sql].
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

/// Base class for [DBConnection] providers.
abstract class DBConnectionProvider<C extends DBConnection> {
  static final Map<String, DBConnectionProviderInstantiator>
      _registeredProviders = {};

  /// Registers a [DBConnectionProvider].
  static void registerProvider(
      String type, DBConnectionProviderInstantiator providerInstantiator) {
    type = type.toLowerCase().trim();
    _registeredProviders[type] = providerInstantiator;
  }

  /// Gets a [DBConnectionProvider] for [type] with [credential].
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

  /// Returns a [DBConnection] to execute commands.
  FutureOr<C> getConnection();

  /// Releases a [DBConnection] to be reused.
  FutureOr<bool> releaseConnection(C connection);

  /// Closes the provider and it's connections.
  FutureOr<int> close();
}

/// A [DBConnection] credential.
class DBConnectionCredential {
  /// DB host/IP.
  final String host;

  /// DB port.
  final int port;

  /// DB username.
  final String user;

  /// DB password.
  final String pass;

  /// DB name/scheme.
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

/// A pool of [DBConnection]s.
class DBConnectionPoolProvider<C extends DBConnection>
    extends DBConnectionProvider<C> {
  /// The credential to create the connections.
  final DBConnectionCredential credential;
  /// The maximum number of connections waiting in the pool.
  final int maxConnections;

  /// The retry interval when trying to connect.
  final Duration retryInterval;
  /// The maximum number of retries before create a connection fails.
  final int maxRetries;

  /// The [Function] that creates new connections for the pool.
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

/// A single connection provider.
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
