import 'dart:convert' as dart_convert;
import 'dart:typed_data';

import 'package:intl/date_symbol_data_local.dart';
import 'package:intl/intl.dart';
import 'package:swiss_knife/swiss_knife.dart';

dynamic toJson(Object? o) {
  if (o == null) return null;

  if (o is String || o is num || o is bool) {
    return o;
  }

  if (o is DateTime) {
    var s = formatDateTime(o);
    return "data:object;<DateTime>,$s";
  }

  if (o is Uint8List) {
    var s = dart_convert.base64.encode(o);
    return "data:application/octet-stream;base64,$s";
  }

  if (o is Map) {
    return o.map((k, v) => MapEntry('$k', toJson(v)));
  }

  if (o is List) {
    return o.map(toJson).toList();
  }

  return o;
}

dynamic fromJson(Object? o) {
  if (o == null) return null;

  if (o is num || o is bool) {
    return o;
  }

  if (o is Map) {
    return o.map((k, v) => MapEntry('$k', fromJson(v)));
  }

  if (o is List) {
    return o.map(fromJson).toList();
  }

  if (o is String) {
    if (o.startsWith("data:")) {
      if (o.startsWith("data:object;<DateTime>,")) {
        var data = o.substring(23);
        return parseDateTime(data);
      }

      var idx = o.indexOf(';base64,');

      if (idx >= 5) {
        var base64Data = o.substring(idx + 8);
        var bytes = dart_convert.base64.decode(base64Data);
        return bytes;
      }
    }

    return o;
  }
}

List<V>? fromJsonList<V>(List? l) {
  if (l == null) return null;

  var valueMapper = _typeMapper<V>();

  return l.map((v) {
    var value = valueMapper(v) as V;
    return value;
  }).toList();
}

Map<K, V>? fromJsonMap<K, V>(Map? m) {
  if (m == null) return null;

  var keyMapper = _typeMapper<K>();
  var valueMapper = _typeMapper<V>();

  return m.map((k, v) {
    var key = keyMapper(k) as K;
    var value = valueMapper(v) as V;
    return MapEntry<K, V>(key, value);
  });
}

T? Function(Object? o) _typeMapper<T>() {
  if (T == String) {
    return (o) => o?.toString() as T?;
  }

  if (T == int) {
    return (o) => parseInt(o) as T?;
  }

  if (T == double) {
    return (o) => parseDouble(o) as T?;
  }

  if (T == num) {
    return (o) => parseNum(o) as T?;
  }

  if (T == DateTime) {
    return (o) => o == null ? null : parseDateTime(o) as T?;
  }

  return (o) => fromJson(o) as T?;
}

final sqlDateTimeFormat = DateFormat('yyyy-MM-dd HH:mm:ss');

DateTime parseDateTime(Object o, {bool utc = true}) {
  if (o is DateTime) return o;

  var s = o.toString();

  try {
    return sqlDateTimeFormat.parse(s.trim(), utc);
  } catch (_) {
    _initializeDateFormatting();
    return sqlDateTimeFormat.parse(s.trim(), utc);
  }
}

String formatDateTime(DateTime d, {bool utc = true}) {
  if (utc) {
    d = d.toUtc();
  }

  try {
    return sqlDateTimeFormat.format(d);
  } catch (_) {
    _initializeDateFormatting();
    return sqlDateTimeFormat.format(d);
  }
}

bool _initializeDateFormattingCall = false;

void _initializeDateFormatting() {
  if (_initializeDateFormattingCall) return;
  _initializeDateFormattingCall = true;

  initializeDateFormatting('en');
}

extension JsonMapExtension on Map {
  Map<String, dynamic> toJsonMap() => map((k, v) => MapEntry('$k', v));
}

extension JsonListExtension on Iterable {
  Iterable<Map<String, dynamic>> whereJsonMap() =>
      whereType<Map>().map((e) => e.toJsonMap());
}
