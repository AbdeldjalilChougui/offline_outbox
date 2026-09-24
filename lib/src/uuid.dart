import 'dart:math';

final Random _secure = Random.secure();

/// Generates a random (version 4) UUID as defined by RFC 4122 / RFC 9562.
///
/// Uses [random] when given, which makes the output reproducible in tests;
/// otherwise a cryptographically secure [Random.secure] source.
String generateUuidV4([Random? random]) {
  final r = random ?? _secure;
  final bytes = List<int>.generate(16, (_) => r.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
  final hex = [for (final b in bytes) b.toRadixString(16).padLeft(2, '0')];
  return '${hex.sublist(0, 4).join()}-${hex.sublist(4, 6).join()}-'
      '${hex.sublist(6, 8).join()}-${hex.sublist(8, 10).join()}-'
      '${hex.sublist(10).join()}';
}
