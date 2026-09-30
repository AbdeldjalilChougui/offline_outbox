import 'dart:math';

import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

void main() {
  final v4 = RegExp(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
  );

  test('generates RFC 4122 version 4 UUIDs', () {
    for (var i = 0; i < 200; i++) {
      expect(generateUuidV4(), matches(v4));
    }
  });

  test('is deterministic for a seeded Random', () {
    expect(generateUuidV4(Random(7)), generateUuidV4(Random(7)));
    expect(generateUuidV4(Random(7)), isNot(generateUuidV4(Random(8))));
  });

  test('does not repeat across many calls', () {
    final seen = {for (var i = 0; i < 5000; i++) generateUuidV4()};
    expect(seen, hasLength(5000));
  });
}
