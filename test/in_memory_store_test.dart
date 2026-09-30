import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

import 'store_contract.dart';

void main() {
  group('InMemoryOutboxStore', () {
    storeContract(() async => InMemoryOutboxStore());

    test('can be seeded, e.g. to simulate a previous run', () async {
      final store = InMemoryOutboxStore([entry('x', seq: 1)]);
      expect((await store.loadAll()).single.id, 'x');
    });
  });
}
