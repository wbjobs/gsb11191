import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  createCounterState,
  mergeOperation,
  nextOperation,
} from '../src/crdt.js';
import { CounterStore } from '../src/counter-store.js';

describe('counter CRDT', () => {
  it('converges after out-of-order delivery around a reset', () => {
    const actor = 'actor-a';
    const source = createCounterState();
    const first = nextOperation(source, actor, 'delta', { amount: 1 });
    mergeOperation(source, first);
    const second = nextOperation(source, actor, 'delta', { amount: 1 });
    mergeOperation(source, second);
    const reset = nextOperation(source, actor, 'reset');

    const received = createCounterState();
    for (const operation of [reset, second, first]) {
      mergeOperation(received, operation);
    }

    assert.equal(received.value, 0);
  });

  it('keeps increments concurrent with reset instead of dropping them', () => {
    const online = createCounterState();
    const earlier = nextOperation(online, 'actor-a', 'delta');
    mergeOperation(online, earlier);
    const reset = nextOperation(online, 'actor-a', 'reset');
    mergeOperation(online, reset);

    const offlineState = createCounterState();
    mergeOperation(offlineState, earlier);
    const concurrent = nextOperation(offlineState, 'actor-b', 'delta');

    mergeOperation(online, concurrent);
    assert.equal(online.value, 1);
  });
});

describe('CounterStore', () => {
  it('keeps all updates from four concurrent tabs', async () => {
    const mesh = new MeshTransportHub();
    const repository = new FakeRepository();
    const stores = await createStores(4, repository, mesh);

    await Promise.all(
      stores.flatMap((store) =>
        Array.from({ length: 100 }, () => store.increment()),
      ),
    );
    await mesh.drain();

    for (const store of stores) {
      assert.equal(store.value, 400);
    }
  });

  it('ends at zero after simultaneous resets', async () => {
    const mesh = new MeshTransportHub();
    const repository = new FakeRepository();
    const stores = await createStores(4, repository, mesh);

    await Promise.all(stores.map((store) => store.reset()));
    await mesh.drain();

    for (const store of stores) {
      assert.equal(store.value, 0);
    }
  });

  it('retains operations when a tab closes before a new tab opens', async () => {
    const mesh = new MeshTransportHub();
    const repository = new FakeRepository();
    const [store] = await createStores(1, repository, mesh);

    for (let index = 0; index < 3; index += 1) {
      await store.increment();
    }
    await store.decrement();
    store.stop();

    const reopenedMesh = new MeshTransportHub();
    const [reopened] = await createStores(1, repository, reopenedMesh, 10);
    assert.equal(reopened.value, 2);
  });

  it('merges operations made while broadcasts cannot be received', async () => {
    const mesh = new MeshTransportHub();
    const repository = new FakeRepository();
    const stores = await createStores(2, repository, mesh);

    mesh.setActive('tab-0', false);
    mesh.setActive('tab-1', false);
    await Promise.all([stores[0].increment(), stores[1].increment()]);
    mesh.setActive('tab-0', true);
    mesh.setActive('tab-1', true);

    await Promise.all(stores.map((store) => store.reconcile()));

    for (const store of stores) {
      assert.equal(store.value, 2);
    }
  });

  it('shows the same value after refresh and repeated messages', async () => {
    const mesh = new MeshTransportHub();
    const repository = new FakeRepository();
    const [store] = await createStores(1, repository, mesh);

    await store.increment();
    await store.reset();
    await store.increment();
    await mesh.drain();
    store.stop();

    const refreshedMesh = new MeshTransportHub();
    const [refreshed] = await createStores(1, repository, refreshedMesh, 10);
    const operations = await repository.load();
    for (const operation of operations) {
      await refreshed.receive(structuredClone(operation));
    }

    assert.equal(refreshed.value, 1);
  });
});

async function createStores(count, repository, mesh, startId = 0) {
  const stores = [];
  for (let index = 0; index < count; index += 1) {
    const actorId = `tab-${startId + index}`;
    const store = new CounterStore({
      actorId,
      repository,
      reconcileDelayMs: 60_000,
      transport: mesh.connect(actorId),
    });
    await store.init();
    stores.push(store);
  }
  return stores;
}

class FakeRepository {
  constructor() {
    this.operations = new Map();
  }

  async load() {
    return [...this.operations.values()].map((operation) => structuredClone(operation));
  }

  async put(operation) {
    this.operations.set(operation.id, structuredClone(operation));
  }
}

class MeshTransportHub {
  constructor() {
    this.transports = new Map();
    this.active = new Map();
    this.pending = 0;
  }

  connect(actorId) {
    const transport = new MeshTransport(actorId, this);
    this.transports.set(actorId, transport);
    this.active.set(actorId, true);
    return transport;
  }

  setActive(actorId, active) {
    this.active.set(actorId, active);
  }

  deliver(senderId, operation) {
    for (const [actorId, transport] of this.transports) {
      if (actorId !== senderId && this.active.get(actorId)) {
        this.pending += 1;
        setTimeout(() => {
          transport.listener?.(structuredClone(operation));
          this.pending -= 1;
        }, 0);
      }
    }
  }

  async drain() {
    await new Promise((resolve) => setTimeout(resolve, 0));
    while (this.pending > 0) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

class MeshTransport {
  constructor(actorId, hub) {
    this.actorId = actorId;
    this.hub = hub;
    this.listener = null;
  }

  subscribe(listener) {
    this.listener = listener;
  }

  send(operation) {
    if (this.hub.active.get(this.actorId)) {
      this.hub.deliver(this.actorId, operation);
    }
  }

  close() {
    this.listener = null;
    this.hub.transports.delete(this.actorId);
  }
}
