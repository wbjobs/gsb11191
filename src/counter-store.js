import {
  createCounterState,
  listOperations,
  mergeOperation,
  mergeOperations,
  nextOperation,
} from './crdt.js';
const RECONCILE_DELAY_MS = 1000;

export class CounterStore {
  constructor({ repository, transport, actorId = createActorId(), reconcileDelayMs = RECONCILE_DELAY_MS }) {
    this.repository = repository;
    this.transport = transport;
    this.actorId = actorId;
    this.reconcileDelayMs = reconcileDelayMs;
    this.state = createCounterState();
    this.listeners = new Set();
    this.queue = Promise.resolve();
    this.reconcileHandle = 0;
    this.stopped = false;
  }

  async init() {
    const operations = await this.repository.load();
    mergeOperations(this.state, operations);
    this.transport.subscribe((operation) => this.receive(operation));
    this.startReconciliation();
    this.emitChange();
  }

  get value() {
    return this.state.value;
  }

  subscribe(listener) {
    this.listeners.add(listener);
    listener(this.state.value);
    return () => this.listeners.delete(listener);
  }

  increment() {
    return this.submit('delta', { amount: 1 });
  }

  decrement() {
    return this.submit('delta', { amount: -1 });
  }

  reset() {
    return this.submit('reset');
  }

  stop() {
    this.stopped = true;
    clearInterval(this.reconcileHandle);
    this.transport.close?.();
  }

  submit(type, options = {}) {
    const task = this.queue.then(async () => {
      const operation = nextOperation(this.state, this.actorId, type, options);
      await this.repository.put(operation);
      mergeOperation(this.state, operation);
      this.transport.send(operation);
      this.emitChange();
    });
    this.queue = task.catch(() => {});
    return task;
  }

  receive(operation) {
    const task = this.queue.then(async () => {
      if (this.hasOperation(operation.id)) {
        return;
      }
      await this.repository.put(operation);
      mergeOperation(this.state, operation);
      this.emitChange();
    });
    this.queue = task.catch(() => {});
    return task;
  }

  reconcile = async () => {
    if (this.stopped) {
      return;
    }

    await this.queue;
    try {
      const operations = await this.repository.load();
      const previous = listOperations(this.state);
      mergeOperations(this.state, operations);
      if (previous.length !== listOperations(this.state).length) {
        this.emitChange();
      }
    } catch (error) {
      console.error('Counter reconciliation failed', error);
    }
  };

  startReconciliation() {
    this.reconcileHandle = setInterval(this.reconcile, this.reconcileDelayMs);
    this.reconcileHandle.unref?.();
  }

  hasOperation(id) {
    return this.state.deltas.has(id) || this.state.resets.has(id);
  }

  emitChange() {
    for (const listener of this.listeners) {
      listener(this.state.value);
    }
  }
}

function createActorId() {
  const storageKey = 'shared-counter-actor-id';
  try {
    let actorId = sessionStorage.getItem(storageKey);
    if (!actorId) {
      actorId = crypto.randomUUID();
      sessionStorage.setItem(storageKey, actorId);
    }
    return actorId;
  } catch {
    return crypto.randomUUID();
  }
}
