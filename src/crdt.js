const DELTA = 'delta';
const RESET = 'reset';

export function createCounterState() {
  return {
    clock: {},
    deltas: new Map(),
    resets: new Map(),
    value: 0,
  };
}

export function nextOperation(state, actorId, type, options = {}) {
  if (!actorId || typeof actorId !== 'string') {
    throw new TypeError('actorId is required');
  }

  const sequence = (state.clock[actorId] || 0) + 1;
  const operation = {
    id: options.id || createOperationId(),
    actor: actorId,
    seq: sequence,
    type,
    at: options.at ?? Date.now(),
  };

  if (type === DELTA) {
    operation.amount = options.amount ?? 1;
  } else if (type === RESET) {
    operation.clock = {
      ...state.clock,
      [actorId]: sequence,
    };
  } else {
    throw new TypeError(`Unsupported operation type: ${type}`);
  }

  return operation;
}

export function mergeOperation(state, operation) {
  validateOperation(operation);

  const existing =
    state.deltas.get(operation.id) || state.resets.get(operation.id);
  if (existing) {
    if (!sameOperation(existing, operation)) {
      throw new Error(`Operation id conflict: ${operation.id}`);
    }
    return state;
  }

  if (operation.type === DELTA) {
    state.deltas.set(operation.id, operation);
    state.clock[operation.actor] = Math.max(
      state.clock[operation.actor] || 0,
      operation.seq,
    );
  } else {
    state.resets.set(operation.id, operation);
    mergeClockInto(state.clock, operation.clock);
  }

  state.value = computeValue(state.deltas, state.resets);
  return state;
}

export function mergeOperations(state, operations) {
  for (const operation of operations) {
    mergeOperation(state, operation);
  }
  return state;
}

export function listOperations(state) {
  return [...state.deltas.values(), ...state.resets.values()].sort(compareOperations);
}

export function isDeltaBeforeReset(delta, reset) {
  return (reset.clock[delta.actor] || 0) >= delta.seq;
}

function computeValue(deltas, resets) {
  let value = 0;

  for (const delta of deltas.values()) {
    let visible = true;
    for (const reset of resets.values()) {
      if (isDeltaBeforeReset(delta, reset)) {
        visible = false;
        break;
      }
    }
    if (visible) {
      value += delta.amount;
    }
  }

  return value;
}

function mergeClockInto(target, source) {
  for (const [actor, sequence] of Object.entries(source)) {
    target[actor] = Math.max(target[actor] || 0, sequence);
  }
}

function compareOperations(left, right) {
  if (left.actor !== right.actor) {
    return left.actor < right.actor ? -1 : 1;
  }
  return left.seq - right.seq;
}

function sameOperation(left, right) {
  return JSON.stringify(structuredCloneLike(left)) ===
    JSON.stringify(structuredCloneLike(right));
}

function structuredCloneLike(value) {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(structuredCloneLike);
  }
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, structuredCloneLike(value[key])]),
  );
}

function validateOperation(operation) {
  if (!operation || typeof operation !== 'object') {
    throw new TypeError('Operation must be an object');
  }
  if (typeof operation.id !== 'string' || operation.id.length === 0) {
    throw new TypeError('Operation id is required');
  }
  if (typeof operation.actor !== 'string' || operation.actor.length === 0) {
    throw new TypeError('Operation actor is required');
  }
  if (!Number.isSafeInteger(operation.seq) || operation.seq < 1) {
    throw new TypeError('Operation seq must be a positive safe integer');
  }

  if (operation.type === DELTA) {
    if (!Number.isSafeInteger(operation.amount) || operation.amount === 0) {
      throw new TypeError('Delta amount must be a non-zero safe integer');
    }
    return;
  }

  if (operation.type === RESET) {
    if (!operation.clock || typeof operation.clock !== 'object') {
      throw new TypeError('Reset clock is required');
    }
    for (const sequence of Object.values(operation.clock)) {
      if (!Number.isSafeInteger(sequence) || sequence < 1) {
        throw new TypeError('Reset clock contains an invalid sequence');
      }
    }
    return;
  }

  throw new TypeError(`Unsupported operation type: ${operation.type}`);
}

export function createOperationId() {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
