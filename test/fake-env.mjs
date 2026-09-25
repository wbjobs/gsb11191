/**
 * 测试替身：
 * - createFakeIndexedDB：最小 IndexedDB 实现，多个连接共享同一 backing store，
 *   模拟同一 origin 下多标签页共享数据库；事务提交带随机延迟以制造交错。
 * - createTestBus：可控的 BroadcastChannel 总线，支持暂停/恢复投递、
 *   乱序（shuffle）、单通道隔离（模拟离线）、flush 等待全部投递完成。
 */

const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));

class FakeRequest {
  constructor() {
    this.onsuccess = null;
    this.onerror = null;
    this.result = undefined;
    this.error = null;
  }
  succeed(result) {
    this.result = result;
    queueMicrotask(() => this.onsuccess && this.onsuccess({ target: this }));
  }
  fail(error) {
    this.error = error;
    queueMicrotask(() => this.onerror && this.onerror({ target: this }));
  }
}

class FakeObjectStore {
  constructor(tx, name) {
    this.tx = tx;
    this.name = name;
  }
  _data() {
    const db = this.tx.conn.shared.dbs.get(this.tx.conn.dbName);
    const store = db.stores.get(this.name);
    if (!store) throw new Error(`object store not found: ${this.name}`);
    return store;
  }
  add(record) {
    const req = new FakeRequest();
    const data = this._data();
    if (data.has(record.id)) req.fail(new Error('ConstraintError'));
    else { data.set(record.id, structuredClone(record)); req.succeed(undefined); }
    return req;
  }
  getAll() {
    const req = new FakeRequest();
    req.succeed([...this._data().values()].map((r) => structuredClone(r)));
    return req;
  }
}

class FakeTransaction {
  constructor(conn, storeNames, mode) {
    this.conn = conn;
    this.storeNames = storeNames;
    this.mode = mode;
    this.oncomplete = null;
    this.onerror = null;
    this.onabort = null;
    // 随机延迟提交，制造跨标签页事务交错
    setTimeout(() => this.oncomplete && this.oncomplete(), Math.random() * 3);
  }
  objectStore(name) {
    return new FakeObjectStore(this, name);
  }
}

class FakeConnection {
  constructor(shared, dbName) {
    this.shared = shared;
    this.dbName = dbName;
  }
  createObjectStore(name) {
    this.shared.dbs.get(this.dbName).stores.set(name, new Map());
  }
  transaction(storeNames, mode) {
    return new FakeTransaction(this, storeNames, mode);
  }
  close() {}
}

export function createFakeIndexedDB() {
  const shared = { dbs: new Map() };
  return {
    shared,
    open(name, _version) {
      const req = new FakeRequest();
      queueMicrotask(() => {
        const isNew = !shared.dbs.has(name);
        if (isNew) shared.dbs.set(name, { stores: new Map() });
        const conn = new FakeConnection(shared, name);
        req.result = conn;
        if (isNew && req.onupgradeneeded) req.onupgradeneeded({ target: req });
        if (req.onsuccess) req.onsuccess({ target: req });
      });
      return req;
    },
  };
}

export function createTestBus() {
  const channels = new Set();
  const pending = [];
  let delivering = true;

  function deliverOne({ msg, from }) {
    for (const ch of channels) {
      if (ch !== from && !ch.isolated && !ch.closed && ch.onmessage) {
        ch.onmessage({ data: structuredClone(msg) });
      }
    }
  }

  function pump() {
    while (delivering && pending.length) deliverOne(pending.shift());
  }

  return {
    createChannel() {
      const ch = {
        closed: false,
        isolated: false,
        onmessage: null,
        postMessage(msg) {
          if (this.closed || this.isolated) return; // 隔离时发不出去（离线）
          pending.push({ msg, from: ch });
          setTimeout(pump, 0);
        },
        close() {
          this.closed = true;
          channels.delete(ch);
        },
      };
      channels.add(ch);
      return ch;
    },
    setDelivering(v) { delivering = v; if (v) pump(); },
    shufflePending(rand = Math.random) {
      for (let i = pending.length - 1; i > 0; i--) {
        const j = Math.floor(rand() * (i + 1));
        [pending[i], pending[j]] = [pending[j], pending[i]];
      }
    },
    isolate(ch) { ch.isolated = true; },
    heal(ch) { ch.isolated = false; },
    async flush() {
      for (let i = 0; i < 20 && pending.length; i++) { pump(); await tick(5); }
      await tick(10); // 等所有 setTimeout(pump) 与事务提交完成
    },
    get pendingCount() { return pending.length; },
  };
}
