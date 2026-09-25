/**
 * 跨标签页共享计数器（BroadcastChannel + IndexedDB）
 *
 * 设计：事件溯源（op log）
 * - 每个操作生成一条 op：{ id, tabId, type: 'inc'|'dec'|'reset', lamport, ts }
 * - op 先持久化到 IndexedDB，再经 BroadcastChannel 广播 -> 关标签页/崩溃不丢
 * - 计数是对 op 集合求值的纯函数：取 (lamport, id) 最大的 reset 为界，
 *   其后的 inc/dec 求和。inc/dec 可交换、reset 用 Lamport 定序，
 *   因此并发、乱序、重复投递都不影响最终结果（最终一致）。
 * - 周期性 + 事件驱动（online/focus/visible）从 IndexedDB 全量 resync，
 *   任何丢失的广播都能自愈（离线恢复）。
 */

const DB_NAME = 'shared-counter';
const DB_VERSION = 1;
const STORE = 'ops';

function randomId() {
  if (globalThis.crypto && crypto.randomUUID) return crypto.randomUUID();
  return `${Date.now()}:${Math.random().toString(36).slice(2)}`;
}

// 全序：先比 lamport，再比 id（保证所有标签页对同一批 op 得到相同排序）
function compareOp(a, b) {
  if (a.lamport !== b.lamport) return a.lamport - b.lamport;
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

export async function createCounter(options = {}) {
  const {
    tabId = randomId(),
    channelName = 'shared-counter-channel',
    idbFactory = globalThis.indexedDB,
    channelFactory = (name) => new BroadcastChannel(name),
    windowLike = typeof window !== 'undefined' ? window : null,
    resyncIntervalMs = 3000,
    onChange = () => {},
  } = options;

  if (!idbFactory) throw new Error('indexedDB is not available');

  const ops = new Map(); // opId -> op（本地已知的全部 op）
  let lamport = 0;
  let value = 0;
  let closed = false;

  const db = await openDb();
  const channel = channelFactory(channelName);

  function openDb() {
    return new Promise((resolve, reject) => {
      const req = idbFactory.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        req.result.createObjectStore(STORE, { keyPath: 'id' });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function txDone(tx) {
    return new Promise((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
  }

  async function persist(op) {
    const tx = db.transaction(STORE, 'readwrite');
    tx.objectStore(STORE).add(op);
    await txDone(tx);
  }

  function loadAll() {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readonly');
      const req = tx.objectStore(STORE).getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  // 计数 = 最近一次 reset（按 (lamport, id) 全序取最大）之后所有 inc/dec 的和
  function recompute() {
    let winningReset = null;
    for (const op of ops.values()) {
      if (op.type === 'reset' && (!winningReset || compareOp(op, winningReset) > 0)) {
        winningReset = op;
      }
    }
    let sum = 0;
    for (const op of ops.values()) {
      if (op.type === 'reset') continue;
      if (winningReset && compareOp(op, winningReset) <= 0) continue;
      sum += op.type === 'inc' ? 1 : -1;
    }
    return sum;
  }

  function mergeOps(list) {
    let changed = false;
    for (const op of list) {
      if (!op || ops.has(op.id)) continue; // 幂等：重复投递安全
      ops.set(op.id, op);
      if (op.lamport > lamport) lamport = op.lamport;
      changed = true;
    }
    if (changed) {
      value = recompute();
      onChange(value);
    }
  }

  // 从 IndexedDB 全量重放，兜底一切丢消息场景（离线恢复、漏广播等）
  async function resync() {
    if (closed) return;
    mergeOps(await loadAll());
  }

  async function apply(type) {
    if (closed) throw new Error('counter is closed');
    lamport += 1;
    const op = { id: `${tabId}:${lamport}`, tabId, type, lamport, ts: Date.now() };
    await persist(op);       // 1. 先落盘：关标签页/崩溃不丢
    mergeOps([op]);          // 2. 本地生效
    channel.postMessage(op); // 3. 再广播给其他标签页
  }

  channel.onmessage = (event) => mergeOps([event.data]);

  // 初始加载（刷新/重开标签页后恢复一致状态）
  mergeOps(await loadAll());

  // 事件驱动 + 周期性 resync
  const unlisteners = [];
  function listen(target, event, fn) {
    if (target && target.addEventListener) {
      target.addEventListener(event, fn);
      unlisteners.push(() => target.removeEventListener(event, fn));
    }
  }
  listen(windowLike, 'online', resync);
  listen(windowLike, 'focus', resync);
  const doc = windowLike && windowLike.document;
  listen(doc, 'visibilitychange', () => {
    if (doc.visibilityState === 'visible') resync();
  });
  const timer = resyncIntervalMs > 0 ? setInterval(resync, resyncIntervalMs) : null;
  if (timer && timer.unref) timer.unref();

  return {
    get value() { return value; },
    get tabId() { return tabId; },
    increment: () => apply('inc'),
    decrement: () => apply('dec'),
    reset: () => apply('reset'),
    resync,
    async close() {
      closed = true;
      if (timer) clearInterval(timer);
      unlisteners.forEach((off) => off());
      channel.close();
      db.close();
    },
  };
}
