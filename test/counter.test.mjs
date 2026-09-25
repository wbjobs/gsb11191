import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCounter } from '../counter.js';
import { createFakeIndexedDB, createTestBus } from './fake-env.mjs';

function makeEnv() {
  const idb = createFakeIndexedDB();
  const bus = createTestBus();
  return { idb, bus };
}

async function makeTab(env, opts = {}) {
  let channel;
  const counter = await createCounter({
    idbFactory: env.idb,
    channelFactory: () => (channel = env.bus.createChannel()),
    windowLike: null,
    resyncIntervalMs: 0, // 测试中手动 resync，保证确定性
    ...opts,
  });
  return { counter, channel };
}

async function settle(env, tabs) {
  await env.bus.flush();
  await Promise.all(tabs.map((t) => t.counter.resync()));
}

function assertConsistent(tabs, expected) {
  for (const t of tabs) assert.equal(t.counter.value, expected);
}

test('4 个标签页同时各加 100 次，最终计数 = 400 且一致', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 4 }, () => makeTab(env)));

  // 不逐个 await，全部并发发出，制造真实交错
  await Promise.all(
    tabs.flatMap((t) => Array.from({ length: 100 }, () => t.counter.increment())),
  );

  await settle(env, tabs);
  assertConsistent(tabs, 400);
  await Promise.all(tabs.map((t) => t.counter.close()));
});

test('同时重置后最终为 0，且重置后仍可继续累加', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 4 }, () => makeTab(env)));

  await Promise.all(tabs.flatMap((t) => Array.from({ length: 50 }, () => t.counter.increment())));
  await settle(env, tabs);
  assertConsistent(tabs, 200);

  // 4 个标签页同时 reset
  await Promise.all(tabs.map((t) => t.counter.reset()));
  await settle(env, tabs);
  assertConsistent(tabs, 0);

  // 重置后并发加减继续正确
  await Promise.all(tabs.flatMap((t, i) => [
    ...Array.from({ length: 10 }, () => t.counter.increment()),
    ...Array.from({ length: i }, () => t.counter.decrement()),
  ]));
  await settle(env, tabs);
  assertConsistent(tabs, 40 - (0 + 1 + 2 + 3)); // 34
  await Promise.all(tabs.map((t) => t.counter.close()));
});

test('标签页关闭后计数不丢，新标签页（刷新）看到一致计数', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 3 }, () => makeTab(env)));

  await Promise.all(tabs.flatMap((t) => Array.from({ length: 30 }, () => t.counter.increment())));
  await env.bus.flush();

  // 关闭一个标签页（channel 断开，但 op 已落盘）
  await tabs[2].counter.close();

  // 其余标签页继续操作
  await Promise.all(tabs.slice(0, 2).flatMap((t) => Array.from({ length: 20 }, () => t.counter.increment())));
  await env.bus.flush();

  // “刷新”被关闭的标签页：同一数据库上新建实例
  const reopened = await makeTab(env);
  assert.equal(reopened.counter.value, 30 * 3 + 20 * 2); // 130

  // 刷新任意其他标签页，计数同样一致
  const refreshed = await makeTab(env);
  assert.equal(refreshed.counter.value, 130);

  await Promise.all([...tabs.slice(0, 2), reopened, refreshed].map((t) => t.counter.close()));
});

test('离线操作恢复后合并正确', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 3 }, () => makeTab(env)));

  // tab0 离线：广播发不出也收不到，但本地操作照常落盘
  env.bus.isolate(tabs[0].channel);
  await Promise.all(Array.from({ length: 50 }, () => tabs[0].counter.increment()));
  await Promise.all(tabs.slice(1).flatMap((t) => Array.from({ length: 30 }, () => t.counter.increment())));
  await env.bus.flush();

  // 离线期间：tab0 只看到自己的 50，其他标签页只看到彼此的 60
  assert.equal(tabs[0].counter.value, 50);
  assert.equal(tabs[1].counter.value, 60);

  // 恢复在线 + resync（对应 online/focus 事件触发）
  env.bus.heal(tabs[0].channel);
  await settle(env, tabs);
  assertConsistent(tabs, 50 + 30 * 2); // 110
  await Promise.all(tabs.map((t) => t.counter.close()));
});

test('消息乱序不丢更新', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 4 }, () => makeTab(env)));

  // 暂停投递，攒一批消息后乱序投递
  env.bus.setDelivering(false);
  await Promise.all(
    tabs.flatMap((t, i) => [
      ...Array.from({ length: 25 }, () => t.counter.increment()),
      ...Array.from({ length: i }, () => t.counter.decrement()),
    ]),
  );
  assert.ok(env.bus.pendingCount > 0);
  env.bus.shufflePending();
  env.bus.setDelivering(true);

  await settle(env, tabs);
  assertConsistent(tabs, 25 * 4 - (0 + 1 + 2 + 3)); // 94
  await Promise.all(tabs.map((t) => t.counter.close()));
});

test('reset 与并发 inc 的因果关系跨标签页一致（乱序下 reset 语义正确）', async () => {
  const env = makeEnv();
  const tabs = await Promise.all(Array.from({ length: 2 }, () => makeTab(env)));

  // tab0 先加 10 次并正常广播
  await Promise.all(Array.from({ length: 10 }, () => tabs[0].counter.increment()));
  await env.bus.flush();

  // tab0 reset、tab1 在看到 reset 之后加 5 次；两批消息乱序投递
  env.bus.setDelivering(false);
  await tabs[0].counter.reset();
  await env.bus.flush(); // reset 留在队列里，但 tab1 的 lamport 已通过 resync 对齐
  await tabs[1].counter.resync();
  await Promise.all(Array.from({ length: 5 }, () => tabs[1].counter.increment()));
  env.bus.shufflePending();
  env.bus.setDelivering(true);

  await settle(env, tabs);
  // reset 清掉之前的 10；reset 之后的 5 次 inc 保留
  assertConsistent(tabs, 5);
  await Promise.all(tabs.map((t) => t.counter.close()));
});
