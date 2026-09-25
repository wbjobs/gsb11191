# 跨标签页共享计数器

一个计数器，支持 +1 / −1 / 重置，在多个浏览器标签页之间实时共享，最终一致、不丢更新。

## 运行

```bash
npm start          # 或 python3 -m http.server 8000
# 打开 http://localhost:8000 ，多开几个标签页同时操作
```

## 测试

```bash
npm test           # node --test，覆盖全部验收标准（见 test/counter.test.mjs）
```

## 设计

技术栈：BroadcastChannel（实时同步）+ IndexedDB（持久化与事实源）。

核心思路是**事件溯源**，不做跨标签页的 read-modify-write：

- 每次操作生成一条 op：`{ id, tabId, type: 'inc'|'dec'|'reset', lamport, ts }`。
- op **先写入 IndexedDB，再广播**——关标签页、崩溃、离线都不会丢已确认的操作。
- 计数是对 op 集合求值的**纯函数**：取 `(lamport, id)` 全序最大的 reset 为界，
  其后的 inc/dec 求和。inc/dec 可交换、reset 用 Lamport 时钟定序，
  因此并发提交、消息乱序、重复投递都得到相同结果（最终一致）。
- `online` / `focus` / `visibilitychange` 事件 + 每 3s 定时从 IndexedDB 全量
  resync，任何丢失的广播都能自愈（离线恢复、漏消息兜底）。

各验收标准的对应机制：

| 场景 | 机制 |
| --- | --- |
| 4 标签页 × 100 并发加 | op 只追加不覆盖，无竞争；求和可交换 |
| 同时重置 | 多个 reset 按 `(lamport, id)` 全序取最大，各端结果一致 |
| 标签页关闭 | op 先落盘再广播，重开时从 IndexedDB 恢复 |
| 离线后恢复 | 离线期间 op 照常落盘；恢复后 resync 全量合并 |
| 消息乱序 | 计数是集合的纯函数，与到达顺序无关；merge 按 id 幂等 |
| 刷新任意标签页 | 启动时从 IndexedDB 全量重放 |

## 文件

- `counter.js` — 核心逻辑（浏览器与 Node 通用，存储/通道可注入）
- `index.html` / `app.js` — 演示页面
- `test/counter.test.mjs` — 验收测试（6 个场景）
- `test/fake-env.mjs` — 测试替身：共享存储的 fake IndexedDB + 可控广播总线
