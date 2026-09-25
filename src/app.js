import { BroadcastTransport } from './broadcast-transport.js';
import { CounterStore } from './counter-store.js';
import { createOperationRepository, openOperationStore } from './storage.js';

const valueElement = document.querySelector('[data-counter-value]');
const statusElement = document.querySelector('[data-status]');
const incrementButton = document.querySelector('[data-action="increment"]');
const decrementButton = document.querySelector('[data-action="decrement"]');
const resetButton = document.querySelector('[data-action="reset"]');
const buttons = [incrementButton, decrementButton, resetButton];

let store;

try {
  const database = await openOperationStore();
  store = new CounterStore({
    repository: createOperationRepository(database),
    transport: new BroadcastTransport(),
  });

  store.subscribe((value) => {
    valueElement.textContent = String(value);
  });
  await store.init();
  statusElement.textContent = '已同步';
} catch (error) {
  console.error(error);
  statusElement.textContent = '无法初始化本地存储';
  valueElement.textContent = '—';
}

for (const button of buttons) {
  button.disabled = !store;
}

incrementButton.addEventListener('click', () => run(store.increment.bind(store)));
decrementButton.addEventListener('click', () => run(store.decrement.bind(store)));
resetButton.addEventListener('click', () => run(store.reset.bind(store)));

window.addEventListener('pagehide', () => store?.stop());

async function run(action) {
  setBusy(true);
  try {
    await action();
  } catch (error) {
    console.error(error);
    statusElement.textContent = '操作保存失败，请重试';
  } finally {
    setBusy(false);
  }
}

function setBusy(busy) {
  for (const button of buttons) {
    button.disabled = busy || !store;
  }
}
