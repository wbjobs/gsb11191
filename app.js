import { createCounter } from './counter.js';

const countEl = document.getElementById('count');
const tabEl = document.getElementById('tab');

const counter = await createCounter({
  onChange: (v) => { countEl.textContent = v; },
});

tabEl.textContent = counter.tabId;
countEl.textContent = counter.value;

document.getElementById('inc').addEventListener('click', () => counter.increment());
document.getElementById('dec').addEventListener('click', () => counter.decrement());
document.getElementById('reset').addEventListener('click', () => counter.reset());
