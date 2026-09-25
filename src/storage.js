const DB_NAME = 'shared-counter';
const DB_VERSION = 1;
const STORE_NAME = 'operations';

export function openOperationStore() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        const store = database.createObjectStore(STORE_NAME, { keyPath: 'id' });
        store.createIndex('actorSeq', ['actor', 'seq'], { unique: true });
      }
    };

    request.onsuccess = () => {
      const database = request.result;
      database.onversionchange = () => database.close();
      resolve(database);
    };
    request.onerror = () => reject(request.error);
    request.onblocked = () => reject(new Error('IndexedDB upgrade is blocked'));
  });
}

export function createOperationRepository(database) {
  return {
    load: () => loadOperations(database),
    put: (operation) => putOperation(database, operation),
  };
}

async function loadOperations(database) {
  const transaction = database.transaction(STORE_NAME, 'readonly');
  return getAll(transaction.objectStore(STORE_NAME));
}

function putOperation(database, operation) {
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(STORE_NAME, 'readwrite');
    transaction.objectStore(STORE_NAME).put(operation);
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error);
  });
}

function getAll(requestSource) {
  return new Promise((resolve, reject) => {
    const request = requestSource.getAll();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}
