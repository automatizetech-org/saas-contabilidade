import { del, get, set } from "idb-keyval";

const STORAGE_BASE_KEY = "saas-contabilidade:react-query:client";

function getOfficeKey(officeId: string) {
  return `${STORAGE_BASE_KEY}:${officeId}`;
}

/**
 * Persister que salva/restaura o estado hidratado do React Query em IndexedDB.
 * Usamos uma chave por `office_id` para evitar "vazamento" de cache entre escritórios.
 */
export function createIndexedDBPersisterForOffice(officeId: string) {
  const key = getOfficeKey(officeId);

  return {
    async persistClient(persistedClient: unknown) {
      await set(key, persistedClient);
    },
    async restoreClient() {
      const value = await get(key);
      return value ?? undefined;
    },
    async removeClient() {
      await del(key);
    },
  };
}

