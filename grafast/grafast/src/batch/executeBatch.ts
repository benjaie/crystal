import { noop } from "../dev.ts";
import type { PromiseOrDirect } from "../interfaces.ts";
import { isPromiseLike } from "../utils.ts";
import type {
  BatchFamily,
  BatchOptions,
  PendingBatch,
} from "./batchInterfaces.ts";

export function executeBatch<TOptions extends BatchOptions, TKeyset, TResult>(
  family: BatchFamily<TOptions, TKeyset, TResult>,
  loadBatch: PendingBatch<TKeyset, TResult>,
): PromiseOrDirect<void> {
  const { deferred, keysets } = loadBatch;
  try {
    const promiseOrValue = family.executor.callback(family.options, keysets);
    if (isPromiseLike(promiseOrValue)) {
      return promiseOrValue.then(processResults).then(null, handleError);
    } else {
      return processResults(promiseOrValue);
    }
  } catch (e) {
    handleError(e);
  }

  function processResults(loadResults: readonly TResult[]) {
    const keyCount = keysets.length;
    if (loadResults.length !== keyCount) {
      // This path shouldn't happen, so expensively handle the case that
      // loadResults may include promises
      void Promise.all(loadResults).catch(noop);
      throw new Error(
        `batch callback returned ${loadResults.length} results for ${keyCount} keysets`,
      );
    }
    const { entries } = loadBatch;
    if (entries === null) {
      const { cache, pending } = family;
      for (let i = 0; i < keyCount; i++) {
        const result = loadResults[i];
        cache.set(keysets[i], result);
        if (isPromiseLike(result)) {
          result.then(null, noop);
        }
      }
      // Cache hits take precedence over pending entries. Once all results are
      // cached, clear the map in one operation if no other batch is pending.
      if (pending.size === keyCount) {
        pending.clear();
      } else {
        for (let i = 0; i < keyCount; i++) {
          pending.delete(keysets[i]);
        }
      }
    } else {
      for (let i = 0; i < keyCount; i++) {
        const result = loadResults[i];
        const entry = entries[i];
        entry.result = result;
        entry.batch = null;
        entry.index = -1;
        if (isPromiseLike(result)) {
          result.then(null, noop);
        }
      }
    }
    deferred.resolve(loadResults);
  }
  function handleError(error: unknown) {
    if (loadBatch.entries !== null) {
      for (const entry of loadBatch.entries) {
        entry.batch = undefined;
        entry.index = -1;
        entry.result = undefined;
      }
    } else {
      for (const spec of keysets) {
        family.cache.delete(spec);
        family.pending.delete(spec);
      }
    }
    deferred.reject(error);
  }
}
