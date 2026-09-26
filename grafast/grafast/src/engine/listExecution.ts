import { isAsyncIterable, isIterable } from "iterall";

import type { Bucket, RequestTools } from "../bucket.ts";
import { FLAG_ERROR, NO_FLAGS } from "../constants.ts";
import { flagError } from "../error.ts";
import type { BatchExecutionValue } from "../interfaces.ts";
import { abortable, consume, isPromiseLike } from "../utils.ts";
import { batchExecutionValue } from "./executeBucket.ts";
import type { LayerPlan, LayerPlanReasonListItem } from "./LayerPlan.ts";

/** One list consumer for each parent position, independent of source identity. */
export interface ListTraversal {
  count: number;
  complete: boolean;
  iterator?: Iterator<any> | AsyncIterator<any>;
}

export interface ListExecution {
  layer: LayerPlan<LayerPlanReasonListItem>;
  values: BatchExecutionValue;
  entries: ListTraversal[];
}

/** Prepare list items before creating and executing the flattened item bucket. */
export function prepareList(
  bucket: Bucket,
  layer: LayerPlan<LayerPlanReasonListItem>,
  request: RequestTools,
): Promise<void> | void {
  const source = bucket.store.get(layer.reason.parentStep.id)!;
  const state: ListExecution = {
    layer,
    values: batchExecutionValue([]),
    entries: [],
  };
  (bucket.listExecutions ??= new Map()).set(layer.id, state);
  const stream = layer.reason.stream;
  const shouldStream =
    stream != null &&
    (stream.ifStepId == null ||
      bucket.store.get(stream.ifStepId)!.unaryValue());
  const initialCount = shouldStream
    ? stream.initialCountStepId == null
      ? 0
      : bucket.store.get(stream.initialCountStepId)!.unaryValue()
    : Infinity;
  const promises: Promise<void>[] = [];
  for (let index = 0; index < bucket.size; index++) {
    const value = source.at(index);
    const flags = source._flagsAt(index);
    const entry: ListTraversal = { count: 0, complete: true };
    state.entries[index] = entry;
    state.values._setResult(index, value, flags);
    const sideEffect = layer.parentSideEffectStep;
    if (
      flags & FLAG_ERROR ||
      value == null ||
      (sideEffect &&
        bucket.store.get(sideEffect.id)!._flagsAt(index) & FLAG_ERROR)
    )
      continue;
    if (
      initialCount < 0 ||
      (!Number.isInteger(initialCount) && initialCount !== Infinity)
    ) {
      state.values._setResult(
        index,
        new Error("initialCount must be a non-negative integer"),
        FLAG_ERROR,
      );
      continue;
    }
    if (
      Array.isArray(value) &&
      value.length <= initialCount &&
      !value.some(isPromiseLike)
    ) {
      entry.count = value.length;
      continue;
    }
    let iterator: Iterator<any> | AsyncIterator<any>;
    try {
      iterator = isAsyncIterable(value)
        ? value[Symbol.asyncIterator]()
        : isIterable(value)
          ? value[Symbol.iterator]()
          : (() => {
              throw new Error("Expected an iterable for a list field");
            })();
    } catch (error) {
      state.values._setResult(index, error, FLAG_ERROR);
      continue;
    }
    const owned = (bucket.iterators[index] ??= new Set());
    owned.add(iterator);
    entry.iterator = iterator;
    entry.complete = false;
    const rows: any[] = [];
    state.values._setResult(index, rows, NO_FLAGS);
    if (initialCount === 0) continue;
    promises.push(
      (async () => {
        try {
          while (rows.length < initialCount && !request.abortSignal.aborted) {
            const result = await abortable(
              request.abortSignal,
              { done: true, value: undefined } as const,
              iterator.next(),
            );
            if (result.done) {
              entry.complete = true;
              entry.iterator = undefined;
              owned.delete(iterator);
              break;
            }
            try {
              const value = await abortable(
                request.abortSignal,
                undefined,
                result.value,
              );
              if (request.abortSignal.aborted) break;
              rows.push(value);
            } catch (error) {
              rows.push(flagError(error));
            }
            entry.count++;
          }
          if (request.abortSignal.aborted) {
            entry.complete = true;
            entry.iterator = undefined;
            owned.delete(iterator);
            try {
              consume(iterator.return?.());
            } catch {
              /* The request is cancelled. */
            }
          }
        } catch (error) {
          state.values._setResult(index, error, FLAG_ERROR);
          entry.complete = true;
          entry.iterator = undefined;
          owned.delete(iterator);
          try {
            consume(iterator.return?.());
          } catch {
            /* Preserve the original error. */
          }
        }
      })(),
    );
  }
  if (promises.length) return Promise.all(promises).then(() => {});
}
