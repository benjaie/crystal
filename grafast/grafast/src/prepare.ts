import * as graphql from "graphql";
import type {
  AsyncExecutionResult,
  ExecutionResult,
} from "graphql/execution/execute.js";
import { isAsyncIterable } from "iterall";

import * as assert from "./assert.ts";
import type { Bucket, RequestTools } from "./bucket.ts";
import {
  $$contextPlanCache,
  $$eventEmitter,
  $$extensions,
  $$streamMore,
  FLAG_ERROR,
  FLAG_NULL,
  NO_FLAGS,
} from "./constants.ts";
import { isDev } from "./dev.ts";
import {
  batchExecutionValue,
  executeBucket,
  newBucket,
  unaryExecutionValue,
} from "./engine/executeBucket.ts";
import type {
  OutputPlanContext,
  PayloadRoot,
  SubsequentPayloadSpec,
  SubsequentStreamSpec,
} from "./engine/executeOutputPlan.ts";
import { executeOutputPlan } from "./engine/executeOutputPlan.ts";
import type { OperationPlan } from "./engine/OperationPlan.ts";
import { POLYMORPHIC_ROOT_PATH } from "./engine/OperationPlan.ts";
import type { OutputPlan } from "./engine/OutputPlan.ts";
import {
  coerceError,
  getChildBucketAndIndex,
  getDirectLayerPlanChild,
} from "./engine/OutputPlan.ts";
import { isFlaggedValue, SafeError } from "./error.ts";
import type {
  ErrorBehavior,
  GrafastInternalExecutionArgs,
  GrafastTimeouts,
  JSONValue,
  PromiseOrDirect,
  StreamMaybeMoreableArray,
  StreamMoreableArray,
} from "./interfaces.ts";
import { _plan } from "./plan.ts";
import { timeSource } from "./timeSource.ts";
import {
  abortable,
  arrayOfLength,
  asyncIteratorWithCleanup,
  consume,
  isPromiseLike,
} from "./utils.ts";

const { GraphQLError } = graphql;

export interface GrafastOperationOptions {
  /**
   * A list of 'explain' types that should be included in `extensions.explain`.
   *
   * - `plan` will cause the plan JSON to be included
   * - other values are dependent on the plugins in play
   *
   * If set to `true` then all possible explain types will be exposed.
   */
  explain?: boolean | string[];

  /**
   * If true, the result will be returned as a string rather than an object -
   * this is an optimization for returning the data over a network socket or
   * similar.
   */
  outputDataAsString?: boolean;

  timeouts?: GrafastTimeouts;

  /**
   * How many planning layers deep do we allow? Should be handled by validation.
   *
   * A planning layer can happen due to:
   *
   * - A nested selection set
   * - Planning a field return type
   * - A list position
   * - A polymorphic type
   * - A deferred/streamed response
   *
   * These reasons may each cause 1, 2 or 3 planning layers to be added, so this
   * limit should be set quite high - e.g. 6x the selection set depth.
   */
  maxPlanningDepth?: number;
}

function noop() {}

function processRoot(
  // errors should already have been handled, and this ctx isn't suitable to be reused.
  ctx: Omit<OutputPlanContext, "errors">,
  iterator: ResultIterator,
  outputDataAsString: boolean,
): PromiseOrDirect<void> {
  const { streams, queue } = ctx.root;

  if (isDev) {
    // Cannot add to streams/queue now - we're finished. The streams/queue
    // get their own new roots where addition streams/queue can be added.
    Object.freeze(streams);
    Object.freeze(queue);
  }

  const promises: PromiseLike<void>[] = [];
  const groups = new Map<
    SubsequentStreamSpec["stream"],
    SubsequentStreamSpec[]
  >();
  for (const spec of streams) {
    const group = groups.get(spec.stream);
    if (group === undefined) {
      groups.set(spec.stream, [spec]);
    } else {
      group.push(spec);
    }
  }
  for (const group of groups.values()) {
    promises.push(
      processStream(ctx.requestContext, iterator, group, outputDataAsString),
    );
  }
  for (const deferred of queue) {
    promises.push(
      processDeferred(
        ctx.requestContext,
        iterator,
        deferred,
        outputDataAsString,
      ),
    );
  }

  // Terminate the iterator when we're done
  if (promises.length !== 0) {
    return Promise.all(promises).then(noop);
  }
}

function releaseUnusedIterators(
  bucket: Bucket,
  bucketIndex: number,
  streams: SubsequentStreamSpec[] | null,
) {
  const allStreams = bucket.iterators[bucketIndex];
  if (!allStreams) {
    if (streams && streams.length > 0) {
      console.error(
        `GrafastInternalError<c3f57a2b-159d-49a9-924b-8e753696de06>: Untracked stream detected!`,
      );
    }
    return;
  }
  if (streams) {
    for (const stream of new Set(streams.map((spec) => spec.stream))) {
      if (!allStreams.delete(stream)) {
        console.error(
          `GrafastInternalError<1e43efdf-f901-43dd-8a82-768246e61478>: Stream was returned from an output plan, but that stream wasn't tracked via the bucket.`,
        );
      }
    }
  }
  if (allStreams.size > 0) {
    for (const stream of allStreams) {
      allStreams.delete(stream);
      if (stream.return) {
        try {
          consume(stream.return());
        } catch {
          /*noop*/
        }
      } else if (stream.throw) {
        try {
          consume(
            stream.throw(
              new Error(
                `Iterator no longer needed (due to OutputPlan branch being skipped)`,
              ),
            ),
          );
        } catch {
          /*noop*/
        }
      }
    }
  }
}

const finalize = (
  data: JSONValue | null | undefined,
  ctx: OutputPlanContext,
  extensions: any,
  outputDataAsString: boolean,
): ExecutionResult | AsyncGenerator<AsyncExecutionResult, void, void> => {
  if (ctx.root.streams.length > 0 || ctx.root.queue.length > 0) {
    // Return an async iterator
    let alive = true;
    const iterator: ResultIterator = newIterator(() => {
      alive = false;
    });
    const payload = Object.create(null);
    if (data !== undefined) {
      payload.data = data;
    }
    const errors = ctx.root.errors;
    if (errors.length > 0) {
      payload.errors = errors;
    }
    if (extensions != null) {
      payload.extensions = extensions;
    }
    payload.hasNext = true;
    iterator.push(payload);

    const promise = processRoot(ctx, iterator, outputDataAsString);
    if (isPromiseLike(promise)) {
      promise.then(
        () => {
          if (!alive) {
            return;
          }
          iterator.push({ hasNext: false });
          iterator.finish();
        },
        (e) => {
          iterator.throw(e).then(null, noop);
        },
      );
    } else {
      iterator.push({ hasNext: false });
      iterator.finish();
    }

    return iterator;
  } else {
    const result = Object.create(null);
    if (data !== undefined) {
      result.data = data;
    }
    const errors = ctx.root.errors;
    if (errors.length > 0) {
      result.errors = errors;
    }
    if (extensions !== undefined) {
      result.extensions = extensions;
    }
    return result;
  }
};

function outputBucket(
  outputPlan: OutputPlan,
  rootBucket: Bucket,
  rootBucketIndex: number,
  requestContext: RequestTools,
  path: readonly (string | number)[],
  variables: { [key: string]: any },
  asString: boolean,
): [ctx: OutputPlanContext, result: JSONValue | null] /*PromiseOrDirect<
  ExecutionResult | AsyncGenerator<AsyncExecutionResult, void, void>
>*/ {
  const operationPlan = rootBucket.layerPlan.operationPlan;
  const root: PayloadRoot = {
    errorBehavior: requestContext.args.onError ?? "PROPAGATE",
    errors: [],
    queue: [],
    streams: [],
    variables,
  };
  const ctx: OutputPlanContext = {
    requestContext,
    root,
    path,
  };
  let childBucket;
  let childBucketIndex;
  if (outputPlan.layerPlan === rootBucket.layerPlan) {
    childBucket = rootBucket;
    childBucketIndex = rootBucketIndex;
  } else {
    const c = getChildBucketAndIndex(
      outputPlan.layerPlan,
      null,
      rootBucket,
      rootBucketIndex,
      null,
    );
    if (!c) {
      throw new Error(
        `GrafastInternalError<8bbf56c1-8e2a-4ee9-b5fc-724fd0ee222b>: could not find relevant bucket for output plan ${outputPlan} from ${rootBucket}[${rootBucketIndex}]`,
      );
    }
    [childBucket, childBucketIndex] = c;
  }
  try {
    const result = executeOutputPlan(
      ctx,
      outputPlan,
      childBucket,
      childBucketIndex,
      asString,
    );
    return [ctx, result ?? null];
  } catch (e) {
    const error = coerceError(
      e,
      operationPlan.rootOutputPlan.locationDetails,
      [],
    );
    ctx.root.errors.push(error);
    return [ctx, null];
  } finally {
    releaseUnusedIterators(rootBucket, rootBucketIndex, ctx.root.streams);
  }
}

/** @internal */
function executePreemptive(
  args: GrafastInternalExecutionArgs,
  operationPlan: OperationPlan,
  variableValues: any,
  onError: ErrorBehavior,
  outputDataAsString: boolean,
  executionTimeout: number | null,
  requestAbortSignal: AbortSignal,
): PromiseOrDirect<
  ExecutionResult | AsyncGenerator<AsyncExecutionResult, void, void>
> {
  const rootBucketIndex = 0;
  const size = 1;

  const polymorphicPathList = [POLYMORPHIC_ROOT_PATH];
  const iterators: Array<Set<AsyncIterator<any> | Iterator<any>>> = [new Set()];

  const store: Bucket["store"] = new Map();
  store.set(
    operationPlan.variableValuesStep.id,
    unaryExecutionValue(variableValues),
  );
  store.set(
    operationPlan.contextStep.id,
    unaryExecutionValue(args.contextValue),
  );
  store.set(
    operationPlan.rootValueStep.id,
    unaryExecutionValue(args.rootValue),
  );

  const rootBucket = newBucket(null, {
    layerPlan: operationPlan.rootLayerPlan,
    size,
    store,
    flagUnion: NO_FLAGS,
    polymorphicPathList,
    polymorphicType: null,
    iterators,
  });
  const startTime = timeSource.now();
  const stopTime =
    executionTimeout !== null ? startTime + executionTimeout : null;
  const rowLimit =
    args.resolvedPreset?.grafast?.maxMaterializedStreamRows ?? 100_000;
  if (
    rowLimit !== Infinity &&
    (!Number.isSafeInteger(rowLimit) || rowLimit < 0)
  ) {
    throw new Error(
      "maxMaterializedStreamRows must be a non-negative safe integer or Infinity",
    );
  }
  const requestContext: RequestTools = {
    args,
    onError,
    variableValues,
    remainingStreamRows: rowLimit,
    retainStreamRows(count) {
      this.remainingStreamRows -= count;
      if (this.remainingStreamRows < 0) {
        throw new SafeError("Materialized stream row limit exceeded");
      }
    },
    startTime,
    stopTime,
    // toSerialize: [],
    eventEmitter: args[$$eventEmitter],
    abortSignal: requestAbortSignal,
  };

  const bucketPromise = executeBucket(rootBucket, requestContext);

  const subscriptionLayerPlan = rootBucket.layerPlan.children.find(
    (c) => c.reason.type === "subscription",
  );

  function executeStreamPayload(
    payload: any,
    index: number,
  ): PromiseOrDirect<ExecutionResult | AsyncGenerator<AsyncExecutionResult>> {
    const layerPlan = subscriptionLayerPlan!;
    const { rootStep } = layerPlan;
    // PERF: we could consider batching this.
    const store: Bucket["store"] = new Map();
    const ZERO = 0;

    for (const depId of layerPlan.copyStepIds) {
      const executionVal = rootBucket.store.get(depId)!;
      // Normally this would need scaling, but not this time since we know it only represents a single entry
      store.set(depId, executionVal);
    }

    const rootExecutionValue = rootStep!._isUnary
      ? unaryExecutionValue(payload)
      : batchExecutionValue([payload]);
    store.set(rootStep!.id, rootExecutionValue);

    const subscriptionBucket = newBucket(rootBucket, {
      layerPlan,
      store,
      flagUnion: rootBucket.flagUnion,
      polymorphicPathList: [POLYMORPHIC_ROOT_PATH],
      polymorphicType: null,
      iterators: [new Set()],
      size: 1, //store.size
    });
    const bucketPromise = executeBucket(subscriptionBucket, requestContext);
    function outputStreamBucket() {
      // NOTE: this is the root output plan for a subscription operation.
      const [ctx, result] = outputBucket(
        operationPlan.rootOutputPlan,
        subscriptionBucket,
        ZERO,
        requestContext,
        [],
        rootBucket.store
          .get(operationPlan.variableValuesStep.id)!
          .at(rootBucketIndex),
        outputDataAsString,
      );
      return finalize(
        result,
        ctx,
        index === 0 ? (args[$$extensions] ?? undefined) : undefined,
        outputDataAsString,
      );
    }
    if (isPromiseLike(bucketPromise)) {
      return bucketPromise.then(outputStreamBucket);
    } else {
      return outputStreamBucket();
    }
  }

  function output() {
    // Later we'll need to loop

    // If it's a subscription we need to use the stream
    const bucketRootEValue =
      rootBucket.layerPlan.rootStep != null &&
      rootBucket.layerPlan.rootStep.id != null
        ? rootBucket.store.get(rootBucket.layerPlan.rootStep.id)!
        : null;
    const bucketRootValue = bucketRootEValue?.at(rootBucketIndex);
    const bucketRootFlags =
      bucketRootEValue?._flagsAt(rootBucketIndex) ?? NO_FLAGS;
    if (bucketRootFlags & FLAG_ERROR) {
      releaseUnusedIterators(rootBucket, rootBucketIndex, null);
      // Something major went wrong!
      const errors = [
        new GraphQLError(bucketRootValue.message, {
          nodes: operationPlan.rootOutputPlan.locationDetails.node,
          source: undefined,
          positions: null,
          path: null,
          originalError: bucketRootValue,
          extensions: null,
        }),
      ];
      const payload = Object.create(null) as ExecutionResult;
      payload.errors = errors;
      const extensions = bucketRootValue[$$extensions];
      if (extensions != null) {
        payload.extensions = extensions;
      }
      return payload;
    }

    // NOTE: this is where we determine whether to stream or not
    if (
      bucketRootValue != null &&
      subscriptionLayerPlan != null &&
      Array.isArray(bucketRootValue) &&
      (bucketRootValue as StreamMaybeMoreableArray)[$$streamMore]
    ) {
      // We expect exactly one streamable, we should not need to
      // `releaseUnusedIterators(rootBucket, rootBucketIndex, null)` here.
      const arr = bucketRootValue as StreamMoreableArray;
      const stream = arr[$$streamMore];
      const iteratorAbortController = new AbortController();
      const abortIteratorWhenRequestAborts = () =>
        iteratorAbortController.abort();
      requestAbortSignal.addEventListener(
        "abort",
        abortIteratorWhenRequestAborts,
        { once: true },
      );
      const iteratorAbortSignal = iteratorAbortController.signal;
      const iterator = newIterator((e) => {
        iteratorAbortController.abort();
        requestAbortSignal.removeEventListener(
          "abort",
          abortIteratorWhenRequestAborts,
        );
        if (e != null) {
          try {
            consume(stream.throw?.(e));
          } catch {
            /*noop*/
          }
        } else {
          try {
            consume(stream.return?.());
          } catch {
            /*noop*/
          }
        }
      });
      (async () => {
        let i = 0;
        while (await iterator.ready()) {
          const next = await abortable(
            iteratorAbortSignal,
            undefined,
            stream.next(),
          );
          if (next === undefined) {
            consume(stream.return?.());
            break;
          }
          if (next.done) {
            // Stream already exited
            break;
          }
          const payload = await abortable(
            iteratorAbortSignal,
            undefined,
            executeStreamPayload(next.value, i),
          );
          if (payload === undefined) {
            break;
          }
          if (isAsyncIterable(payload)) {
            const payloadIterator = payload[Symbol.asyncIterator]();
            while (true) {
              if (!(await iterator.ready())) {
                await payloadIterator.return?.(undefined);
                break;
              }
              const next = await abortable(
                iteratorAbortSignal,
                undefined,
                payloadIterator.next(),
              );
              if (next === undefined) {
                consume(payloadIterator.return?.(undefined));
                break;
              }
              if (next.done) {
                // Iterator already exited
                break;
              }
              iterator.push(next.value);
            }
          } else {
            iterator.push(payload);
          }
          i++;
        }
      })()
        .then(
          () => iterator.finish(),
          (e) => iterator.throw(e),
        )
        .then(null, noop);
      return iterator;
    }

    // NOTE: this is the root output plan of a query/mutation operation
    const [ctx, result] = outputBucket(
      operationPlan.rootOutputPlan,
      rootBucket,
      rootBucketIndex,
      requestContext,
      [],
      rootBucket.store.get(operationPlan.variableValuesStep.id)!.at(0),
      outputDataAsString,
    );
    return finalize(result, ctx, args[$$extensions], outputDataAsString);
  }

  if (isPromiseLike(bucketPromise)) {
    return bucketPromise.then(output);
  } else {
    return output();
  }
}

/**
 * @internal
 */
export function grafastPrepare(
  args: GrafastInternalExecutionArgs,
): PromiseOrDirect<
  ExecutionResult | AsyncGenerator<AsyncExecutionResult, void, void>
> {
  const planResult = _plan(args);
  if (planResult.errors != null) {
    const { errors } = planResult;
    return { errors };
  }
  const { operationPlan, variableValues, onError } = planResult;

  if (
    args.explain === true ||
    (args.explain && args.explain.includes("plan"))
  ) {
    // Only build the plan once
    if (operationPlan[$$contextPlanCache] == null) {
      operationPlan[$$contextPlanCache] = operationPlan.generatePlanJSON();
    }
    args[$$extensions]?.explain?.operations.push({
      type: "plan",
      title: "Plan",
      plan: operationPlan[$$contextPlanCache],
    });
  }

  const executionTimeout = args.timeouts?.execution ?? null;
  const abortController = new AbortController();
  try {
    const result = executePreemptive(
      args,
      operationPlan,
      variableValues,
      onError,
      args.outputDataAsString ?? false,
      executionTimeout,
      abortController.signal,
    );
    if (isPromiseLike(result)) {
      return result.then(
        (v) => handleMaybeIterator(abortController, v),
        (e) => {
          abortController.abort(e);
          throw e;
        },
      );
    } else {
      return handleMaybeIterator(abortController, result);
    }
  } catch (e) {
    abortController.abort(e);
    throw e;
  }
}

interface PushableAsyncGenerator<T> extends AsyncGenerator<T, void, undefined> {
  push(v: T): void;
  ready(): PromiseOrDirect<boolean>;
  finish(): void;
}

type ResultIterator = PushableAsyncGenerator<AsyncExecutionResult>;

function newIterator<T = any>(
  abort: (e?: any) => void,
): PushableAsyncGenerator<T> {
  const valueQueue: any[] = [];
  const pullQueue: Array<
    [
      (
        value: IteratorResult<T, any> | PromiseLike<IteratorResult<T, any>>,
      ) => void,
      (reason?: any) => void,
    ]
  > = [];
  let done = false;
  let drained: PromiseWithResolvers<boolean> | null = null;
  let failure: { error: unknown } | null = null;
  return {
    ready() {
      if (done) {
        return false;
      }
      if (valueQueue.length === 0) {
        return true;
      }
      drained ??= Promise.withResolvers<boolean>();
      return drained.promise;
    },
    [Symbol.asyncIterator]() {
      return this;
    },
    async [Symbol.asyncDispose]() {
      await this.return();
    },
    push(v: T | PromiseLike<T>) {
      if (done) {
        // A projection already in flight can finish after the client returns.
        consume(v);
        return;
      }
      const cbs = pullQueue.shift();
      if (cbs !== undefined) {
        if (isPromiseLike(v)) {
          v.then(
            (v) => cbs[0]({ done: false, value: v }),
            (e) => {
              try {
                consume(cbs[1](e));
              } catch (e) {
                // ignore
              }
              this.throw(e).then(null, noop);
            },
          );
        } else {
          cbs[0]({ done: false, value: v });
        }
      } else {
        valueQueue.push(v);
      }
    },
    next() {
      if (valueQueue.length > 0) {
        const next = valueQueue.shift();
        if (valueQueue.length === 0) {
          drained?.resolve(true);
          drained = null;
        }
        return Promise.resolve(next).then((value) => ({
          done: false,
          value,
        }));
      } else if (failure !== null) {
        return Promise.reject(failure.error);
      } else if (done) {
        return Promise.resolve({
          done: true,
          value: undefined,
        });
      } else {
        return new Promise((resolve, reject) => {
          pullQueue.push([resolve, reject]);
        });
      }
    },
    finish() {
      if (!done) {
        done = true;
        drained?.resolve(false);
        abort();
        for (const entry of pullQueue) {
          try {
            entry[0]({ done: true, value: undefined });
          } catch (e) {
            // ignore
          }
        }
        pullQueue.length = 0;
      }
    },
    return() {
      valueQueue.length = 0;
      failure = null;
      this.finish();
      return Promise.resolve({ done: true, value: undefined });
    },
    throw(e) {
      if (!done) {
        done = true;
        drained?.resolve(false);
        failure = { error: e };
        abort(e);
        for (const entry of pullQueue) {
          try {
            entry[1](e);
          } catch (e) {
            // ignore
          }
        }
      }
      return Promise.reject(e);
    },
  };
}

async function processStream(
  requestContext: RequestTools,
  iterator: ResultIterator,
  specs: SubsequentStreamSpec[],
  outputDataAsString: boolean,
): Promise<void> {
  type ResultTuple = [value: any, index: number, flags: number];
  const failDescendants = (error: unknown) => {
    consume(iterator.throw(error));
  };
  const entryFor = (
    result: IteratorResult<any>,
    index: number,
  ): ResultTuple => {
    let value = result.value;
    let flags = NO_FLAGS;
    if (value !== null && typeof value === "object" && isFlaggedValue(value)) {
      flags = value.flags;
      value = value.value;
    }
    if (value == null) {
      flags |= FLAG_NULL;
    }
    return [value, index, flags];
  };
  const project = (
    spec: SubsequentStreamSpec,
    entries: ResultTuple[],
    metaByMetaKey: ReturnType<OperationPlan["makeMetaByMetaKey"]>,
    descendants: PromiseLike<void>[],
  ) => {
    const size = entries.length;
    const store: Bucket["store"] = new Map();
    const polymorphicPathList: (string | null)[] = [];
    const iterators: Array<Set<AsyncIterator<any> | Iterator<any>>> = [];

    const directLayerPlanChild = getDirectLayerPlanChild(
      spec.bucket.layerPlan,
      spec.outputPlan.layerPlan,
    );
    const { id: listItemStepId, _isUnary: isUnary } =
      directLayerPlanChild.rootStep!;

    for (const copyStepId of directLayerPlanChild.copyStepIds) {
      const executionValue = spec.bucket.store.get(copyStepId)!;
      if (!executionValue) {
        throw new Error(
          `GrafastInternalError<2db7b749-399f-486b-bd12-7ca337b937e4>: ${spec.bucket.layerPlan} doesn't seem to include ${copyStepId} (required by ${directLayerPlanChild} via ${spec.outputPlan})`,
        );
      }
      if (executionValue.isBatch) {
        const values: any[] = arrayOfLength(
          size,
          executionValue.at(spec.bucketIndex),
        );
        store.set(
          copyStepId,
          batchExecutionValue(
            values,
            arrayOfLength(size, executionValue._flagsAt(spec.bucketIndex)),
          ),
        );
      } else {
        store.set(copyStepId, executionValue);
      }
    }

    if (isUnary) {
      assert.ok(entries.length === 1, "Unary step should only have one index");
      store.set(
        listItemStepId,
        unaryExecutionValue(entries[0][0], entries[0][2]),
      );
    } else {
      const listItemStepIdList = entries.map((e) => e[0]);
      store.set(
        listItemStepId,
        batchExecutionValue(
          listItemStepIdList,
          entries.map((entry) => entry[2]),
        ),
      );
    }

    for (let bucketIndex = 0; bucketIndex < size; bucketIndex++) {
      polymorphicPathList[bucketIndex] =
        spec.bucket.polymorphicPathList[spec.bucketIndex];
      iterators[bucketIndex] = new Set();
    }

    // const childBucket = newBucket(directLayerPlanChild, noDepsList, store);
    // const childBucketIndex = 0;
    const rootBucket = newBucket(
      { sharedState: spec.bucket.sharedState, metaByMetaKey },
      {
        layerPlan: directLayerPlanChild,
        size,
        store,
        flagUnion: entries.reduce(
          (union, entry) => union | entry[2],
          spec.bucket.flagUnion,
        ),
        polymorphicPathList,
        polymorphicType: null,
        iterators,
      },
    );

    const bucketPromise = executeBucket(rootBucket, requestContext);

    const output = () => {
      for (let bucketIndex = 0; bucketIndex < size; bucketIndex++) {
        const actualIndex = entries[bucketIndex][1];
        // NOTE: this is a stream, so rootBucket.reason.type === 'listItem'
        const [ctx, result] = outputBucket(
          spec.outputPlan,
          rootBucket,
          bucketIndex,
          requestContext,
          [...spec.path, actualIndex],
          spec.root.variables,
          outputDataAsString,
        );
        const iteratorPayload = Object.create(null);
        if (result !== undefined) {
          iteratorPayload.data = result;
        }
        iteratorPayload.hasNext = true;
        if (spec.label != null) {
          iteratorPayload.label = spec.label;
        }
        if (ctx.root.errors.length > 0) {
          iteratorPayload.errors = ctx.root.errors;
        }
        iteratorPayload.path = ctx.path;
        // TODO: extensions?

        iterator.push(iteratorPayload);
        const promise = processRoot(ctx, iterator, outputDataAsString);
        if (isPromiseLike(promise)) {
          descendants.push(promise.then(undefined, failDescendants));
        }
      }
    };

    if (isPromiseLike(bucketPromise)) {
      return bucketPromise.then(output);
    } else {
      return output();
    }
  };

  const { stream, startIndex } = specs[0];
  let index = startIndex;
  let next: Promise<IteratorResult<any>> | null = null;
  const pendingBatches = new Set<Promise<void>>();
  let batchFinished: PromiseWithResolvers<void> | null = null;
  try {
    while (await iterator.ready()) {
      const first = await (next ?? stream.next());
      next = null;
      if (first.done) {
        break;
      }
      const entries: ResultTuple[] = [entryFor(first, index++)];
      let done = false;
      let timer: ReturnType<typeof setTimeout>;
      const deadline = new Promise<null>((resolve) => {
        timer = setTimeout(resolve, 1, null);
      });
      try {
        while (entries.length < 100) {
          next = Promise.resolve(stream.next());
          const result = await Promise.race([next, deadline]);
          if (result === null) {
            break;
          }
          next = null;
          if (result.done) {
            done = true;
            break;
          }
          entries.push(entryFor(result, index++));
        }
      } finally {
        clearTimeout(timer!);
      }
      if (entries.length > 0) {
        // Batch caches for projected rows must become collectible as the
        // response advances. Sibling projections still share their batch.
        const meta =
          specs[0].bucket.layerPlan.operationPlan.makeMetaByMetaKey();
        const descendants: PromiseLike<void>[] = [];
        await Promise.all(
          specs.map((spec) => project(spec, entries, meta, descendants)),
        );
        if (descendants.length > 0) {
          const completion = Promise.all(descendants).then(() => {
            pendingBatches.delete(completion);
            batchFinished?.resolve();
          });
          pendingBatches.add(completion);
        }
      }
      if (done) {
        break;
      }
      // Let the next batch advance while the previous batch runs its defers
      // and streams. Bound the number of batches retained by slow descendants.
      if (pendingBatches.size >= 2) {
        batchFinished = Promise.withResolvers<void>();
        await abortable(
          requestContext.abortSignal,
          undefined,
          batchFinished.promise,
        );
        batchFinished = null;
        if (requestContext.abortSignal.aborted) {
          break;
        }
      }
    }
  } finally {
    await stream.return?.();
  }
  if (pendingBatches.size > 0) {
    await abortable(
      requestContext.abortSignal,
      undefined,
      Promise.all(pendingBatches),
    );
  }
}

function processSingleDeferred(
  requestContext: RequestTools,
  outputPlan: OutputPlan,
  specs: Array<[ResultIterator, SubsequentPayloadSpec]>,
  asString: boolean,
) {
  const size = specs.length;
  const store: Bucket["store"] = new Map();

  const polymorphicPathList: (string | null)[] = [];
  const iterators: Array<Set<AsyncIterator<any> | Iterator<any>>> = [];

  const firstBucket = specs[0][1].bucket;

  for (const copyStepId of outputPlan.layerPlan.copyStepIds) {
    const executionValue = firstBucket.store.get(copyStepId)!;
    if (executionValue!.isBatch) {
      const values = specs.map(([, spec]) =>
        executionValue.at(spec.bucketIndex),
      );
      store.set(
        copyStepId,
        batchExecutionValue(
          values,
          specs.map(([, spec]) => executionValue._flagsAt(spec.bucketIndex)),
        ),
      );
    } else {
      store.set(copyStepId, executionValue);
    }
  }

  for (let bucketIndex = 0; bucketIndex < size; bucketIndex++) {
    const spec = specs[bucketIndex][1];
    polymorphicPathList[bucketIndex] =
      spec.bucket.polymorphicPathList[spec.bucketIndex];
    iterators[bucketIndex] = new Set();
  }

  // const childBucket = newBucket(spec.outputPlan.layerPlan, noDepsList, store);
  // const childBucketIndex = 0;
  // Deferred payloads continue the originating execution and share its caches.
  const rootBucket = newBucket(firstBucket, {
    layerPlan: outputPlan.layerPlan,
    size,
    store,
    flagUnion: firstBucket.flagUnion,
    polymorphicPathList,
    polymorphicType: null,
    iterators,
  });

  const bucketPromise = executeBucket(rootBucket, requestContext);

  const output = (): void | Promise<void> => {
    const promises: PromiseLike<any>[] = [];
    for (let bucketIndex = 0; bucketIndex < size; bucketIndex++) {
      const [iterator, spec] = specs[bucketIndex];
      // NOTE: this is a deferred output plan, so it'll be a `defer` bucket and
      // an `object` outputPlan
      const [ctx, result] = outputBucket(
        spec.outputPlan,
        rootBucket,
        bucketIndex,
        requestContext,
        spec.path,
        spec.root.variables,
        asString,
      );
      const iteratorPayload = Object.create(null);
      if (result !== undefined) {
        iteratorPayload.data = result;
      }
      iteratorPayload.hasNext = true;
      if (spec.label != null) {
        iteratorPayload.label = spec.label;
      }
      if (ctx.root.errors.length > 0) {
        iteratorPayload.errors = ctx.root.errors;
      }
      // TODO: extensions?
      iteratorPayload.path = ctx.path;
      iterator.push(iteratorPayload);
      const promise = processRoot(ctx, iterator, asString);
      if (isPromiseLike(promise)) {
        promises.push(promise);
      }
    }
    if (promises.length !== 0) {
      return Promise.all(promises).then(noop);
    }
  };

  if (isPromiseLike(bucketPromise)) {
    return bucketPromise.then(output);
  } else {
    return output();
  }
}

interface DeferredBatch {
  entries: Array<[ResultIterator, SubsequentPayloadSpec]>;
  done: PromiseWithResolvers<void>;
}

// Unary values and shared execution state belong to the originating bucket.
const deferredBatches = new WeakMap<Bucket, Map<OutputPlan, DeferredBatch>>();

function processDeferred(
  requestContext: RequestTools,
  iterator: ResultIterator,
  spec: SubsequentPayloadSpec,
  outputDataAsString: boolean,
): PromiseLike<void> {
  let batches = deferredBatches.get(spec.bucket);
  if (batches === undefined) {
    batches = new Map();
    deferredBatches.set(spec.bucket, batches);
  }
  const existing = batches.get(spec.outputPlan);
  if (existing !== undefined) {
    existing.entries.push([iterator, spec]);
    return existing.done.promise;
  }
  const batch: DeferredBatch = {
    entries: [[iterator, spec]],
    done: Promise.withResolvers<void>(),
  };
  batches.set(spec.outputPlan, batch);
  setTimeout(() => {
    batches.delete(spec.outputPlan);
    if (requestContext.abortSignal.aborted) {
      batch.done.resolve();
      return;
    }
    try {
      batch.done.resolve(
        processSingleDeferred(
          requestContext,
          spec.outputPlan,
          batch.entries,
          outputDataAsString,
        ),
      );
    } catch (e) {
      batch.done.reject(e);
    }
  }, 1);
  return batch.done.promise;
}

/**
 * The promise has been resolved, but this may still be an AsyncGenerator.
 * If so, wrap it so that we know when the generator completes.
 */
function handleMaybeIterator(
  abortController: AbortController,
  result:
    | graphql.ExecutionResult
    | AsyncGenerator<graphql.AsyncExecutionResult, void, void>,
):
  | graphql.ExecutionResult
  | AsyncGenerator<graphql.AsyncExecutionResult, void, void> {
  if (Symbol.asyncIterator in result) {
    return asyncIteratorWithCleanup(
      result,
      abortController.abort.bind(abortController),
    );
  } else {
    abortController.abort();
    return result;
  }
}
