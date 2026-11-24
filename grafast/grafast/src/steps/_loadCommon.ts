import type { BatchTupleIndex } from "../batch/batchTupleIndex.ts";
import { internBatchTupleEntry } from "../batch/batchTupleIndex.ts";
import type { OperationPlan } from "../engine/OperationPlan.ts";
import type {
  BatchExecutor,
  ExecutionDetails,
  ExecutionResultValue,
  PromiseOrDirect,
} from "../interfaces.ts";
import type { Multistep, UnwrapMultistep } from "../multistep.ts";
import { isListLikeStep, isObjectLikeStep, Step } from "../step.ts";
import {
  arrayOfLength,
  arraysMatch,
  isTuple,
  recordsMatch,
  stableStringSortFirstTupleEntry,
} from "../utils.ts";
import { access } from "./access.ts";
import type { LoadOneInfo } from "./loadOne.ts";

export type IOEquivalence<TSpec> =
  | null
  | string
  | { [key in Exclude<keyof TSpec, keyof any[]>]?: string | null };

export function makeAccessMap<TLookup extends Multistep>(
  $spec: Step,
  ioEquivalence: IOEquivalence<UnwrapMultistep<TLookup>>,
): Record<string, Step> {
  const map = Object.create(null) as Record<string, Step>;
  if (ioEquivalence == null) {
    return map;
  } else if (typeof ioEquivalence === "string") {
    map[ioEquivalence] = $spec;
    return map;
  } else if (isTuple(ioEquivalence)) {
    for (let i = 0, l = ioEquivalence.length; i < l; i++) {
      const key = ioEquivalence[i];
      map[key] = isListLikeStep($spec) ? $spec.at(i) : access($spec, [i]);
    }
    return map;
  } else if (typeof ioEquivalence === "object") {
    for (const key of Object.keys(ioEquivalence)) {
      const attr = ioEquivalence[key];
      if (attr != null) {
        map[attr] = isObjectLikeStep($spec)
          ? $spec.get(key)
          : access($spec, [key]);
      }
    }
    return map;
  } else {
    throw new Error(`ioEquivalence passed to loadOne() call not understood`);
  }
}

export function ioEquivalenceMatches(
  io1: IOEquivalence<Multistep>,
  io2: IOEquivalence<Multistep>,
): boolean {
  if (io1 === io2) return true;

  if (io1 == null) return false;
  if (io2 == null) return false;

  if (typeof io1 === "string") return false;
  if (typeof io2 === "string") return false;
  if (Array.isArray(io1)) {
    if (!Array.isArray(io2)) return false;
    return arraysMatch(io1, io2);
  } else {
    if (Array.isArray(io2)) return false;
    return recordsMatch(io1, io2);
  }
}

export function paramSig(
  paramDepIdByKey: Record<string, number>,
  depIdToStepId: (depId: number) => number,
): string {
  // No more params allowed!
  Object.freeze(paramDepIdByKey);
  return JSON.stringify(
    Object.fromEntries(
      Object.entries(paramDepIdByKey)
        .map(([key, depId]) => [key, depIdToStepId(depId)] as const)
        .sort(stableStringSortFirstTupleEntry),
    ),
  );
}

export type LoadBatchOptions<
  TSpec,
  TItem,
  TData,
  TParams extends Record<string, any>,
  TShared,
> = {
  readonly load: LoadCallback<TSpec, TItem, TData, TParams, TShared>;
  readonly attributes: (keyof TItem)[];
  readonly shared: TShared | undefined;
  readonly params: Readonly<Partial<TParams>>;
};

const EMPTY_OBJECT = Object.freeze({});
const $$loadPlanningMeta = Symbol("load planning meta");

export function internLoadOptions<
  TSpec,
  TItem,
  TData,
  TParams extends Record<string, any>,
  TShared,
>(
  operationPlan: OperationPlan,
  load: LoadCallback<TSpec, TItem, TData, TParams, TShared>,
  attributes: (keyof TItem)[],
): LoadBatchOptions<TSpec, TItem, TData, TParams, TShared> {
  type Options = LoadBatchOptions<TSpec, TItem, TData, TParams, TShared>;
  type Entry = {
    attributes: (keyof TItem)[];
    optionsByLoad: Map<typeof load, Options>;
  };
  const meta = operationPlan.getPlanningMeta($$loadPlanningMeta);
  const attributeIndex: BatchTupleIndex<Entry> = (meta.attributeIndex ??=
    new Map());
  const entry = internBatchTupleEntry(
    attributeIndex,
    attributes,
    (entry) => entry.attributes,
    (attributes) => {
      Object.freeze(attributes);
      return { attributes, optionsByLoad: new Map() };
    },
  );
  let options = entry.optionsByLoad.get(load);
  if (options === undefined) {
    options = {
      load,
      attributes: entry.attributes,
      shared: undefined,
      params: EMPTY_OBJECT,
    };
    entry.optionsByLoad.set(load, options);
  }
  return options;
}

function executeLoadBatch<
  TSpec,
  TItem,
  TData,
  TParams extends Record<string, any>,
  TShared,
>(
  {
    load,
    attributes,
    shared: sharedValue,
    params,
  }: LoadBatchOptions<TSpec, TItem, TData, TParams, TShared>,
  specs: readonly TSpec[],
): PromiseOrDirect<ReadonlyArray<PromiseOrDirect<TData>>> {
  const shared = sharedValue as TShared;
  return load(specs, {
    shared,
    attributes,
    params,
    unary: shared,
  });
}

// Preserve the generic callbacks: each use specializes this same executor
// object, without erasing the relationship between its options, keys and data.
const loadBatchExecutor = {
  name: "grafast/load",
  keysetEquality: "strict",
  callback: executeLoadBatch,
  optionsMatch: {
    load: "strict",
    params: "record",
    shared: "strict",
    attributes: "tuple",
  },
} satisfies BatchExecutor<
  LoadBatchOptions<unknown, unknown, unknown, Record<string, unknown>, unknown>,
  unknown,
  unknown
>;

const loadBatchExecutors = {
  strict: loadBatchExecutor,
  tuple: {
    ...loadBatchExecutor,
    name: "grafast/load/tuple",
    keysetEquality: "tuple" as const,
  },
  object: {
    ...loadBatchExecutor,
    name: "grafast/load/object",
    keysetEquality: "object" as const,
  },
};

export function getLoadBatchExecutor(lookup: Multistep) {
  return lookup == null || lookup instanceof Step
    ? loadBatchExecutors.strict
    : isTuple(lookup)
      ? loadBatchExecutors.tuple
      : loadBatchExecutors.object;
}

type LoadBatchExecutor = ReturnType<typeof getLoadBatchExecutor>;

export function getLoadParamEntries(
  sharedDepId: number | null,
  paramDepIdByKey: Record<string, number>,
): readonly (readonly [string, number])[] | null {
  const entries = Object.entries(paramDepIdByKey);
  // Null means the entire options object can be used as planned.
  return sharedDepId === null && entries.length === 0 ? null : entries;
}

type LoadCallback<
  TSpec,
  TItem,
  TData,
  TParams extends Record<string, any>,
  TShared,
> = (
  specs: readonly TSpec[],
  info: LoadOneInfo<TItem, TParams, TShared>,
) => PromiseOrDirect<ReadonlyArray<PromiseOrDirect<TData>>>;

export function executeLoad<
  TSpec,
  TItem,
  TData,
  TParams extends Record<string, any>,
  TShared,
>(
  details: ExecutionDetails<readonly [TSpec, ...any[]]>,
  sharedDepId: number | null,
  paramEntries: readonly (readonly [string, number])[] | null,
  plannedOptions: LoadBatchOptions<TSpec, TItem, TData, TParams, TShared>,
  executor: LoadBatchExecutor,
): PromiseOrDirect<
  ReadonlyArray<PromiseOrDirect<ExecutionResultValue<TData>>>
> {
  const { count, values } = details;
  const values0 = values[0];
  let options = plannedOptions;
  if (paramEntries !== null) {
    options = {
      load: plannedOptions.load,
      attributes: plannedOptions.attributes,
      shared:
        sharedDepId !== null ? values[sharedDepId].unaryValue() : undefined,
      params:
        paramEntries.length > 0
          ? (Object.fromEntries(
              paramEntries.map(([key, depId]) => [
                key,
                values[depId].unaryValue(),
              ]),
            ) as Partial<TParams>)
          : EMPTY_OBJECT,
    };
  }

  return details.batch<
    LoadBatchOptions<TSpec, TItem, TData, TParams, TShared>,
    TSpec,
    PromiseOrDirect<TData>
  >(
    executor,
    values0.isBatch ? values0.entries : arrayOfLength(count, values0.value),
    options,
  );
}
