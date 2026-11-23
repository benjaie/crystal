import type { BatchExecutor } from "../interfaces.ts";
import { arraysMatch, isPojo } from "../utils.ts";
import type { BatchFamily, BatchOptions } from "./batchInterfaces.ts";
import type { BatchTupleIndex } from "./batchTupleIndex.ts";

interface KeyOrder {
  symbolOrder: Map<symbol, number> | null;
}

/** @internal */
interface OptionsMatcher {
  matches(left: unknown, right: unknown): boolean;
  readonly leadingKey?: PropertyKey;
  /** Reads key components only as the trie needs them. */
  keys(value: unknown, order: KeyOrder): IterableIterator<unknown>;
}

const strictMatcher: OptionsMatcher = {
  matches: (left, right) => left === right,
  *keys(value) {
    yield value;
  },
};

const $$scalar = Symbol("non-structural batch option");

const tupleMatcher: OptionsMatcher = {
  matches: (left, right) =>
    left === right ||
    (Array.isArray(left) && Array.isArray(right) && arraysMatch(left, right)),
  *keys(value) {
    if (Array.isArray(value)) {
      yield value.length;
      for (let i = 0; i < value.length; i++) {
        yield value[i];
      }
    } else {
      yield $$scalar;
      yield value;
    }
  },
};

function symbolRank(order: Map<symbol, number>, key: symbol): number {
  let rank = order.get(key);
  if (rank === undefined) {
    rank = order.size;
    order.set(key, rank);
  }
  return rank;
}

function compareOptionKeys(
  a: string | symbol,
  z: string | symbol,
  order: KeyOrder,
): number {
  if (typeof a === "string") {
    return typeof z === "string" ? (a < z ? -1 : a === z ? 0 : 1) : -1;
  } else if (typeof z === "string") {
    return 1;
  } else {
    const ranks = (order.symbolOrder ??= new Map());
    return symbolRank(ranks, a) - symbolRank(ranks, z);
  }
}

function isOptionsObject(
  value: unknown,
): value is Record<PropertyKey, unknown> {
  return Array.isArray(value) || isPojo(value);
}

function makeObjectMatcher(
  rules?: Map<PropertyKey, OptionsMatcher>,
): OptionsMatcher {
  const leadingRule = rules?.entries().next().value;
  return {
    leadingKey: leadingRule?.[1] === strictMatcher ? leadingRule[0] : undefined,
    matches(left, right) {
      if (left === right) {
        return true;
      } else if (
        !isOptionsObject(left) ||
        !isOptionsObject(right) ||
        Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)
      ) {
        return false;
      }
      let matchedKeys = 0;
      if (rules !== undefined) {
        for (const [key, rule] of rules) {
          const hasKey = Object.hasOwn(left, key);
          if (hasKey !== Object.hasOwn(right, key)) {
            return false;
          }
          if (hasKey) {
            if (!rule.matches(left[key], right[key])) {
              return false;
            }
            matchedKeys++;
          }
        }
      }
      const keys = Reflect.ownKeys(left);
      if (keys.length !== Reflect.ownKeys(right).length) {
        return false;
      }
      if (keys.length === matchedKeys) {
        return true;
      }
      for (const key of keys) {
        if (
          !rules?.has(key) &&
          (!Object.hasOwn(right, key) || left[key] !== right[key])
        ) {
          return false;
        }
      }
      return true;
    },
    *keys(value, order) {
      if (!isOptionsObject(value)) {
        yield $$scalar;
        yield value;
        return;
      }
      yield Object.getPrototypeOf(value);
      if (rules !== undefined) {
        // Try explicit discriminators before enumerating the rest of the object.
        for (const [key, rule] of rules) {
          if (Object.hasOwn(value, key)) {
            yield key;
            if (rule === strictMatcher) {
              yield value[key];
            } else {
              yield* rule.keys(value[key], order);
            }
          }
        }
      }
      const ownKeys = Reflect.ownKeys(value);
      if (rules !== undefined) {
        let remaining = 0;
        for (const key of ownKeys) {
          if (!rules.has(key)) {
            ownKeys[remaining++] = key;
          }
        }
        ownKeys.length = remaining;
      }
      yield ownKeys.length;
      if (ownKeys.length > 1) {
        ownKeys.sort((a, z) => compareOptionKeys(a, z, order));
      }
      for (const key of ownKeys) {
        yield key;
        yield value[key];
      }
    },
  };
}

const recordMatcher = makeObjectMatcher();

interface OptionsMatchDescriptor {
  readonly [key: PropertyKey]:
    | "strict"
    | "tuple"
    | "record"
    | OptionsMatchDescriptor
    | undefined;
}

function compileOptionsMatch(
  descriptor: OptionsMatchDescriptor,
): OptionsMatcher {
  const rules = new Map<PropertyKey, OptionsMatcher>();
  for (const key of Reflect.ownKeys(descriptor)) {
    const rule = descriptor[key];
    if (rule === "tuple") {
      rules.set(key, tupleMatcher);
    } else if (rule === "record") {
      rules.set(key, recordMatcher);
    } else if (rule === "strict") {
      rules.set(key, strictMatcher);
    } else if (isPojo(rule)) {
      rules.set(key, compileOptionsMatch(rule));
    } else if (rule !== undefined) {
      throw new Error(`Invalid batch optionsMatch rule for '${String(key)}'`);
    }
  }
  return rules.size === 0 ? recordMatcher : makeObjectMatcher(rules);
}

const compiled = new WeakMap<OptionsMatchDescriptor, OptionsMatcher>();

/** @internal */
export function getOptionsMatcher(
  descriptor: OptionsMatchDescriptor | undefined,
): OptionsMatcher {
  if (descriptor === undefined) {
    return recordMatcher;
  }
  let matcher = compiled.get(descriptor);
  if (matcher === undefined) {
    matcher = compileOptionsMatch(descriptor);
    compiled.set(descriptor, matcher);
  }
  return matcher;
}

interface OptionsIndex<TOptions, TEntry> extends KeyOrder {
  byIdentity: Map<TOptions, TEntry> | null;
  keys: BatchTupleIndex<TEntry | TEntry[]>;
}

const $$endOfOptions = Symbol("end of batch options");
const $$missingOption = Symbol("missing leading batch option");

function leadingValue(options: BatchOptions, leadingKey: PropertyKey): unknown {
  return options != null && Object.hasOwn(options, leadingKey)
    ? (options as Record<PropertyKey, unknown>)[leadingKey]
    : $$missingOption;
}

function nextKey(keys: Iterator<unknown>): unknown {
  const next = keys.next();
  return next.done === true ? $$endOfOptions : next.value;
}

/**
 * The index only narrows the search: the matcher decides whether to merge.
 *
 * @internal
 */
export class BatchOptionsIndex<
  TOptions extends BatchOptions,
  TKeyset,
  TResult,
> {
  private first: BatchFamily<TOptions, TKeyset, TResult> | null = null;
  private index: OptionsIndex<
    TOptions,
    BatchFamily<TOptions, TKeyset, TResult>
  > | null = null;
  private readonly executor: BatchExecutor<TOptions, TKeyset, TResult>;
  private readonly matcher: OptionsMatcher;

  constructor(executor: BatchExecutor<TOptions, TKeyset, TResult>) {
    this.executor = executor;
    this.matcher = getOptionsMatcher(executor.optionsMatch);
  }

  private createFamily(
    options: TOptions,
  ): BatchFamily<TOptions, TKeyset, TResult> {
    return {
      executor: this.executor,
      options,
      queued: null,

      cache: new Map(),
      pending: new Map(),
      tupleKeyIndex: null,
      objectKeyIndexes: null,
    };
  }

  getOrCreate(options: TOptions): BatchFamily<TOptions, TKeyset, TResult> {
    const first = this.first;
    if (first === null) {
      return (this.first = this.createFamily(options));
    }

    let index = this.index;
    const identical = index?.byIdentity?.get(options);
    if (identical !== undefined) {
      return identical;
    }
    if (index === null && first.options === options) {
      return first;
    }

    const { leadingKey } = this.matcher;
    const value =
      leadingKey === undefined ? undefined : leadingValue(options, leadingKey);
    if (index === null) {
      const firstValue =
        leadingKey === undefined
          ? undefined
          : leadingValue(first.options, leadingKey);
      if (
        (leadingKey === undefined || firstValue === value) &&
        this.matcher.matches(first.options, options)
      ) {
        return first;
      }
      // Most executors have just one family. Build the index on the first miss.
      // A branch with a strict leading key can find its family directly.
      index = {
        symbolOrder: null,
        byIdentity:
          leadingKey === undefined ? new Map([[first.options, first]]) : null,
        keys: new Map(),
      };
      const firstKey =
        leadingKey === undefined
          ? nextKey(this.matcher.keys(first.options, index))
          : firstValue;
      index.keys.set(firstKey, first);
      this.index = index;
    }

    let keys: Iterator<unknown> | undefined;
    let node = index.keys;
    let previousKeys: Iterator<unknown> | undefined;
    for (let depth = 0; ; depth++) {
      // A strict discriminator can be read directly on the first branch.
      const key =
        depth === 0 && leadingKey !== undefined
          ? value
          : nextKey((keys ??= this.matcher.keys(options, index)));
      const entry = node.get(key);
      if (entry === undefined) {
        const family = this.createFamily(options);
        node.set(key, family);
        if (leadingKey === undefined || depth > 0) {
          index.byIdentity?.set(options, family);
        }
        return family;
      } else if (entry instanceof Map) {
        node = entry;
      } else if (Array.isArray(entry)) {
        for (const family of entry) {
          if (this.matcher.matches(family.options, options)) {
            return family;
          }
        }
        const family = this.createFamily(options);
        entry.push(family);
        index.byIdentity?.set(options, family);
        return family;
      } else {
        if (previousKeys === undefined) {
          if (this.matcher.matches(entry.options, options)) {
            return entry;
          }
        }
        if (key === $$endOfOptions) {
          // Key components can collide even when the options do not match.
          const family = this.createFamily(options);
          node.set(key, [entry, family]);
          index.byIdentity?.set(options, family);
          return family;
        }
        if (previousKeys === undefined) {
          // Cache identities once this family needs a structural lookup.
          (index.byIdentity ??= new Map()).set(entry.options, entry);
          // A leaf keeps its family. Matching components stay stable, so we
          // can replay its prefix only when another family needs this branch.
          previousKeys = this.matcher.keys(entry.options, index);
          // The iterator starts after the direct discriminator, if present.
          const consumed = leadingKey === undefined ? depth + 1 : depth;
          for (let i = 0; i < consumed; i++) {
            previousKeys.next();
          }
        }
        const children: typeof node = new Map();
        children.set(nextKey(previousKeys), entry);
        node.set(key, children);
        node = children;
      }
    }
  }
}
