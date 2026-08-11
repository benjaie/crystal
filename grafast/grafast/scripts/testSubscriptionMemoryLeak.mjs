import { resolvePreset } from "graphile-config";

import { constant, grafast, makeGrafastSchema } from "../dist/index.js";

const DURATION_MS = 120_000;

const schema = makeGrafastSchema({
  typeDefs: /* GraphQL */ `
    type Query {
      dummy: Boolean!
    }

    type Subscription {
      counter: Int!
    }
  `,
  objects: {
    Query: {
      plans: {
        dummy() {
          return constant(true);
        },
      },
    },
    Subscription: {
      plans: {
        counter: {
          subscribe() {
            return increasingNumbers();
          },
          resolve(value) {
            return value;
          },
        },
      },
    },
  },
});

async function* increasingNumbers() {
  let value = 0;
  while (true) {
    yield ++value;
  }
}

function memoryUsage() {
  const { heapUsed, rss } = process.memoryUsage();
  return {
    heapUsedMiB: (heapUsed / 1024 / 1024).toFixed(1),
    rssMiB: (rss / 1024 / 1024).toFixed(1),
  };
}

if (typeof global.gc !== "function") {
  throw new Error("Run this script with Node's --expose-gc option.");
}

global.gc();
console.log("Memory before subscription:", memoryUsage());

const result = await grafast({
  schema,
  source: /* GraphQL */ `
    subscription {
      counter
    }
  `,
  resolvedPreset: resolvePreset({}),
});

if (!(Symbol.asyncIterator in result)) {
  throw new Error("Expected an async iterable subscription result.");
}

const iterator = result[Symbol.asyncIterator]();
const startedAt = performance.now();
let count = 0;
let sum = 0n;

try {
  while (performance.now() - startedAt < DURATION_MS) {
    const next = await iterator.next();
    if (next.done) {
      throw new Error("Subscription ended unexpectedly.");
    }

    const value = next.value.data?.counter;
    if (value !== count + 1) {
      throw new Error(
        `Expected event ${count + 1}, but received ${String(value)}.`,
      );
    }
    count++;
    sum += BigInt(value);
  }
} finally {
  await iterator.return?.();
}

const expectedSum = (BigInt(count) * BigInt(count + 1)) / 2n;
if (sum !== expectedSum) {
  throw new Error(`Expected sum ${expectedSum}, but received ${sum}.`);
}

global.gc();
console.log(`Delivered ${count.toLocaleString()} events in 120 seconds.`);
console.log(`Sum: ${sum}`);
console.log("Memory after subscription and GC:", memoryUsage());
