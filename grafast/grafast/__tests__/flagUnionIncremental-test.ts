/* eslint-disable graphile-export/exhaustive-deps, graphile-export/export-methods, graphile-export/export-plans, graphile-export/export-instances, graphile-export/export-subclasses, graphile-export/no-nested */
import { expect } from "chai";
import { resolvePreset } from "graphile-config";
import { it } from "mocha";

import type { AsyncExecutionResult } from "graphql";
import type { UnbatchedExecutionExtra } from "../dist/index.js";
import {
  DEFAULT_ACCEPT_FLAGS,
  TRAP_ERROR,
  UnbatchedStep,
  constant,
  flagError,
  grafast,
  lambda,
  makeGrafastSchema,
  Step,
} from "../dist/index.js";

import { resolveStreamDefer, streamToArray } from "./incrementalUtils.ts";

const resolvedPreset = resolvePreset({});
const requestContext = {};

class FlagUnionCheckStep extends UnbatchedStep<number> {
  isSyncAndSafe = false;
  constructor($dep: Step<unknown>) {
    super();
    this.addDependency({
      step: $dep,
      acceptFlags: DEFAULT_ACCEPT_FLAGS | TRAP_ERROR,
    });
  }
  unbatchedExecute(
    _extra: UnbatchedExecutionExtra,
    _value: unknown,
  ): number {
    if ((_extra._bucket.flagUnion & TRAP_ERROR) === 0) {
      throw new Error("Missing flagUnion error flag");
    }
    return 1;
  }
}

it("carries error flags into stream buckets", async () => {
  let $bad: Step<unknown> | null = null;
  const schema = makeGrafastSchema({
    typeDefs: /* GraphQL */ `
      type Thing {
        id: Int
        check: Int
      }
      type Query {
        list: [Thing!]!
      }
    `,
    objects: {
      Query: {
        plans: {
          list() {
            $bad = lambda(null, () =>
              flagError(new Error("Root error flag")),
            );
            return constant([1, 2]);
          },
        },
      },
      Thing: {
        plans: {
          id($i: Step<number>) {
            return $i;
          },
          check() {
            if ($bad == null) {
              throw new Error("Expected $bad to be initialised");
            }
            return new FlagUnionCheckStep($bad);
          },
        },
      },
    },
    enableDeferStream: true,
  });

  const source = /* GraphQL */ `
    {
      list @stream(initialCount: 0) {
        id
        check
      }
    }
  `;

  const result = await grafast({
    schema,
    source,
    resolvedPreset,
    requestContext,
  });
  const payloads = (await streamToArray(result)) as AsyncExecutionResult[];
  payloads.forEach((payload) => {
    expect(payload.errors).to.equal(undefined);
  });
  const merged = resolveStreamDefer(payloads);
  expect(merged.data).to.deep.equal({
    list: [
      { id: 1, check: 1 },
      { id: 2, check: 1 },
    ],
  });
});

it("carries error flags into deferred buckets", async () => {
  let $bad: Step<unknown> | null = null;
  const schema = makeGrafastSchema({
    typeDefs: /* GraphQL */ `
      type Thing {
        id: Int
        check: Int
      }
      type Query {
        thing: Thing
      }
    `,
    objects: {
      Query: {
        plans: {
          thing() {
            $bad = lambda(null, () =>
              flagError(new Error("Root error flag")),
            );
            return constant(1);
          },
        },
      },
      Thing: {
        plans: {
          id($i: Step<number>) {
            return $i;
          },
          check() {
            if ($bad == null) {
              throw new Error("Expected $bad to be initialised");
            }
            return new FlagUnionCheckStep($bad);
          },
        },
      },
    },
    enableDeferStream: true,
  });

  const source = /* GraphQL */ `
    {
      thing {
        id
        ... @defer {
          check
        }
      }
    }
  `;

  const result = await grafast({
    schema,
    source,
    resolvedPreset,
    requestContext,
  });
  const payloads = (await streamToArray(result)) as AsyncExecutionResult[];
  payloads.forEach((payload) => {
    expect(payload.errors).to.equal(undefined);
  });
  const merged = resolveStreamDefer(payloads);
  expect(merged.data).to.deep.equal({
    thing: { id: 1, check: 1 },
  });
});
