/* eslint-disable graphile-export/exhaustive-deps, graphile-export/export-methods, graphile-export/export-plans, graphile-export/export-instances, graphile-export/no-nested */
import { expect } from "chai";
import { parse } from "graphql";
import { it } from "mocha";

import type { UnbatchedExecutionExtra } from "../dist/index.js";
import {
  execute,
  makeGrafastSchema,
  prepare,
  sideEffect,
  UnbatchedStep,
} from "../dist/index.js";

it("prepares an operation without executing it", async () => {
  let queryHelloPlans = 0;
  let executions = 0;
  const schema = makeGrafastSchema({
    typeDefs: /* GraphQL */ `
      type Query {
        hello: String!
      }
    `,
    objects: {
      Query: {
        plans: {
          hello() {
            queryHelloPlans++;
            return sideEffect(null, () => {
              executions++;
              return "world";
            });
          },
        },
      },
    },
    enableDeferStream: false,
  });
  const args = { schema, document: parse("query { hello }") };

  expect(executions).to.equal(0);
  expect(queryHelloPlans).to.equal(0);

  const prepared = prepare(args);
  if (prepared.errors) throw prepared.errors[0];
  expect(queryHelloPlans).to.equal(1);
  expect(executions).to.equal(0);

  const result = await execute(args);
  expect(result).to.deep.equal({ data: { hello: "world" } });
  expect(queryHelloPlans).to.equal(1);
  expect(executions).to.equal(1);

  const preparedAgain = prepare(args);
  if (preparedAgain.errors) throw preparedAgain.errors[0];
  expect(preparedAgain.operationPlan).to.equal(prepared.operationPlan);
  expect(queryHelloPlans).to.equal(1);
  expect(executions).to.equal(1);
});

it("aborts retained request signals, including signals first read after completion", () => {
  const requests: UnbatchedExecutionExtra["_requestContext"][] = [];
  const signals: AbortSignal[] = [];
  const aborted: AbortSignal[] = [];
  class ObserveRequestStep extends UnbatchedStep<number> {
    isSyncAndSafe = true;
    unbatchedExecute(extra: UnbatchedExecutionExtra) {
      const request = extra._requestContext;
      requests.push(request);
      if (requests.length === 2) {
        const signal = request.abortSignal;
        expect(signal.aborted).to.equal(false);
        signals.push(signal);
        signal.addEventListener("abort", () => aborted.push(signal));
      }
      return 1;
    }
  }

  const schema = makeGrafastSchema({
    typeDefs: "type Query { value: Int }",
    plans: { Query: { value: () => new ObserveRequestStep() } },
  });
  const args = { schema, document: parse("{ value }") };
  expect(execute(args)).to.deep.equal({ data: { value: 1 } });
  expect(execute(args)).to.deep.equal({ data: { value: 1 } });

  const lateSignal = requests[0].abortSignal;
  expect(lateSignal).to.equal(requests[0].abortSignal);
  expect(lateSignal).not.to.equal(signals[0]);
  expect(lateSignal.aborted).to.equal(true);
  expect(lateSignal.reason.name).to.equal("AbortError");
  expect(requests[1].abortSignal).to.equal(signals[0]);
  expect(aborted).to.deep.equal(signals);
});
