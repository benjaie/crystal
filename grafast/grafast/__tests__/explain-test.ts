import { expect } from "chai";
import { it } from "mocha";

import { getExplain } from "../dist/index.js";

it("expands permitted SQL explain scopes", () => {
  expect(
    getExplain(
      ["plan", "sql:explain:analyze", "sql:explain:buffers"],
      "plan,sql:explain:analyze,sql:explain:buffers",
    ),
  ).to.deep.equal([
    "plan",
    "sql",
    "sql:explain",
    "sql:explain:analyze",
    "sql:explain:buffers",
  ]);
});

it("does not infer server permission for parent SQL explain scopes", () => {
  expect(getExplain(["sql"], "sql:explain:analyze")).to.be.undefined;
});
