import { $$deepDepSkip } from "../constants.ts";
import type { ExecutionDetails } from "../interfaces.ts";
import type { ListCapableStep } from "../step.ts";
import { Step } from "../step.ts";
import type { __ItemStep } from "./__item.ts";

export class __CloneStreamStep extends Step {
  static $$export = {
    moduleName: "grafast",
    exportName: "__CloneStreamStep",
  };
  public isSyncAndSafe = false;
  constructor($dep: Step) {
    super();
    this.addDependency($dep);
    if (
      this.layerPlan.ancestry.some((layer) => layer.reason.type === "listItem")
    ) {
      // Each list position must open its own iterator, even for a unary source.
      this.operationPlan.stepTracker.setNonUnary(this, []);
    }
  }
  [$$deepDepSkip](): Step {
    return this.getDepOptions(0).step;
  }
  listItem($item: __ItemStep<any>): Step {
    const $dep = this.getDepOptions(0).step as Step &
      Partial<ListCapableStep<any, any, any>>;
    return $dep.listItem?.($item) ?? $item;
  }
  optimize() {
    // IMPORTANT: optimization is handled in OperationPlan's inlineSteps()
    return this;
  }
  execute({ values: [val], indexMap }: ExecutionDetails) {
    // executeBucket obtains this field's iterator and walks its initial items.
    if (val.isBatch) {
      return val.entries;
    } else {
      const v = val.value;
      return indexMap(() => v);
    }
  }
}

export function __cloneStream($dep: Step) {
  return new __CloneStreamStep($dep);
}
