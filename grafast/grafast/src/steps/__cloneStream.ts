import { $$deepDepSkip } from "../constants.ts";
import type { ExecutionDetails } from "../interfaces.ts";
import type { ListCapableStep } from "../step.ts";
import { Step } from "../step.ts";
import { MaterializedList } from "../streamItems.ts";
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
    return indexMap((i) => {
      const value = val.at(i);
      return value instanceof MaterializedList ? value.read() : value;
    });
  }
}

export function __cloneStream($dep: Step) {
  return new __CloneStreamStep($dep);
}
