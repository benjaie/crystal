import type { FragmentDefinitionNode, OperationDefinitionNode } from "graphql";
import { getVariableValues, GraphQLError } from "graphql";
import { Kind } from "graphql/language/kinds.js";

import type { GrafastExecutionArgs } from ".";

interface GrafastExecutionContext {
  operation: OperationDefinitionNode;
  fragments: Record<string, FragmentDefinitionNode>;
  variableValues: Record<string, any>;
}

/**
 * Constructs a ExecutionContext object from the arguments passed to
 * execute, which we will pass throughout the other execution methods.
 *
 * Throws a GraphQLError if a valid execution context cannot be created.
 *
 * @internal
 */
export function buildExecutionContext(
  args: GrafastExecutionArgs,
): ReadonlyArray<GraphQLError> | GrafastExecutionContext {
  const {
    schema,
    document,
    variableValues: rawVariableValues,
    operationName,
    options,
  } = args;

  let operation: OperationDefinitionNode | undefined;
  const fragments: Record<string, FragmentDefinitionNode> = Object.create(null);
  for (const definition of document.definitions) {
    switch (definition.kind) {
      case Kind.OPERATION_DEFINITION:
        if (operationName == null) {
          if (operation !== undefined) {
            return [
              new GraphQLError(
                "Must provide operation name if query contains multiple operations.",
              ),
            ];
          }
          operation = definition;
        } else if (definition.name?.value === operationName) {
          operation = definition;
        }
        break;
      case Kind.FRAGMENT_DEFINITION:
        fragments[definition.name.value] = definition;
        break;
      default:
      // ignore non-executable definitions
    }
  }

  if (!operation) {
    if (operationName != null) {
      return [new GraphQLError(`Unknown operation named "${operationName}".`)];
    }
    return [new GraphQLError("Must provide an operation.")];
  }

  const variableDefinitions = operation.variableDefinitions ?? [];

  const coercedVariableValues = getVariableValues(
    schema,
    variableDefinitions,
    rawVariableValues ?? {},
    { maxErrors: options?.maxCoercionErrors ?? 50 },
  );

  if (coercedVariableValues.errors) {
    return coercedVariableValues.errors;
  }

  // V16/V17 compatibility
  const targetObj =
    "variableValues" in coercedVariableValues
      ? (coercedVariableValues.variableValues as {
          coerced: Record<string, unknown>;
        })
      : (coercedVariableValues as { coerced: Record<string, unknown> });

  const coerced = targetObj.coerced;
  if (!coerced) {
    throw new Error("GraphQL v16/v17 compatibility error");
  }

  return {
    fragments,
    operation,
    variableValues: coerced,
  };
}
