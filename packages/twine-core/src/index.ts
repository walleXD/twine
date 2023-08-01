import { ZodType, z } from "zod";

export type Context = Record<string, unknown>;
export type Input = unknown;
export type Options = { context: Context };

type ExecutionStep<I = Input, O = Input> = {
  name: string;
  fn: FunctionCallback<I, O> | Graph;
  inputSchema: ZodType<I, any, any>;
  type: "task" | "effect" | "map";
};

type FunctionCallback<I = Input, O = Input> = (
  input: I,
  options: Options
) => O | Promise<O>;

type Graph = {
  executionSteps: ExecutionStep[];
};

/* function isGraph(value: Graph | FunctionCallback): value is Graph { */
/*   return (value as Graph).executionSteps !== undefined; */
/* } */

// Utility function to check if 'fn' is a FunctionCallback
function isFunctionCallback<I, O>(
  fn: FunctionCallback<I, O> | Graph
): fn is FunctionCallback<I, O> {
  return typeof fn === "function";
}

// Utility function to check if 'fn' is a Graph
function isGraph(fn: FunctionCallback<any, any> | Graph): fn is Graph {
  return (fn as Graph).executionSteps !== undefined;
}

function createExecutionStep<I, O>(
  fn: FunctionCallback<I, O> | Graph,
  name: string,
  inputSchema: ZodType<any, any, any>,
  type: "task" | "effect" | "map"
): ExecutionStep {
  // Check if 'fn' is a Graph, if so, handle it accordingly
  if (isGraph(fn)) {
    return {
      fn,
      name,
      inputSchema,
      type,
    };
  }

  // If 'fn' is not a Graph, we check if it has the correct signature
  // and cast it to FunctionCallback<unknown, unknown> before using it
  if (isFunctionCallback<I, O>(fn)) {
    return {
      fn: fn as FunctionCallback<unknown, unknown>,
      name,
      inputSchema,
      type,
    };
  }

  throw new Error("Invalid function or graph passed to addExecutionStep");
}

export function createTwine() {
  const graph: Graph = {
    executionSteps: [],
  };

  function addExecutionStep<I = unknown, O = unknown>(
    name: string,
    fn: FunctionCallback<I, O> | Graph,
    inputSchema: ZodType<I, any, any>,
    type: "task" | "effect" | "map"
  ) {
    graph.executionSteps.push(createExecutionStep(fn, name, inputSchema, type));
  }

  return {
    task<I = Input, O = Input>(
      name: string,
      inputSchema: ZodType<I, any, any>,
      fn: FunctionCallback<I, O>
    ) {
      addExecutionStep(name, fn, inputSchema, "task");
      return this;
    },

    effect<I = Input, O = Input>(
      name: string,
      inputSchema: ZodType<I, any, any>,
      fn: FunctionCallback<I, O>
    ) {
      addExecutionStep(name, fn, inputSchema, "effect");
      return this;
    },

    map<I = Input, O = Input>(
      name: string,
      inputSchema: ZodType<I, any, any>,
      fn: FunctionCallback<I, O> | Graph
    ) {
      const schema = z.array(inputSchema) as unknown as ZodType<I, any, any>;
      addExecutionStep(name, fn, schema, "map");
      return this;
    },

    build() {
      return graph;
    },
  };
}

export function bootstrap(graph: Graph) {
  return async function run(
    initialInput: unknown,
    context: Context = {}
  ): Promise<unknown> {
    let currentData = initialInput;

    for (const step of graph.executionSteps) {
      if (!step.inputSchema.safeParse(currentData).success) {
        throw new Error(
          `Invalid input for execution step "${step.name}": ${currentData}`
        );
      }

      if (step.type === "map") {
        if (!Array.isArray(currentData)) {
          throw new Error(
            `Map function expected an array as input but received: ${currentData}`
          );
        }

        if (isGraph(step.fn)) {
          const runner = bootstrap(step.fn);
          currentData = await Promise.all(
            currentData.map((data) => runner(data, context))
          );
        } else {
          currentData = await Promise.all(
            currentData.map((data) =>
              (step.fn as FunctionCallback)(data, { context })
            )
          );
        }
      } else {
        if (isGraph(step.fn)) {
          const runner = bootstrap(step.fn);
          currentData = await runner(currentData, context);
        } else {
          currentData = await (step.fn as FunctionCallback)(currentData, {
            context,
          });
        }
      }
    }

    return currentData;
  };
}
