import JSONbig from "json-bigint";
import { z } from "zod";
import { env } from "./env";

const jsonWithBigIntsAsStrings = JSONbig({ storeAsString: true });

const errorBodySchema = z.object({ error: z.string() });

export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, body: unknown) {
    const parsedErrorBody = errorBodySchema.safeParse(body);
    super(
      parsedErrorBody.success
        ? parsedErrorBody.data.error
        : `Request failed with status ${String(status)}`,
    );
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

interface ApiRequestOptions {
  formBody?: URLSearchParams;
  headers?: Record<string, string>;
  jsonBody?: unknown;
  method?: "GET" | "POST";
  signal?: AbortSignal;
}

export const fetchFromApi = async <Output>(
  path: string,
  schema: z.ZodType<Output>,
  options: ApiRequestOptions = {},
): Promise<Output> => {
  const headers = new Headers(options.headers);
  let body: string | undefined;

  if (options.formBody !== undefined) {
    headers.set("Content-Type", "application/x-www-form-urlencoded");
    body = options.formBody.toString();
  } else if (options.jsonBody !== undefined) {
    headers.set("Content-Type", "application/json");
    body = jsonWithBigIntsAsStrings.stringify(options.jsonBody);
  }

  const response = await fetch(`${env.apiBaseUrl}${path}`, {
    method: options.method ?? "GET",
    ...(body === undefined ? {} : { body }),
    headers,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });

  const responseText = await response.text();
  const responseBody: unknown =
    responseText === "" ? null : jsonWithBigIntsAsStrings.parse(responseText);

  if (!response.ok) {
    throw new ApiError(response.status, responseBody);
  }

  return schema.parse(responseBody);
};
