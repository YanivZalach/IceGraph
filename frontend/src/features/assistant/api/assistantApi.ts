import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { fetchFromApi } from "../../../shared/lib/api";
import type { GraphRequestParameters } from "../../table/api/graphCache";
import type { AssistantPage } from "../assistantContext";

const ASSISTANT_POLL_INTERVAL_MS = 400;

const assistantStatusSchema = z.object({
  enabled: z.boolean(),
  max_context_characters: z.number().int().nonnegative(),
});

export const assistantMessageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.string(),
});

const assistantJobSubmissionSchema = z.object({
  key: z.string(),
  status: z.literal("processing"),
  "X-IceGraph-Job-Token": z.string(),
});

const assistantAnswerSchema = z.object({
  key: z.string(),
  status: z.enum(["processing", "completed"]),
  answer: z.string(),
});

export type AssistantMessage = z.infer<typeof assistantMessageSchema>;

export interface AssistantQuestion {
  page: AssistantPage;
  context: string;
  messages: AssistantMessage[];
  parameters: GraphRequestParameters;
}

export const assistantStatusQueryOptions = () =>
  queryOptions({
    queryKey: ["assistant", "status"],
    queryFn: ({ signal }) =>
      fetchFromApi("/assistant", assistantStatusSchema, { signal }),
    retry: false,
    staleTime: Number.POSITIVE_INFINITY,
  });

const waitForNextPoll = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    const handleAbort = () => {
      window.clearTimeout(timeoutId);
      reject(new DOMException("Assistant request aborted", "AbortError"));
    };
    const timeoutId = window.setTimeout(() => {
      signal.removeEventListener("abort", handleAbort);
      resolve();
    }, ASSISTANT_POLL_INTERVAL_MS);
    signal.addEventListener("abort", handleAbort, { once: true });
  });

export const askAssistant = async (
  question: AssistantQuestion,
  onAnswerProgress: (answer: string) => void,
  signal: AbortSignal,
): Promise<string> => {
  const submittedJob = await fetchFromApi(
    "/assistant/messages",
    assistantJobSubmissionSchema,
    {
      jsonBody: {
        table_name: question.parameters.tableName,
        start_snapshot_id: question.parameters.startSnapshotId,
        end_snapshot_id: question.parameters.endSnapshotId,
        page: question.page,
        context: question.context,
        messages: question.messages,
      },
      method: "POST",
      signal,
    },
  );

  while (!signal.aborted) {
    const jobResult = await fetchFromApi(
      `/assistant/messages/${encodeURIComponent(submittedJob.key)}`,
      assistantAnswerSchema,
      {
        headers: {
          "X-IceGraph-Job-Token": submittedJob["X-IceGraph-Job-Token"],
        },
        signal,
      },
    );

    if (jobResult.status === "completed") return jobResult.answer;

    onAnswerProgress(jobResult.answer);
    await waitForNextPoll(signal);
  }

  throw new DOMException("Assistant request aborted", "AbortError");
};
