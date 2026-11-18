# AI assistant

## Status

Accepted

## Context

Users want to ask questions about a table in plain language. IceGraph is read-only, avoids extra services and persistent server state, and must work with any OpenAI-compatible model, including self-hosted ones such as LM Studio or Ollama with small context windows.

## Decision

The assistant is enabled only when `AI_API_BASE_URL` and `AI_MODEL` are set. Otherwise the Ask AI button is hidden and the question endpoint returns 404.

The browser builds the context. It sends only data it already loaded (table metadata, issues, snapshots, and graph nodes) and only the chips the user selected. The backend never collects data for the assistant and does not read the table again. Data file statistics are a separate chip because they can be large and contain real values.

One question is one background job:

1. The browser posts the question, the selected context, the page, the table name, and the snapshot range.
2. The backend rejects a body that is too large (413), invalid (400), or over `AI_MAX_CONTEXT_CHARACTERS` (413), counted in UTF-16 characters like the browser counter. It rejects a question when `AI_MAX_CONCURRENT_REQUESTS` answers are already running (429).
3. Otherwise it starts a job and returns its key and a token (202).
4. The job streams the answer from the model and appends each part to the job.
5. The browser polls the job every 400 ms with the token and shows the answer as it grows.
6. The job is removed `COMPUTE_CLEANUP_TIME_SECONDS` after it finishes.

`AI_REQUEST_TIMEOUT_SECONDS` limits the whole answer, not only the connection. Model errors, including errors sent in the middle of a stream, become readable messages (502).

The conversation is stored in `localStorage`, at most 50 messages, keyed by table and snapshot range on table views and by table on the snapshot selection page. Clear can be undone for 5 seconds.

History is what the browser sends besides the new question. The backend keeps none of it between requests, so the browser sends it again with every question.

- Each request has a `messages` list: the earlier messages, oldest first, then the new question as the last `user` message.
- With History on, the earlier messages are the latest saved messages, up to 12 (up to 6 questions and their answers). With History off, the list holds only the new question.
- A message is only a `role` (`user` or `assistant`) and its `content`: the text of the question or the full answer. The backend drops any other field.
- Only answered questions are saved, so a question that failed or was stopped never becomes history.
- The context of earlier questions is not saved or sent again. Each request carries only the chips selected now, and the backend puts that context in the system prompt, before the history.
- History counts toward `AI_MAX_CONTEXT_CHARACTERS`, the same as the context.

## Consequences

- No new service or persistent server state is required.
- Answers are lost if the backend restarts, and a finished answer is kept only briefly.
- Everything the user selects, including data file statistics, leaves the server and goes to the configured model.
- The character limit is enforced on both sides, but it counts characters, not tokens, and leaves out the system prompt and the answer. For a model with a small context window, set `AI_MAX_CONTEXT_CHARACTERS` well below its context length.
- Conversations stay in one browser and are not shared between users or devices.
