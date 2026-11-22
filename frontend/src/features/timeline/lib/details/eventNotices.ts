import type { MetadataFileNode, SnapshotNode } from "../../api/nodeSchemas";
import type { CommitRow } from "../timelineRow";

export interface DetailNotice {
  kind: "error" | "warning";
  text: string;
}

const nodeNotices = (
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
): DetailNotice[] => {
  const noticesOf = (
    kind: DetailNotice["kind"],
    texts: string[],
  ): DetailNotice[] => texts.map((text) => ({ kind, text }));
  const candidates = [
    ...noticesOf("error", file.errors),
    ...noticesOf("error", snapshot?.errors ?? []),
    ...noticesOf("warning", file.warnings),
    ...noticesOf("warning", snapshot?.warnings ?? []),
  ];

  const seenTexts = new Set<string>();
  const notices: DetailNotice[] = [];
  for (const { kind, text } of candidates) {
    if (text !== "" && !seenTexts.has(text)) {
      seenTexts.add(text);
      notices.push({ kind, text });
    }
  }
  return notices;
};

const UNKNOWN_CHANGE_NOTICE: DetailNotice = {
  kind: "warning",
  text: "The event before this one could not be read or is not loaded, so what this event changed cannot be shown. Everything below is this event's own state.",
};

export const buildEventNotices = (
  row: CommitRow,
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
): DetailNotice[] => [
  ...(row.kind === "unknown-change" ? [UNKNOWN_CHANGE_NOTICE] : []),
  ...nodeNotices(file, snapshot),
];
