import { formatAbsoluteTimestamp } from "../../lib/format/formatTimelineTime";
import type { GapRow } from "../../lib/timelineRow";

interface GapDetailsProps {
  row: GapRow;
}

const GapDetails = ({ row }: GapDetailsProps) => (
  <div className="flex flex-col gap-4">
    <p className="rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
      These events could not be read. An event right after them shows no
      description, because what it changed cannot be worked out.
    </p>
    <ul className="flex flex-col gap-3">
      {row.unreadableFiles.map((file) => (
        <li
          key={file.path}
          className="flex flex-col gap-1 border-b border-edge pb-3 last:border-0"
        >
          <span className="font-mono text-xs break-all text-slate-200">
            {file.path}
          </span>
          <span className="text-xs text-slate-400">
            {formatAbsoluteTimestamp(file.timestampMs)}
          </span>
          {file.reasons.map((reason) => (
            <span
              key={reason}
              className="text-xs break-words whitespace-pre-line text-amber-300"
            >
              {reason}
            </span>
          ))}
        </li>
      ))}
    </ul>
  </div>
);

export default GapDetails;
