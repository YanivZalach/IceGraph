import { useEffect, useRef, useState } from "react";
import type { TableMetadata } from "../../table/api/metadataSchemas";
import {
  hasPreviousSpec,
  type SpecDetail,
  type SpecSelection,
  type SpecView,
} from "../tableSpecs";
import { cn } from "../../../shared/lib/cn";
import SpecDetailBody from "./SpecDetailBody";

interface SpecDetailPanelProps {
  metadata: TableMetadata;
  detail: SpecDetail;
  specView: SpecView;
  columnNames: ReadonlyMap<string, string>;
  onViewChange: (view: SpecView) => void;
  onClear: () => void;
}

const NO_PREVIOUS_LABELS: Record<SpecSelection["kind"], string> = {
  schema: "No previous schema",
  partition: "No previous partition spec",
  order: "No previous sort order",
};

const SpecDetailPanel = ({
  metadata,
  detail,
  specView,
  columnNames,
  onViewChange,
  onClear,
}: SpecDetailPanelProps) => {
  const panelRef = useRef<HTMLDivElement>(null);
  const [isJsonCopied, setIsJsonCopied] = useState(false);
  const hasPrevious = hasPreviousSpec(metadata, {
    kind: detail.type,
    id: detail.id,
  });
  const isDiff = specView === "diff" && hasPrevious;

  useEffect(() => {
    panelRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [detail.type, detail.id]);

  const handleCopyJson = (): void => {
    void navigator.clipboard.writeText(JSON.stringify(detail.data, null, 2));
    setIsJsonCopied(true);
    setTimeout(() => {
      setIsJsonCopied(false);
    }, 2000);
  };

  return (
    <div ref={panelRef} className="rounded-lg border-2 border-accent">
      <div className="flex items-center justify-between px-4 py-2 bg-accent">
        <span className="text-sm font-bold text-white">{detail.label}</span>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-0">
            <button
              type="button"
              aria-pressed={!isDiff}
              className={cn(
                "text-xs font-bold px-2 py-0.5 rounded-l-full border border-white/30 transition",
                isDiff
                  ? "bg-transparent text-white/70 hover:text-white"
                  : "bg-white text-accent",
              )}
              onClick={() => {
                onViewChange("full");
              }}
            >
              Full
            </button>
            <button
              type="button"
              aria-pressed={isDiff}
              disabled={!hasPrevious}
              title={
                hasPrevious
                  ? "Show diff to previous version"
                  : NO_PREVIOUS_LABELS[detail.type]
              }
              className={cn(
                "text-xs font-bold px-2 py-0.5 rounded-r-full border border-white/30 transition",
                isDiff
                  ? "bg-white text-accent"
                  : hasPrevious
                    ? "bg-transparent text-white/70 hover:text-white cursor-pointer"
                    : "bg-transparent text-white/30 cursor-not-allowed",
              )}
              onClick={() => {
                onViewChange("diff");
              }}
            >
              Diff
            </button>
          </div>
          <button
            type="button"
            className="text-xs font-bold px-2 py-0.5 rounded-full border border-white/30 bg-transparent text-white/70 hover:text-white transition cursor-pointer"
            onClick={handleCopyJson}
          >
            {isJsonCopied ? "✓ Copied" : "Copy JSON"}
          </button>
          <button
            type="button"
            aria-label="Clear selection"
            className="text-white/70 hover:text-white text-xl leading-none cursor-pointer transition"
            onClick={onClear}
          >
            ×
          </button>
        </div>
      </div>
      <div className="max-h-[300px] overflow-y-auto">
        <SpecDetailBody
          metadata={metadata}
          detail={detail}
          isDiff={isDiff}
          columnNames={columnNames}
        />
      </div>
    </div>
  );
};
export default SpecDetailPanel;
