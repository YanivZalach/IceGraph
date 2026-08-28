import { useRef, useState } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { cn } from "../../../shared/lib/cn";
import { useOutsideClick } from "../../../shared/lib/useOutsideClick";
import type { FileTreeViewSettings as FileTreeViewSettingsState } from "../hooks/useFileTreeState";
import type { Branch, SnapshotNode } from "../types";
import FileTreeSnapshotSelect from "./FileTreeSnapshotSelect";

interface FileTreeViewSettingsProps {
  branches: Branch[];
  currentSnapshotId: string;
  selectedBranchName: string | null;
  snapshots: SnapshotNode[];
  viewSettings: FileTreeViewSettingsState;
}

const CONTROL_CLASS =
  "w-full rounded-lg border border-edge bg-canvas px-3 py-2 text-sm text-ink focus:border-accent focus:outline-none";

const FileTreeViewSettings = ({
  branches,
  currentSnapshotId,
  selectedBranchName,
  snapshots,
  viewSettings,
}: FileTreeViewSettingsProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const close = () => {
    setIsOpen(false);
  };
  useHotkey("Escape", close, { enabled: isOpen });
  useOutsideClick(containerRef, close, isOpen);

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        aria-expanded={isOpen}
        onClick={() => {
          setIsOpen((current) => !current);
        }}
        className={cn(
          "flex cursor-pointer items-center gap-2 rounded-lg border px-3 py-1.5 text-sm transition",
          isOpen
            ? "border-accent bg-accent-muted text-white"
            : "border-edge bg-surface text-slate-300 hover:border-edge-hover",
        )}
      >
        <svg
          className="size-4"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          aria-hidden="true"
        >
          <path d="M2 4h12M4 8h8M6 12h4" strokeLinecap="round" />
          <circle cx="5" cy="4" r="1.2" fill="currentColor" />
          <circle cx="10" cy="8" r="1.2" fill="currentColor" />
          <circle cx="8" cy="12" r="1.2" fill="currentColor" />
        </svg>
        View settings
      </button>
      {isOpen && (
        <div
          role="dialog"
          aria-label="File tree view settings"
          className="absolute left-0 top-full z-50 mt-2 max-h-[calc(100dvh-8rem)] w-[min(40rem,calc(100vw-2rem))] overflow-y-auto rounded-xl border border-edge bg-surface p-4 shadow-2xl"
        >
          <h2 className="mb-4 text-sm font-bold text-ink">View settings</h2>
          <div className="flex flex-col gap-4">
            {branches.length > 0 && (
              <label className="flex flex-col gap-1.5 text-xs font-bold uppercase tracking-wide text-slate-500">
                Branch
                <select
                  value={selectedBranchName ?? ""}
                  onChange={(event) => {
                    viewSettings.setBranch(
                      event.target.value === "" ? null : event.target.value,
                    );
                  }}
                  className={CONTROL_CLASS}
                >
                  <option value="">All branches</option>
                  {branches.map((branch) => (
                    <option key={branch.name} value={branch.name}>
                      {branch.name}
                    </option>
                  ))}
                </select>
              </label>
            )}
            <FileTreeSnapshotSelect
              className={CONTROL_CLASS}
              currentSnapshotId={currentSnapshotId}
              onChange={viewSettings.setSnapshot}
              snapshots={snapshots}
            />
            <fieldset>
              <legend className="mb-1.5 text-xs font-bold uppercase tracking-wide text-slate-500">
                File scope
              </legend>
              <div className="grid grid-cols-2 overflow-hidden rounded-lg border border-edge">
                <button
                  type="button"
                  onClick={() => {
                    viewSettings.setScope("snapshot");
                  }}
                  className={cn(
                    "cursor-pointer px-3 py-2 text-sm",
                    viewSettings.scope === "snapshot"
                      ? "bg-accent text-white"
                      : "bg-canvas text-slate-400 hover:text-ink",
                  )}
                >
                  At snapshot
                </button>
                <button
                  type="button"
                  onClick={() => {
                    viewSettings.setScope("commit");
                  }}
                  className={cn(
                    "cursor-pointer border-l border-edge px-3 py-2 text-sm",
                    viewSettings.scope === "commit"
                      ? "bg-accent text-white"
                      : "bg-canvas text-slate-400 hover:text-ink",
                  )}
                >
                  Added in commit
                </button>
              </div>
            </fieldset>
            <fieldset>
              <legend className="mb-1.5 text-xs font-bold uppercase tracking-wide text-slate-500">
                Grouping
              </legend>
              <div className="grid grid-cols-2 overflow-hidden rounded-lg border border-edge">
                {(["flat", "tree"] as const).map((mode) => (
                  <button
                    key={mode}
                    type="button"
                    onClick={() => {
                      viewSettings.setViewMode(mode);
                    }}
                    className={cn(
                      "cursor-pointer px-3 py-2 text-sm capitalize first:border-r first:border-edge",
                      viewSettings.viewMode === mode
                        ? "bg-accent text-white"
                        : "bg-canvas text-slate-400 hover:text-ink",
                    )}
                  >
                    {mode}
                  </button>
                ))}
              </div>
            </fieldset>
          </div>
        </div>
      )}
    </div>
  );
};

export default FileTreeViewSettings;
