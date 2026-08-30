import type { MouseEvent } from "react";

interface GraphLinkButtonProps {
  label: string;
  onOpen: (event: MouseEvent<HTMLButtonElement>) => void;
}

const GraphLinkButton = ({ label, onOpen }: GraphLinkButtonProps) => (
  <button
    type="button"
    onClick={onOpen}
    title={`Open ${label.toLowerCase()} in a new tab, selected in the graph`}
    className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-accent/40 px-2.5 py-1.5 text-xs font-bold tracking-wide text-accent uppercase transition-colors hover:border-accent hover:bg-accent-muted"
  >
    <svg
      className="h-3.5 w-3.5 shrink-0"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
    >
      <circle cx="4" cy="8" r="2" />
      <circle cx="12" cy="4" r="2" />
      <circle cx="12" cy="12" r="2" />
      <path d="M6 7.2L10 4.8M6 8.8L10 11.2" strokeLinecap="round" />
    </svg>
    {label}
  </button>
);

export default GraphLinkButton;
