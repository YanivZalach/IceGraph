import { type ReactNode, useId, useRef, useState } from "react";
import SchemaFieldList from "./SchemaFieldList";
import { cn } from "../../../shared/lib/cn";

interface ExpandableSchemaSectionProps {
  ariaLabel: string;
  title: string;
  headerActions?: ReactNode;
  schema: unknown;
  showIds?: boolean;
  children: ReactNode;
}

const ExpandableSchemaSection = ({
  ariaLabel,
  title,
  headerActions,
  schema,
  showIds = true,
  children,
}: ExpandableSchemaSectionProps) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const panelId = useId();
  const panelRef = useRef<HTMLDivElement>(null);

  return (
    <section
      aria-label={ariaLabel}
      className="overflow-hidden rounded-xl border border-edge bg-surface"
    >
      <div className="p-5">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="text-base font-semibold text-ink">{title}</h2>
          {headerActions}
          <button
            type="button"
            onClick={() => {
              const willExpand = !isExpanded;
              setIsExpanded(willExpand);
              if (willExpand) panelRef.current?.focus({ preventScroll: true });
            }}
            aria-expanded={isExpanded}
            aria-controls={panelId}
            className="ml-auto cursor-pointer rounded-lg border border-edge px-3 py-2 text-xs text-ink hover:border-accent"
          >
            {isExpanded ? "Collapse schema" : "Expand schema"}
          </button>
        </div>
        {children}
      </div>
      <div
        ref={panelRef}
        id={panelId}
        data-metadata-scroll
        tabIndex={0}
        role="region"
        aria-label="Schema fields"
        className={cn(
          "overflow-auto border-t border-edge [scrollbar-gutter:stable] focus-visible:outline-2 focus-visible:outline-accent",
          !isExpanded && "max-h-96",
        )}
      >
        <SchemaFieldList schema={schema} showIds={showIds} />
      </div>
    </section>
  );
};

export default ExpandableSchemaSection;
