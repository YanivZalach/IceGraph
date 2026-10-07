import { Fragment, useRef, type MouseEvent } from "react";
import { optionKey, type GroupCounts, type TableOption } from "./tableOptions";
import TableOptionLabel from "./TableOptionLabel";
import TableOptionIcon from "./TableOptionIcon";
import { UI_FIELD_LABEL_CLASS, UI_HELPER_TEXT_CLASS } from "../../uiTypography";
import { cn } from "../../shared/lib/cn";

interface TableOptionListProps {
  listId: string;
  options: TableOption[];
  highlightedIndex: number;
  hiddenCount: number;
  groupCounts: GroupCounts;
  isLoading: boolean;
  optionId: (index: number) => string;
  onHighlight: (option: TableOption) => void;
  onOpen: (option: TableOption) => void;
  className: string;
}

const SKELETON_ROWS = [0, 1, 2, 3, 4];

const TableOptionList = ({
  listId,
  options,
  highlightedIndex,
  hiddenCount,
  groupCounts,
  isLoading,
  optionId,
  onHighlight,
  onOpen,
  className,
}: TableOptionListProps) => {
  const lastPointer = useRef({ x: -1, y: -1 });

  const handleMouseMove = (
    event: MouseEvent<HTMLLIElement>,
    option: TableOption,
  ): void => {
    const { clientX: x, clientY: y } = event;
    if (x === lastPointer.current.x && y === lastPointer.current.y) return;
    lastPointer.current = { x, y };
    onHighlight(option);
  };

  return (
    <div className={cn("overflow-y-auto", className)}>
      <ul id={listId} role="listbox" aria-label="Tables" className="pb-1">
        {options.map((option, index) => (
          <Fragment key={optionKey(option)}>
            {option.group !== options[index - 1]?.group &&
              (option.group === "Recent" || option.group === "Tables") && (
                <li
                  role="presentation"
                  className={cn(
                    "sticky top-0 z-10 flex items-center gap-2 bg-surface-deep px-4 pt-2.5 pb-1.5",
                    index > 0 && "mt-1.5 border-t border-edge",
                  )}
                >
                  <TableOptionIcon
                    group={option.group}
                    className="h-3.5 w-3.5 text-slate-500"
                  />
                  <span className={UI_FIELD_LABEL_CLASS}>{option.group}</span>
                  <span className="text-xs text-slate-400 tabular-nums">
                    {(groupCounts[option.group] ?? 0).toLocaleString()}
                  </span>
                </li>
              )}
            <li
              id={optionId(index)}
              role="option"
              aria-selected={index === highlightedIndex}
              title={option.group === "Typed" ? undefined : option.name}
              onMouseMove={(event) => {
                handleMouseMove(event, option);
              }}
              onClick={() => {
                onOpen(option);
              }}
              className={cn(
                "mx-1 flex min-h-11 cursor-pointer scroll-mt-10 items-center gap-3 rounded-md px-3 py-1.5",
                option.group === "Typed" && "mt-1 border-t border-edge",
                index === highlightedIndex && "bg-accent-muted",
              )}
            >
              <TableOptionIcon
                group={option.group}
                className={
                  index === highlightedIndex
                    ? "text-accent-text"
                    : "text-slate-500"
                }
              />
              <TableOptionLabel option={option} />
              {index === highlightedIndex && (
                <kbd className="ml-auto shrink-0 font-mono text-xs text-slate-400">
                  ↵
                </kbd>
              )}
            </li>
          </Fragment>
        ))}
        {isLoading &&
          SKELETON_ROWS.map((row) => (
            <li
              key={row}
              role="presentation"
              className="mx-1 flex min-h-11 flex-col justify-center gap-1.5 px-3"
            >
              <span className="h-3 w-40 animate-pulse rounded bg-edge" />
              <span className="h-2 w-24 animate-pulse rounded bg-edge" />
            </li>
          ))}
      </ul>
      {options.length === 0 && !isLoading && (
        <p className={cn(UI_HELPER_TEXT_CLASS, "px-4 py-6 text-center")}>
          No tables in the catalog. Type a full table name to open it.
        </p>
      )}
      {hiddenCount > 0 && (
        <p className={cn(UI_HELPER_TEXT_CLASS, "px-4 py-2")}>
          {hiddenCount.toLocaleString()} more tables. Keep typing to narrow the
          list.
        </p>
      )}
    </div>
  );
};

export default TableOptionList;
