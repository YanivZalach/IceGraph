import { useRef, useState } from "react";
import TableSpotlight from "./TableSpotlight";
import { rememberRecentTable } from "../catalog/recentTables";
import { BASE_PATH, IS_MOCK, MOCK_TABLE } from "../../appConstants";
import { UI_TABLE_NAME_BUTTON_CLASS } from "../../uiTypography";
import { cn } from "../../shared/lib/cn";

interface TablePickerProps {
  tableName: string;
}

const tableUrl = (tableName: string): string =>
  `${BASE_PATH}/snapshots-selection?${new URLSearchParams({ table: tableName }).toString()}`;

const TablePicker = ({ tableName }: TablePickerProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const handleClose = (): void => {
    setIsOpen(false);
    triggerRef.current?.focus();
  };

  const handleOpenTable = (name: string): void => {
    const nextTableName = IS_MOCK ? MOCK_TABLE : name.trim();
    if (nextTableName === "") return;
    rememberRecentTable(nextTableName);
    window.open(tableUrl(nextTableName), "_blank", "noopener,noreferrer");
    handleClose();
  };

  return (
    <div className="min-w-20 shrink">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => {
          setIsOpen(true);
        }}
        className={cn(
          UI_TABLE_NAME_BUTTON_CLASS,
          "block w-full max-w-60 truncate",
        )}
        title={`${tableName} (change table)`}
        aria-haspopup="dialog"
        aria-expanded={isOpen}
      >
        {tableName}
      </button>
      {isOpen && (
        <TableSpotlight onOpenTable={handleOpenTable} onClose={handleClose} />
      )}
    </div>
  );
};
export default TablePicker;
