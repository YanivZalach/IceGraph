import type {
  IcebergInteger,
  TableMetadata,
} from "../features/table/api/metadataSchemas";
import type { SpecDetail, SpecSelection } from "../features/table/tableSpecs";
import { UI_STRUCTURED_SECTION_TITLE_CLASS } from "../uiTypography";

interface MetadataStructuredProps {
  metadata: TableMetadata;
  onSelect: (kind: SpecSelection["kind"], id: IcebergInteger) => void;
  selection: SpecDetail | null;
}

interface HistoryItem {
  id: IcebergInteger;
  fieldCount: number;
}

interface HistoryBoxesProps {
  activeId: IcebergInteger | null | undefined;
  items: HistoryItem[] | undefined;
  label: string;
  onSelect: MetadataStructuredProps["onSelect"];
  selection: SpecDetail | null;
  type: SpecSelection["kind"];
}

const HistoryBoxes = ({
  activeId,
  items,
  label,
  onSelect,
  selection,
  type,
}: HistoryBoxesProps) => {
  if (items === undefined) return null;
  return (
    <div className="mb-6">
      <div className={UI_STRUCTURED_SECTION_TITLE_CLASS}>
        <span>{label} History</span>
        <span className="rounded-full bg-edge px-2 py-0.5 text-xs font-medium text-slate-400 normal-case">
          {items.length} {items.length === 1 ? "item" : "items"}
        </span>
      </div>
      <div className="flex flex-wrap gap-2.5">
        {items.map(({ id, fieldCount }) => {
          const isActive = String(id) === String(activeId);
          const isSelected =
            selection?.type === type && String(selection.id) === String(id);
          return (
            <button
              type="button"
              key={String(id)}
              aria-pressed={isSelected}
              className={`group relative min-w-[70px] cursor-pointer rounded-xl border-2 p-3 text-center transition-all duration-200 ${
                isSelected
                  ? "border-amber-400 bg-amber-900/20 shadow-sm"
                  : isActive
                    ? "border-accent bg-accent-muted shadow-sm"
                    : "border-edge bg-surface-deep hover:border-edge-hover hover:shadow-sm"
              }`}
              onClick={() => {
                onSelect(type, id);
              }}
              title={`Fields: ${String(fieldCount)}`}
            >
              {isActive && (
                <span className="absolute -top-2.5 left-1/2 z-10 -translate-x-1/2 rounded-full bg-accent px-2 py-0.5 text-xs font-black tracking-wider text-white shadow-sm">
                  ACTIVE
                </span>
              )}
              <span
                className={`block text-xl leading-none font-black ${
                  isActive || isSelected ? "text-ink" : "text-slate-300"
                }`}
              >
                {id}
              </span>
              {isSelected && (
                <div className="absolute -bottom-1 left-1/2 size-1.5 -translate-x-1/2 rounded-full bg-amber-400" />
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
};

const MetadataStructured = ({
  metadata,
  onSelect,
  selection,
}: MetadataStructuredProps) => (
  <div className="py-2">
    <HistoryBoxes
      items={metadata.schemas?.map((item) => ({
        id: item["schema-id"],
        fieldCount: item.fields?.length ?? 0,
      }))}
      label="Schema"
      activeId={metadata["current-schema-id"]}
      type="schema"
      onSelect={onSelect}
      selection={selection}
    />
    <HistoryBoxes
      items={metadata["partition-specs"]?.map((item) => ({
        id: item["spec-id"],
        fieldCount: item.fields?.length ?? 0,
      }))}
      label="Partition"
      activeId={metadata["default-spec-id"]}
      type="partition"
      onSelect={onSelect}
      selection={selection}
    />
    <HistoryBoxes
      items={metadata["sort-orders"]?.map((item) => ({
        id: item["order-id"],
        fieldCount: item.fields?.length ?? 0,
      }))}
      label="Order"
      activeId={metadata["default-sort-order-id"]}
      type="order"
      onSelect={onSelect}
      selection={selection}
    />
  </div>
);

export default MetadataStructured;
