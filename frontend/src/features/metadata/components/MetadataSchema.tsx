import { useId, useState } from "react";
import type { TableSchema } from "../../table/api/metadataSchemas";
import type { SpecSelection } from "../../table/tableSpecs";
import SchemaFieldList from "../../schema/components/SchemaFieldList";
import { parseIcebergSchema } from "../../schema/schemaModel";
import { cn } from "../../../shared/lib/cn";
import { schemaColumnNames } from "../metadataPresentation";
import MetadataHelp from "./MetadataHelp";

interface MetadataSchemaProps {
  schema: TableSchema | undefined;
  schemas: TableSchema[];
  onOpenSpec: (selection: SpecSelection) => void;
}

const MetadataSchema = ({
  schema,
  schemas,
  onOpenSpec,
}: MetadataSchemaProps) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const panelId = useId();
  if (!schema)
    return (
      <section className="rounded-xl border border-edge bg-surface p-5">
        <h2 className="text-base font-semibold text-ink">Schema</h2>
        <p className="mt-3 text-sm text-slate-400">
          Current schema is unavailable in this metadata.
        </p>
      </section>
    );
  const parsed = parseIcebergSchema(schema);
  const names = schemaColumnNames(parsed);
  const earlierCount = schemas.filter(
    (item) => BigInt(item["schema-id"]) < BigInt(schema["schema-id"]),
  ).length;
  const identifiers = schema["identifier-field-ids"] ?? [];
  const summary = [
    `${String(parsed.fields.length)} top-level columns.`,
    earlierCount > 0 &&
      `${String(earlierCount)} earlier ${earlierCount === 1 ? "schema" : "schemas"} in this metadata.`,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <section
      aria-label="Schema for this metadata version"
      className="overflow-hidden rounded-xl border border-edge bg-surface"
    >
      <div className="p-5">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="text-base font-semibold text-ink">Schema</h2>
          <button
            type="button"
            onClick={() => {
              onOpenSpec({ kind: "schema", id: schema["schema-id"] });
            }}
            className="cursor-pointer text-xs text-accent-text hover:underline"
            aria-haspopup="dialog"
          >
            Schema {String(schema["schema-id"])} ↗
          </button>
          <button
            type="button"
            onClick={() => {
              setIsExpanded(!isExpanded);
            }}
            aria-expanded={isExpanded}
            aria-controls={panelId}
            className="ml-auto cursor-pointer rounded-lg border border-edge px-3 py-2 text-xs text-ink hover:border-accent"
          >
            {isExpanded ? "Collapse schema" : "Expand schema"}
          </button>
        </div>
        <p className="mt-3 text-sm text-slate-400">{summary}</p>
        {identifiers.length > 0 && (
          <p className="mt-3 text-sm text-slate-300">
            <MetadataHelp label="Identifier fields">
              Fields designated to identify rows. Iceberg does not enforce
              uniqueness; this does not itself enable upserts.
            </MetadataHelp>
            :{" "}
            {identifiers
              .map(
                (id) =>
                  `${names.get(String(id)) ?? "Unresolved field"} (${String(id)})`,
              )
              .join(", ")}
          </p>
        )}
      </div>
      <div
        id={panelId}
        data-metadata-scroll
        tabIndex={0}
        role="region"
        aria-label="Schema fields"
        className={cn(
          "overflow-auto border-t border-edge px-5 py-3 [scrollbar-gutter:stable] focus-visible:outline-2 focus-visible:outline-accent",
          !isExpanded && "max-h-96",
        )}
      >
        <SchemaFieldList schema={schema} />
      </div>
    </section>
  );
};
export default MetadataSchema;
