import type { TableSchema } from "../../table/api/metadataSchemas";
import type { SpecSelection } from "../../specs/tableSpecs";
import ExpandableSchemaSection from "../../schema/components/ExpandableSchemaSection";
import { parseIcebergSchema } from "../../schema/schemaModel";
import { schemaColumnNames } from "../metadataPresentation";
import HelpTerm from "../../../shared/components/HelpTerm";

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
    <ExpandableSchemaSection
      ariaLabel="Schema for this metadata version"
      title="Schema"
      headerActions={
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
      }
      schema={schema}
    >
      <p className="mt-3 text-sm text-slate-400">{summary}</p>
      <p className="mt-2 text-sm text-slate-400">
        Column documentation appears below column names when available.{" "}
        <HelpTerm label="How to add column docs">
          Add a column comment with Spark SQL:
          <code className="mt-2 block whitespace-pre-wrap break-words font-mono">
            {
              "ALTER TABLE my.table\nALTER COLUMN event_type\nCOMMENT 'Signup, purchase, or cancellation';"
            }
          </code>
        </HelpTerm>
      </p>
      {identifiers.length > 0 && (
        <p className="mt-3 text-sm text-slate-300">
          <HelpTerm label="Identifier fields">
            Fields designated to identify rows. Iceberg does not enforce
            uniqueness; this does not itself enable upserts.
          </HelpTerm>
          :{" "}
          {identifiers
            .map(
              (id) =>
                `${names.get(String(id)) ?? "Unresolved field"} (${String(id)})`,
            )
            .join(", ")}
        </p>
      )}
    </ExpandableSchemaSection>
  );
};
export default MetadataSchema;
