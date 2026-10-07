import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import MetadataWarnings from "../../metadata/components/MetadataWarnings";
import ExpandableSchemaSection from "../../schema/components/ExpandableSchemaSection";
import { parseIcebergSchema } from "../../schema/schemaModel";
import { tableDescriptionQueryOptions } from "../api/tableDescriptionQueries";
import TableDescriptionRows from "./TableDescriptionRows";
import TableDescriptionFacts from "./TableDescriptionFacts";

interface TableDescriptionProps {
  tableName: string;
}

interface DescriptionSectionProps {
  title: string;
  children: ReactNode;
}

const DescriptionSection = ({ title, children }: DescriptionSectionProps) => (
  <section aria-label={title}>
    <h2 className="mb-3 text-base font-semibold text-ink">{title}</h2>
    <div className="overflow-hidden rounded-xl border border-edge bg-surface">
      {children}
    </div>
  </section>
);

const TableDescription = ({ tableName }: TableDescriptionProps) => {
  const descriptionQuery = useQuery(tableDescriptionQueryOptions(tableName));

  if (descriptionQuery.isPending)
    return (
      <p className="text-sm text-slate-400">Loading table description...</p>
    );

  if (descriptionQuery.isError)
    return (
      <p className="text-sm break-words text-red-400">
        Failed to load table description: {descriptionQuery.error.message}
      </p>
    );

  const { spark_schema, spark_partitions, sections, properties, warnings } =
    descriptionQuery.data;

  return (
    <>
      <MetadataWarnings warnings={warnings} />
      {spark_schema != null && (
        <ExpandableSchemaSection
          ariaLabel="Spark schema"
          title="Spark Schema"
          schema={spark_schema}
          showIds={false}
        >
          <p className="mt-3 text-sm text-slate-400">
            {`${String(parseIcebergSchema(spark_schema).fields.length)} top-level columns.`}
          </p>
        </ExpandableSchemaSection>
      )}
      {spark_partitions.length > 0 && (
        <DescriptionSection title="Spark Partitions">
          <TableDescriptionRows rows={spark_partitions} />
        </DescriptionSection>
      )}
      {properties !== null && (
        <DescriptionSection title="Table Properties">
          <TableDescriptionFacts
            rows={Object.entries(properties).map(([name, value]) => ({
              name,
              value,
              comment: "",
            }))}
          />
        </DescriptionSection>
      )}
      {sections.map((section, sectionIndex) => (
        <DescriptionSection
          key={`${section.title}.${String(sectionIndex)}`}
          title={section.title}
        >
          <TableDescriptionFacts rows={section.rows} />
        </DescriptionSection>
      ))}
    </>
  );
};

export default TableDescription;
