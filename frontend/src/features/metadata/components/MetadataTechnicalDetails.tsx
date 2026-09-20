import type { TableMetadata } from "../../table/api/metadataSchemas";
import type { GraphNode } from "../../table/api/graphSchemas";
import CopyIconButton from "../../../components/CopyIconButton";
import MetadataProperties from "./MetadataProperties";
import MetadataHelp from "./MetadataHelp";
import {
  formatCount,
  formatSnapshotTime,
  integerText,
} from "../metadataPresentation";
import {
  METADATA_SECTION_BODY_CLASS,
  METADATA_SUMMARY_CLASS,
} from "../metadataStyles";

interface MetadataTechnicalDetailsProps {
  metadata: TableMetadata;
  snapshot: GraphNode | undefined;
}

const MetadataTechnicalDetails = ({
  metadata,
  snapshot,
}: MetadataTechnicalDetailsProps) => {
  // The committed time is a rendered string, so it is shown without a copy
  // action rather than offering a value that is not the stored timestamp.
  const facts = [
    { label: "Table UUID", value: metadata["table-uuid"], isCopyable: true },
    { label: "Table location", value: metadata.location, isCopyable: true },
    {
      label: "Iceberg format version",
      value: integerText(metadata["format-version"]),
      isCopyable: false,
    },
    {
      label: "Current snapshot in this metadata",
      value: integerText(metadata["current-snapshot-id"]),
      isCopyable: true,
    },
    {
      label: "Snapshot committed",
      value: formatSnapshotTime(snapshot?.timestamp),
      isCopyable: false,
    },
  ];
  const refs = Object.entries(metadata.refs ?? {});
  return (
    <section aria-labelledby="metadata-technical-title">
      <h2
        id="metadata-technical-title"
        className="mb-3 text-base font-semibold text-ink"
      >
        Technical details
      </h2>
      <div className="overflow-hidden rounded-xl border border-edge bg-surface">
        <details>
          <summary className={METADATA_SUMMARY_CLASS}>
            Storage and identifiers
          </summary>
          <dl className={`${METADATA_SECTION_BODY_CLASS} px-5 py-2`}>
            {facts.map(({ label, value, isCopyable }) => (
              <div
                key={label}
                className="grid gap-2 border-t border-edge py-3 first:border-t-0 md:grid-cols-[13rem_minmax(0,1fr)]"
              >
                <dt className="text-sm text-slate-400">{label}</dt>
                <dd className="flex min-w-0 items-start gap-2">
                  <code className="min-w-0 flex-1 break-all text-xs text-slate-300">
                    {value ?? "Unavailable"}
                  </code>
                  {isCopyable && value != null && (
                    <CopyIconButton text={value} title={`Copy ${label}`} />
                  )}
                </dd>
              </div>
            ))}
          </dl>
        </details>
        <details className="border-t border-edge">
          <summary className={METADATA_SUMMARY_CLASS}>
            Branches and tags{" "}
            <span className="ml-2 text-xs font-normal text-slate-400">
              {refs.length} references
            </span>
          </summary>
          <div className={`${METADATA_SECTION_BODY_CLASS} px-5 py-2`}>
            {refs.length === 0 && (
              <p className="text-sm text-slate-400">
                No named references recorded.
              </p>
            )}
            {refs.map(([name, ref]) => (
              <div
                key={name}
                className="flex flex-wrap items-center gap-3 border-t border-edge py-3 text-sm first:border-t-0"
              >
                <strong className="text-ink">{name}</strong>
                <span className="rounded bg-edge px-2 py-0.5 text-xs capitalize text-slate-400">
                  {ref.type}
                </span>
                <code className="min-w-0 break-all text-xs text-slate-300 md:ml-auto">
                  {String(ref["snapshot-id"])}
                </code>
                <CopyIconButton
                  text={String(ref["snapshot-id"])}
                  title={`Copy ${name} snapshot`}
                />
              </div>
            ))}
          </div>
        </details>
        <MetadataProperties properties={metadata.properties} />
        <details className="border-t border-edge">
          <summary className={METADATA_SUMMARY_CLASS}>
            Delete statistics
          </summary>
          <dl
            className={`${METADATA_SECTION_BODY_CLASS} space-y-3 px-5 py-4 text-sm text-slate-400`}
          >
            <div className="flex flex-wrap justify-between gap-3">
              <dt>
                <MetadataHelp label="Position deletes">
                  Delete records identifying rows by data file and row position.
                  These are records, not a count of unique deleted rows.
                </MetadataHelp>
              </dt>
              <dd>
                {formatCount(
                  integerText(snapshot?.summary?.["total-position-deletes"]),
                )}
              </dd>
            </div>
            <div className="flex flex-wrap justify-between gap-3">
              <dt>
                <MetadataHelp label="Equality deletes">
                  Delete records matching field values. One record may match
                  multiple rows; this is not a live row count.
                </MetadataHelp>
              </dt>
              <dd>
                {formatCount(
                  integerText(snapshot?.summary?.["total-equality-deletes"]),
                )}
              </dd>
            </div>
          </dl>
        </details>
      </div>
    </section>
  );
};
export default MetadataTechnicalDetails;
