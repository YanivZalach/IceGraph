import CopyIconButton from "../../../components/CopyIconButton";
import { ApiError } from "../../../shared/lib/api";
import { partitionFilesErrorBodySchema } from "../api/partitionFilesSchemas";

interface PartitionFilesErrorProps {
  error: Error;
  actionLabel: string;
  onAction: () => void;
}

const sparkSqlOf = (error: Error): string | null => {
  if (!(error instanceof ApiError)) return null;
  const body = partitionFilesErrorBodySchema.safeParse(error.body);
  return body.success ? (body.data.spark_sql ?? null) : null;
};

const PartitionFilesError = ({
  error,
  actionLabel,
  onAction,
}: PartitionFilesErrorProps) => {
  const sparkSql = sparkSqlOf(error);

  return (
    <div className="flex w-full flex-col gap-4 rounded-xl border border-red-800 bg-red-950/50 px-8 py-6 text-red-300">
      <h2 className="font-bold text-red-400">
        {sparkSql === null
          ? "Failed to load the partition files"
          : "The partition is too large to load"}
      </h2>
      <p className="whitespace-pre-wrap text-sm">{error.message}</p>
      {sparkSql !== null && (
        <div className="rounded-lg border border-edge bg-canvas">
          <div className="flex items-center justify-between border-b border-edge px-3 py-1.5 text-xs font-bold uppercase tracking-wide text-slate-400">
            Spark SQL
            <CopyIconButton text={sparkSql} title="Copy Spark SQL" />
          </div>
          <pre className="overflow-x-auto px-3 py-2 font-mono text-xs text-ink">
            {sparkSql}
          </pre>
        </div>
      )}
      <button
        type="button"
        onClick={onAction}
        className="w-fit cursor-pointer rounded-lg border-2 border-accent bg-accent px-5 py-2 text-sm font-bold text-white transition hover:bg-accent-dark"
      >
        {actionLabel}
      </button>
    </div>
  );
};

export default PartitionFilesError;
