interface FileTreeDetailRowProps {
  label: string;
  value: unknown;
}

const formatDetailValue = (value: unknown): string => {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return String(value);
  }
  return JSON.stringify(value, null, 2);
};

const FileTreeDetailRow = ({ label, value }: FileTreeDetailRowProps) => (
  <div className="flex flex-col gap-1">
    <span className="text-xs font-bold uppercase tracking-wide text-slate-500">
      {label}
    </span>
    <span className="overflow-x-auto whitespace-pre-wrap break-words rounded-lg bg-canvas px-3 py-2 font-mono text-xs text-slate-200">
      {formatDetailValue(value)}
    </span>
  </div>
);

export default FileTreeDetailRow;
