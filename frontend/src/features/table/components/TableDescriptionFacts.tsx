import type { TableDescriptionRow } from "../api/tableDescriptionQueries";

interface TableDescriptionFactsProps {
  rows: TableDescriptionRow[];
}

const TableDescriptionFacts = ({ rows }: TableDescriptionFactsProps) => {
  if (rows.length === 0) {
    return <p className="px-5 py-4 text-sm italic text-slate-400">Empty.</p>;
  }

  return (
    <dl className="px-5 py-2">
      {rows.map((row, rowIndex) => (
        <div
          key={`${row.name}.${String(rowIndex)}`}
          className="grid gap-2 border-t border-edge py-3 first:border-t-0 md:grid-cols-[13rem_minmax(0,1fr)]"
        >
          <dt className="text-sm text-slate-400">{row.name}</dt>
          <dd className="min-w-0">
            <code className="break-all text-xs text-slate-300">
              {row.value}
            </code>
            {row.comment !== "" && (
              <p className="mt-1 text-xs text-slate-500">{row.comment}</p>
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
};

export default TableDescriptionFacts;
