import { useNavigate } from "@tanstack/react-router";
import TableDescription from "./TableDescription";

interface NotIcebergTablePageProps {
  tableName: string;
  errorMessages: string[];
}

const NotIcebergTablePage = ({
  tableName,
  errorMessages,
}: NotIcebergTablePageProps) => {
  const navigate = useNavigate();

  return (
    <div className="min-w-0 flex-1 overflow-y-auto bg-canvas">
      <main className="mx-auto flex max-w-5xl flex-col gap-7 px-5 py-8 md:px-8">
        <div className="rounded-xl border border-red-800 bg-red-950/50 p-5 text-red-400">
          <h2 className="font-bold">Failed to Load Table</h2>
          {[...new Set(errorMessages)].map((message) => (
            <p key={message} className="mt-2 text-sm break-words">
              {message}
            </p>
          ))}
          <p className="mt-4 text-sm text-slate-300">
            IceGraph can&apos;t analyze this table, but here is how Spark
            describes it.
          </p>
          <button
            type="button"
            onClick={() => {
              void navigate({ to: "/" });
            }}
            className="mt-4 cursor-pointer text-sm text-slate-400 hover:text-white"
          >
            Go Back
          </button>
        </div>
        <TableDescription tableName={tableName} />
      </main>
    </div>
  );
};

export default NotIcebergTablePage;
