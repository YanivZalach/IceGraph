import { useNavigate } from "@tanstack/react-router";

interface UnreachableMetadataFilePageProps {
  metadataFile: string;
}

const UnreachableMetadataFilePage = ({
  metadataFile,
}: UnreachableMetadataFilePageProps) => {
  const navigate = useNavigate();

  return (
    <div className="min-w-0 flex-1 overflow-y-auto bg-canvas">
      <main className="mx-auto flex max-w-5xl flex-col gap-7 px-5 py-8 md:px-8">
        <div className="rounded-xl border border-red-800 bg-red-950/50 p-5 text-red-400">
          <h2 className="font-bold">Metadata File Unreachable</h2>
          <p className="mt-2 text-sm text-slate-300">
            IceGraph can&apos;t open this table: the catalog points to a
            metadata file that can&apos;t be read. This file is the table&apos;s
            entry point, so until it is restored, or the catalog points to a
            readable metadata file, Spark and IceGraph can&apos;t load the
            table.
          </p>
          <p className="mt-4 text-sm font-bold">Metadata file</p>
          <p className="mt-1 font-mono text-sm break-all">{metadataFile}</p>
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
      </main>
    </div>
  );
};

export default UnreachableMetadataFilePage;
