interface MetadataWarningsProps {
  warnings: Record<string, string[]>;
}

const MetadataWarnings = ({ warnings }: MetadataWarningsProps) => {
  const warningEntries = Object.entries(warnings).filter(
    ([, messages]) => messages.length > 0,
  );
  if (warningEntries.length === 0) return null;

  return (
    <div className="rounded-xl border border-amber-500/40 bg-amber-900/20 px-5 py-4">
      <h2 className="font-bold text-amber-400">Warning</h2>
      {warningEntries.map(([source, messages]) =>
        messages.map((message, index) => (
          <p
            key={`${source}-${String(index)}`}
            className="mt-2 text-sm text-amber-300"
          >
            {message}
          </p>
        )),
      )}
    </div>
  );
};

export default MetadataWarnings;
