const MetadataViewSection = () => (
  <div className="space-y-4">
    <p>
      Shows the structured metadata of your table - schema, partition spec, and
      sort order. Use this to verify column types, understand partition
      strategies, and inspect how the schema has evolved.
    </p>
    <ul className="list-disc list-inside space-y-1">
      <li>
        Column IDs are stable even when columns are renamed - useful for tracing
        schema evolution
      </li>
      <li>
        <strong className="text-white">Overview</strong> fields include a
        clipboard icon to copy individual values
      </li>
    </ul>
  </div>
);

export default MetadataViewSection;
