const SpecsPanelSection = () => (
  <div className="space-y-4">
    <p>
      The <strong className="text-white">Specs</strong> button in the navbar
      opens the Table Specification panel, which shows the full history of your
      table's structural definitions across three sections:
    </p>
    <ul className="list-disc list-inside space-y-2">
      <li>
        <strong className="text-white">Schema History</strong> - every schema
        version the table has had, showing each by its schema ID
      </li>
      <li>
        <strong className="text-white">Partition History</strong> - every
        partition spec version, showing each by its spec ID
      </li>
      <li>
        <strong className="text-white">Order History</strong> - every sort order
        version, showing each by its order ID
      </li>
    </ul>
    <p>
      The currently active version in each section is highlighted with an{" "}
      <strong className="text-white">ACTIVE</strong> badge. Click any version to
      expand its full field-level definition - schema fields render with their
      types (including nested structs, lists, and maps), while partition and
      sort order fields render as structured tables. Use the{" "}
      <strong className="text-white">Full</strong> /{" "}
      <strong className="text-white">Diff</strong> toggle to compare a version
      against the one before it, with added, removed, and changed fields
      highlighted.
    </p>
  </div>
);

export default SpecsPanelSection;
