const LoadingTableSection = () => (
  <div className="space-y-5">
    <div className="space-y-2">
      <h3 className="text-white font-semibold">1. Enter the table name</h3>
      <p>
        From the Home page, type the fully-qualified name of your Iceberg table
        (e.g.{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          database.table_name
        </code>
        ) and press Enter or click Continue. You can also click{" "}
        <strong className="text-white">Browse catalog</strong> to list Iceberg
        tables from the Spark catalog. Use the filter field to narrow the list
        when many tables are available. When the non-Iceberg catalogs are
        included, the list notes that non-Iceberg tables may also appear.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">2. Pick a snapshot range</h3>
      <p>
        IceGraph shows you the table's snapshot history. Select the range of
        snapshots you want to explore. A smaller range loads faster and produces
        a less cluttered graph. If you just want the table's current state,
        click <strong className="text-white">Latest Metadata Only</strong> to
        the left of the snapshot pickers to skip range selection and go straight
        to the Metadata view for the latest snapshot.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">3. Wait for the graph</h3>
      <p>
        IceGraph fetches the metadata in the background. Once ready, you land on
        the Timeline view. Large ranges with many data files may take a moment.
        The loading panel shows which preparation steps are active and how many
        have completed, including the final graph build.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Switching tables</h3>
      <p>
        While viewing a table, click the table name in the navbar to change
        tables. Enter a new table or use{" "}
        <strong className="text-white">Browse catalog</strong>, then click
        Continue. IceGraph opens the new table in a separate browser tab so your
        current graph stays loaded.
      </p>
    </div>
  </div>
);

export default LoadingTableSection;
