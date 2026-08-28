### 1. Enter the table name

From the Home page, type the fully-qualified name of your Iceberg table (e.g. `database.table_name` ) and press Enter or click Continue. You can also click **Browse catalog** to list Iceberg tables from the Spark catalog. Use the filter field to narrow the list when many tables are available. When the non-Iceberg catalogs are included, the list notes that non-Iceberg tables may also appear.

### 2. Pick a snapshot range

IceGraph shows you the table's snapshot history. Select the range of snapshots you want to explore. A smaller range loads faster and produces a less cluttered graph. If you just want the table's current state, click **Latest Metadata Only** to the left of the snapshot pickers to skip range selection and go straight to the Metadata view for the latest snapshot.

### 3. Wait for the graph

IceGraph fetches the metadata in the background. Once ready, you land on the Timeline view. Large ranges with many data files may take a moment. The loading panel shows which preparation steps are active and how many have completed, including the final graph build.

### Switching tables

While viewing a table, click the table name in the navbar to change tables. Enter a new table or use **Browse catalog**, then click Continue. IceGraph opens the new table in a separate browser tab so your current graph stays loaded.
