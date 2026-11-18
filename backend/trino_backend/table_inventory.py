from table_inventory.table_inventory import TableInventory
from trino_backend.data_files import TrinoCollectDataFiles
from trino_backend.manifests import TrinoCollectManifests
from trino_backend.metadata import TrinoCollectMetadata, TrinoTableMetadataCollector
from trino_backend.partition_statistics import TrinoCollectPartitionStatistics
from trino_backend.snapshots import TrinoCollectSnapshots, find_search_cutoff
from trino_backend.statistics import TrinoCollectTableStatistics


class TrinoTableInventory(TableInventory):
    SNAPSHOTS_COLLECTOR = TrinoCollectSnapshots
    METADATA_COLLECTOR = TrinoCollectMetadata
    TABLE_STATISTICS_COLLECTOR = TrinoCollectTableStatistics
    PARTITION_STATISTICS_COLLECTOR = TrinoCollectPartitionStatistics
    MANIFESTS_COLLECTOR = TrinoCollectManifests
    DATA_FILES_COLLECTOR = TrinoCollectDataFiles
    TABLE_METADATA_COLLECTOR = TrinoTableMetadataCollector

    def _find_search_cutoff(self):
        self._search_cutoff = find_search_cutoff(self._table_name, self._start_snapshot_id, self._end_snapshot_id)
        self._warnings.update(self._search_cutoff.warnings)
