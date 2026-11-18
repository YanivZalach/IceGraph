import os

from dotenv import load_dotenv

load_dotenv()


class Env:
    """Application settings loaded from environment variables."""

    # Maximum number of graphs to compute in parallel.
    MAX_NUMBER_OF_GRAPHS_TO_COMPUTE: int = int(os.getenv("MAX_NUMBER_OF_GRAPHS_TO_COMPUTE", "15"))

    # Maximum number of snapshots loaded per page on the snapshot selection page.
    MAX_SNAPSHOTS_TO_SHOW: int = int(os.getenv("MAX_SNAPSHOTS_TO_SHOW", "20"))

    # Maximum number of snapshots processed per graph.
    MAX_SNAPSHOTS_TO_COMPUTE: int = int(os.getenv("MAX_SNAPSHOTS_TO_COMPUTE", "50"))

    # Delay before a completed graph is removed from memory.
    COMPUTE_CLEANUP_TIME_SECONDS: int = int(os.getenv("COMPUTE_CLEANUP_TIME_SECONDS", "15"))

    # Maximum number of data files collected per graph.
    MAX_DATA_FILES_TO_COLLECT: int = int(os.getenv("MAX_DATA_FILES_TO_COLLECT", "5000"))

    # Maximum number of manifests collected per graph.
    MAX_MANIFESTS_TO_COLLECT: int = int(os.getenv("MAX_MANIFESTS_TO_COLLECT", "300"))

    # Maximum number of metadata files collected per graph.
    MAX_METADATA_FILES_TO_COLLECT: int = int(os.getenv("MAX_METADATA_FILES_TO_COLLECT", "100"))

    # Maximum number of partitions sampled from each partition statistics file.
    MAX_PARTITION_STATISTICS_ROWS: int = int(os.getenv("MAX_PARTITION_STATISTICS_ROWS", "10"))

    # Maximum number of partition statistics files read per graph, newest first.
    MAX_PARTITION_STATISTICS_FILES_TO_READ: int = int(os.getenv("MAX_PARTITION_STATISTICS_FILES_TO_READ", "50"))

    # Cache lifetime for the table selection endpoint.
    TABLE_LIST_CACHE_TTL_SECONDS: int = int(os.getenv("TABLE_LIST_CACHE_TTL_SECONDS", "60"))

    # Age after which the browser automatically refreshes its saved table list.
    TABLE_LIST_BROWSER_REFRESH_SECONDS: int = int(os.getenv("TABLE_LIST_BROWSER_REFRESH_SECONDS", "82800"))

    # Base URL used to link Spark snapshots to their History Server applications.
    SPARK_HISTORY_SERVER_URL: str = os.getenv("SPARK_HISTORY_SERVER_URL", "")

    # Whether non-Iceberg catalogs are included in table selection.
    INCLUDE_NONE_ICEBERG_CATALOGS: bool = os.getenv("INCLUDE_NONE_ICEBERG_CATALOGS", "true").lower() == "true"

    # Maximum time allowed for graceful application shutdown.
    MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS: int = int(os.getenv("MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS", "10"))

    # Port the application listens on.
    APPLICATION_PORT: int = int(os.getenv("APPLICATION_PORT", "5050"))

    # Whether to serve the application with Waitress.
    PRODUCTION_MODE: bool = os.getenv("PRODUCTION_MODE", "false").lower() == "true"

    # Whether the Prometheus metrics endpoint is served.
    METRICS_ENABLED: bool = os.getenv("METRICS_ENABLED", "true").lower() == "true"

    # Number of request threads used by Waitress.
    WSGI_THREADS: int = int(os.getenv("WSGI_THREADS", "20"))

    # Query engine used to read Iceberg metadata: spark or trino.
    QUERY_ENGINE: str = os.getenv("QUERY_ENGINE", "spark").lower()

    # Trino coordinator host, used when QUERY_ENGINE is trino.
    TRINO_HOST: str = os.getenv("TRINO_HOST", "localhost")

    # Trino coordinator port.
    TRINO_PORT: int = int(os.getenv("TRINO_PORT", "8080"))

    # Trino user that runs IceGraph queries.
    TRINO_USER: str = os.getenv("TRINO_USER", "icegraph")

    # Trino password for basic authentication over https, left empty to disable authentication.
    TRINO_PASSWORD: str = os.getenv("TRINO_PASSWORD", "")

    # Protocol used to reach Trino: http or https.
    TRINO_HTTP_SCHEME: str = os.getenv("TRINO_HTTP_SCHEME", "http")

    # Existing Hive catalog.schema, backed by a file metastore, where IceGraph registers external tables to read raw Iceberg files.
    TRINO_RAW_FILES_SCHEMA: str = os.getenv("TRINO_RAW_FILES_SCHEMA", "hive.icegraph")
