import os

from dotenv import load_dotenv

load_dotenv()


class Env:
    """Application settings loaded from environment variables."""

    # Maximum number of graphs to compute in parallel.
    MAX_NUMBER_OF_GRAPHS_TO_COMPUTE = int(os.getenv("MAX_NUMBER_OF_GRAPHS_TO_COMPUTE", "15"))

    # Maximum number of snapshots shown on the snapshot selection page.
    MAX_SNAPSHOTS_TO_SHOW = int(os.getenv("MAX_SNAPSHOTS_TO_SHOW", "20"))

    # Maximum number of snapshots processed per graph.
    MAX_SNAPSHOTS_TO_COMPUTE = int(os.getenv("MAX_SNAPSHOTS_TO_COMPUTE", "50"))

    # Delay before a completed graph is removed from memory.
    COMPUTE_CLEANUP_TIME_SECONDS = int(os.getenv("COMPUTE_CLEANUP_TIME_SECONDS", "12"))

    # Maximum number of data files collected per graph.
    MAX_DATA_FILES_TO_COLLECT = int(os.getenv("MAX_DATA_FILES_TO_COLLECT", "5000"))

    # Cache lifetime for the table selection endpoint.
    TABLE_LIST_CACHE_TTL_SECONDS = int(os.getenv("TABLE_LIST_CACHE_TTL_SECONDS", "60"))

    # Whether non-Iceberg catalogs are included in table selection.
    INCLUDE_NONE_ICEBERG_CATALOGS = os.getenv("INCLUDE_NONE_ICEBERG_CATALOGS", "true").lower() == "true"

    # Maximum time allowed for graceful application shutdown.
    MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS = int(os.getenv("MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS", "10"))

    # Whether to serve the application with Waitress.
    PRODUCTION_MODE = os.getenv("PRODUCTION_MODE", "false").lower() == "true"

    # Number of request threads used by Waitress.
    WSGI_THREADS = int(os.getenv("WSGI_THREADS", "20"))
