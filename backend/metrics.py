from prometheus_client import Counter, Histogram

OPERATION_DURATION_BUCKETS = (0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)

operation_duration_seconds = Histogram(
    "icegraph_operation_duration_seconds",
    "Duration of timed backend operations.",
    ["operation"],
    buckets=OPERATION_DURATION_BUCKETS,
)

graph_computations_total = Counter(
    "icegraph_graph_computations_total",
    "Graph computations submitted to Spark.",
)

cache_reads_total = Counter(
    "icegraph_cache_reads_total",
    "Requests served from a server-side cache.",
    ["cache"],
)

GRAPH_JOB_CACHE = "graph_job"
TABLE_LIST_CACHE = "table_list"

for cache_name in (GRAPH_JOB_CACHE, TABLE_LIST_CACHE):
    cache_reads_total.labels(cache=cache_name)
