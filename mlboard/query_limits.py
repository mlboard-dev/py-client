"""Local query ceilings shared by validation, execution and viewer capabilities."""

QUERY_LIMITS = {
    "max_runs": 10,
    "max_series": 20,
    "max_points_per_series": 2000,
    "max_points": 20000,
    "max_scanned_points": 250000,
    "max_bytes": 2 * 1024**2,
    "timeout_ms": 2000,
    "paged_snapshots": 8,
}
MAX_ACTIVE_QUERIES = 2
SNAPSHOT_TTL_SECONDS = 120
MIN_DOWNSAMPLE_POINTS = 4
