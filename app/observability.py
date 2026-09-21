from prometheus_client import Counter, Gauge, Histogram

INGESTION_RUNS = Counter(
    "earthquake_ingestion_runs_total", "Number of ingestion executions", ["status"]
)
EVENTS_RECEIVED = Counter("earthquake_events_received_total", "Events returned by USGS")
EVENTS_INSERTED = Counter("earthquake_events_inserted_total", "New events inserted")
EVENTS_DUPLICATED = Counter("earthquake_events_duplicated_total", "Duplicate events ignored")
EVENTS_FAILED = Counter("earthquake_events_failed_total", "Events that failed processing")
INGESTION_DURATION = Histogram(
    "earthquake_ingestion_duration_seconds", "Duration of an ingestion execution"
)
PENDING_EVENTS = Gauge(
    "earthquake_events_pending_processing", "Events pending metrics processing"
)
API_REQUESTS = Counter(
    "earthquake_api_requests_total", "HTTP requests", ["method", "path", "status"]
)
API_DURATION = Histogram(
    "earthquake_api_request_duration_seconds", "HTTP request duration", ["method", "path"]
)

