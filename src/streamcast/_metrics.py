"""API-only instruments; the application configures the provider."""

from opentelemetry import metrics

PUBLISH_LAG = metrics.get_meter("streamcast").create_histogram(
    "streamcast.publish.lag",
    unit="s",
    explicit_bucket_boundaries_advisory=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
