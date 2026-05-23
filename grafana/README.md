# tickr — Grafana dashboard

Import `tickr-dashboard.json` into Grafana (Dashboards → New → Import →
Upload JSON file). The dashboard expects a Prometheus datasource and a
`job` label on the scraped target — pick yours from the dashboard's
template variable.

## Prometheus scrape

Expose the metrics from your app via the standard `promhttp` handler:

```go
import (
    "net/http"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"

    tprom "github.com/ndmt1at21/tickr/metrics/prom"
    "github.com/ndmt1at21/tickr"
)

reg := prometheus.NewRegistry()
metrics := tprom.New(reg)
// ... wire metrics into ClientConfig.Metrics and WorkerConfig.Metrics

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

Then add a scrape job in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: tickr-app
    static_configs:
      - targets: ['app:8080']
```

## Tempo / Jaeger deep-links

The handler span emits `messaging.message.id` as an attribute. In Grafana,
add a derived field on your Tempo/Jaeger datasource that links from a
log line containing `message_id=<uuid>` to the trace UI:

- Name: `tickr message_id`
- Regex: `message_id=(\S+)`
- URL: `/explore?...&query=${__value.raw}` (or your Tempo "find trace by tag" query)

## Panels

| Panel | What it tells you |
|---|---|
| Enqueue / Success / DLQ rate | Top-line throughput and failure shape |
| Inflight handlers | Saturation; should sit well below your pool size |
| Handler outcomes by event_type | Which event types are misbehaving |
| Handler duration p50/p95/p99 | Regression detection |
| Queue depth by status | CREATED/RETRYING growth = backlog; DEAD growth = poison pills |
| Lease reclaim rate | Spikes indicate workers crashing mid-handle |
| Claim batch fill ratio | Low fill = idle workers; full fill = saturated, scale up |
| Claim query duration | DB health; spikes correlate with autovacuum / IO issues |

## Suggested alerts (PromQL)

```promql
# DLQ rate sustained — something is permanently broken
rate(tickr_messages_dead_total[10m]) > 0.05

# Backlog growing — workers can't keep up
deriv(sum(tickr_queue_depth{status=~"CREATED|RETRYING"})[15m:]) > 1

# p99 handler latency regression
histogram_quantile(0.99, sum by (le) (rate(tickr_handler_duration_seconds_bucket[5m])))
  > 2 * histogram_quantile(0.99, sum by (le) (rate(tickr_handler_duration_seconds_bucket[1h] offset 1d)))
```
