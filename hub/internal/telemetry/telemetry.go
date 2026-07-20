// Package telemetry wires the hub's observability (M6a, ADR-0020): an
// OpenTelemetry meter backed by a Prometheus exporter, surfaced as a
// /metrics HTTP handler. Metadata only — no payload, no secret values ever
// touch a metric (two-plane split, ADR-0016). Metrics live only in hub/ and
// runner/; the engine and pkg/ stay telemetry-free.
package telemetry

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Snapshot is the platform-wide control-plane state a scrape observes. It
// mirrors store.Stats but is defined here so telemetry does not couple to
// the store package; the caller adapts.
type Snapshot struct {
	Tasks           map[string]int64 // count by state
	OldestQueuedSec float64
	RunnersActive   int64
	RunnersTotal    int64
	SchedulesDue    int64
	Schedules       int64
	Flows           int64
}

// NewHub builds the Prometheus /metrics handler. statsFn is invoked once per
// scrape (async observable callback) to source live values; it must be
// cheap and safe to call from a background context (no tenant scope).
// rejectedFn returns cumulative rate-limit rejections by class (M6c); may
// be nil (no limiter).
func NewHub(statsFn func(context.Context) (Snapshot, error), rejectedFn func() map[string]int64) (http.Handler, error) {
	reg := prometheus.NewRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp)).Meter("shift.hub")

	tasks, err := m.Int64ObservableGauge("shift_hub_tasks",
		metric.WithDescription("Tasks in the queue by state."))
	if err != nil {
		return nil, err
	}
	oldest, err := m.Float64ObservableGauge("shift_hub_oldest_queued_seconds",
		metric.WithDescription("Age of the oldest queued task."))
	if err != nil {
		return nil, err
	}
	runnersActive, err := m.Int64ObservableGauge("shift_hub_runners_active",
		metric.WithDescription("Runners seen in the last 2 minutes."))
	if err != nil {
		return nil, err
	}
	runnersTotal, err := m.Int64ObservableGauge("shift_hub_runners_total",
		metric.WithDescription("Registered runners."))
	if err != nil {
		return nil, err
	}
	schedulesDue, err := m.Int64ObservableGauge("shift_hub_schedules_due",
		metric.WithDescription("Enabled schedules due to fire."))
	if err != nil {
		return nil, err
	}
	schedules, err := m.Int64ObservableGauge("shift_hub_schedules",
		metric.WithDescription("Total schedules."))
	if err != nil {
		return nil, err
	}
	flows, err := m.Int64ObservableGauge("shift_hub_flows",
		metric.WithDescription("Deployed flows."))
	if err != nil {
		return nil, err
	}
	ratelimited, err := m.Int64ObservableCounter("shift_hub_ratelimited_total",
		metric.WithDescription("Requests rejected by the rate limiter, by class."))
	if err != nil {
		return nil, err
	}

	// One callback, one stats query per scrape — observes every instrument.
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		s, err := statsFn(ctx)
		if err != nil {
			return err
		}
		for state, n := range s.Tasks {
			o.ObserveInt64(tasks, n, metric.WithAttributes(attribute.String("state", state)))
		}
		o.ObserveFloat64(oldest, s.OldestQueuedSec)
		o.ObserveInt64(runnersActive, s.RunnersActive)
		o.ObserveInt64(runnersTotal, s.RunnersTotal)
		o.ObserveInt64(schedulesDue, s.SchedulesDue)
		o.ObserveInt64(schedules, s.Schedules)
		o.ObserveInt64(flows, s.Flows)
		if rejectedFn != nil {
			for class, n := range rejectedFn() {
				o.ObserveInt64(ratelimited, n, metric.WithAttributes(attribute.String("class", class)))
			}
		}
		return nil
	}, tasks, oldest, runnersActive, runnersTotal, schedulesDue, schedules, flows, ratelimited)
	if err != nil {
		return nil, fmt.Errorf("telemetry: register callback: %w", err)
	}

	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}
