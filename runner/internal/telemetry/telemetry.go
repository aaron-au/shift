// Package telemetry wires the runner's observability (M6a, ADR-0020): an
// OpenTelemetry meter backed by a Prometheus exporter, surfaced as a
// /metrics HTTP handler. All values come from the in-memory service
// snapshot (governor, task totals, connector pool) — honest signals, no
// payload, no secret values (two-plane split, ADR-0016).
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

// ConnUse is one connector's in-use count in a Snapshot.
type ConnUse struct {
	Name  string
	InUse int64
}

// Snapshot is the runner state a scrape observes (from service.Status()).
type Snapshot struct {
	GovBudget int64 // memory-admission budget (bytes)
	GovUsed   int64
	GovPeak   int64
	MaxByMem  int64 // admission headroom: concurrent tasks the budget allows
	Submitted int64
	Completed int64
	Failed    int64
	Waiting   int64
	Running   int64
	RecordsIn int64
	Conns     []ConnUse
}

// NewRunner builds the Prometheus /metrics handler. snapFn is invoked once
// per scrape; it reads in-memory state only (no I/O), so it is cheap.
func NewRunner(snapFn func() Snapshot) (http.Handler, error) {
	reg := prometheus.NewRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp)).Meter("shift.runner")

	g := func(name, desc string) metric.Int64ObservableGauge {
		inst, e := m.Int64ObservableGauge(name, metric.WithDescription(desc))
		if e != nil {
			err = e
		}
		return inst
	}
	c := func(name, desc string) metric.Int64ObservableCounter {
		inst, e := m.Int64ObservableCounter(name, metric.WithDescription(desc))
		if e != nil {
			err = e
		}
		return inst
	}
	govBudget := g("shift_runner_governor_budget_bytes", "Memory-admission budget.")
	govUsed := g("shift_runner_governor_used_bytes", "Memory reserved right now.")
	govPeak := g("shift_runner_governor_peak_bytes", "Peak memory reserved.")
	maxByMem := g("shift_runner_max_concurrent_by_mem", "Concurrent tasks the budget allows.")
	running := g("shift_runner_tasks_running", "Tasks executing now.")
	waiting := g("shift_runner_tasks_waiting", "Tasks admitted but waiting on capacity.")
	submitted := c("shift_runner_tasks_submitted_total", "Tasks accepted since start.")
	completed := c("shift_runner_tasks_completed_total", "Tasks completed since start.")
	failed := c("shift_runner_tasks_failed_total", "Tasks failed since start.")
	recordsIn := c("shift_runner_records_in_total", "Records read from sources since start.")
	connInUse := g("shift_runner_connector_in_use", "Connector processes in use, by connector.")
	if err != nil {
		return nil, fmt.Errorf("telemetry: instruments: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := snapFn()
		o.ObserveInt64(govBudget, s.GovBudget)
		o.ObserveInt64(govUsed, s.GovUsed)
		o.ObserveInt64(govPeak, s.GovPeak)
		o.ObserveInt64(maxByMem, s.MaxByMem)
		o.ObserveInt64(running, s.Running)
		o.ObserveInt64(waiting, s.Waiting)
		o.ObserveInt64(submitted, s.Submitted)
		o.ObserveInt64(completed, s.Completed)
		o.ObserveInt64(failed, s.Failed)
		o.ObserveInt64(recordsIn, s.RecordsIn)
		for _, cu := range s.Conns {
			o.ObserveInt64(connInUse, cu.InUse, metric.WithAttributes(attribute.String("connector", cu.Name)))
		}
		return nil
	}, govBudget, govUsed, govPeak, maxByMem, running, waiting, submitted, completed, failed, recordsIn, connInUse)
	if err != nil {
		return nil, fmt.Errorf("telemetry: register callback: %w", err)
	}

	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}
