package lock

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/Scalingo/go-etcd-lock/v5/lock"

type lockType string

const (
	readLock  lockType = "read"
	writeLock lockType = "write"
)

type lockMetrics struct {
	acquireCounter  metric.Int64Counter
	releaseCounter  metric.Int64Counter
	acquireDuration metric.Float64Histogram
}

// WithMetrics configures the OpenTelemetry meter provider used by a locker.
// A nil provider uses the global OpenTelemetry meter provider.
func WithMetrics(provider metric.MeterProvider) EtcdLockerOpt {
	return func(locker *EtcdLocker) {
		if provider == nil {
			locker.metrics = newLockMetrics(otel.GetMeterProvider())
			return
		}
		locker.metrics = newLockMetrics(provider)
	}
}

func newLockMetrics(provider metric.MeterProvider) *lockMetrics {
	meter := provider.Meter(meterName)

	acquireCounter, err := meter.Int64Counter("etcd_lock.acquire.count", metric.WithDescription("Successful lock acquisitions"))
	if err != nil {
		reportMetricInitError("etcd_lock.acquire.count", err)
		return nil
	}
	releaseCounter, err := meter.Int64Counter("etcd_lock.release.count", metric.WithDescription("Successful releases that effectively free a lock"))
	if err != nil {
		reportMetricInitError("etcd_lock.release.count", err)
		return nil
	}
	acquireDuration, err := meter.Float64Histogram("etcd_lock.acquire.duration", metric.WithDescription("Duration of successful acquisitions, including retries and waits"), metric.WithUnit("s"))
	if err != nil {
		reportMetricInitError("etcd_lock.acquire.duration", err)
		return nil
	}
	return &lockMetrics{
		acquireCounter:  acquireCounter,
		releaseCounter:  releaseCounter,
		acquireDuration: acquireDuration,
	}
}

func reportMetricInitError(name string, err error) {
	slog.Error("Disable lock metrics after instrument initialization error", "metric_name", name, "error", err)
	otel.Handle(err)
}

// recordAcquire increments the counter once per successful acquisition.
func (m *lockMetrics) recordAcquire(ctx context.Context, typ lockType, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("lock.type", string(typ)))
	m.acquireCounter.Add(ctx, 1, attrs)
	m.acquireDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *lockMetrics) recordRelease(ctx context.Context, typ lockType) {
	if m == nil {
		return
	}
	m.releaseCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("lock.type", string(typ))))
}
