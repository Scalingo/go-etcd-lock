package lock

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Scalingo/go-utils/errors/v3"
)

const meterName = "github.com/Scalingo/go-etcd-lock/v5/lock"

type lockType string

const (
	readLock  lockType = "read"
	writeLock lockType = "write"

	acquiredResult      = "acquired"
	alreadyLockedResult = "already_locked"
	errorResult         = "error"
	okResult            = "ok"
)

type lockMetrics struct {
	acquireCounter  metric.Int64Counter
	releaseCounter  metric.Int64Counter
	acquireDuration metric.Float64Histogram
	heldLocks       metric.Int64UpDownCounter
}

func initMetrics() *lockMetrics {
	return newLockMetrics(otel.GetMeterProvider())
}

func newLockMetrics(provider metric.MeterProvider) *lockMetrics {
	meter := provider.Meter(meterName)

	acquireCounter, err := meter.Int64Counter("etcd_lock.acquire.count", metric.WithDescription("Number of lock acquisition attempts"))
	if err != nil {
		otel.Handle(err)
		return nil
	}
	releaseCounter, err := meter.Int64Counter("etcd_lock.release.count", metric.WithDescription("Number of lock release attempts"))
	if err != nil {
		otel.Handle(err)
		return nil
	}
	acquireDuration, err := meter.Float64Histogram("etcd_lock.acquire.duration", metric.WithDescription("Duration of lock acquisition"), metric.WithUnit("s"))
	if err != nil {
		otel.Handle(err)
		return nil
	}
	heldLocks, err := meter.Int64UpDownCounter("etcd_lock.held", metric.WithDescription("Number of locks currently held"))
	if err != nil {
		otel.Handle(err)
		return nil
	}

	return &lockMetrics{
		acquireCounter:  acquireCounter,
		releaseCounter:  releaseCounter,
		acquireDuration: acquireDuration,
		heldLocks:       heldLocks,
	}
}

func acquireResult(err error) string {
	if err == nil {
		return acquiredResult
	}
	var alreadyLocked *ErrAlreadyLocked
	if errors.As(err, &alreadyLocked) {
		return alreadyLockedResult
	}
	return errorResult
}

func (m *lockMetrics) recordAcquire(ctx context.Context, typ lockType, wait bool, result string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("lock.type", string(typ)),
		attribute.Bool("lock.wait", wait),
		attribute.String("lock.result", result),
	)
	m.acquireCounter.Add(ctx, 1, attrs)
	m.acquireDuration.Record(ctx, duration.Seconds(), attrs)
	if result == "acquired" {
		m.heldLocks.Add(ctx, 1, metric.WithAttributes(attribute.String("lock.type", string(typ))))
	}
}

// released indicates that this call actually freed the lock. A release that
// leaves the lock held, or a repeated release, must not decrement the count.
func (m *lockMetrics) recordRelease(ctx context.Context, typ lockType, result string, released bool) {
	if m == nil {
		return
	}
	m.releaseCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("lock.type", string(typ)),
		attribute.String("lock.result", result),
	))
	if released {
		m.heldLocks.Add(ctx, -1, metric.WithAttributes(attribute.String("lock.type", string(typ))))
	}
}
