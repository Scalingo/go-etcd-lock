package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type acquireSeries struct {
	typ    string
	wait   bool
	result string
}

type releaseSeries struct {
	typ    string
	result string
}

func TestLockMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })

	cli := client()
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	locker := newEtcdLocker(cli, WithTryLockTimeout(100*time.Millisecond))
	locker.metrics = newLockMetrics(provider)
	rwLocker := &EtcdRWLocker{writer: newEtcdLocker(cli)}
	rwLocker.writer.metrics = locker.metrics
	key := "/lock-metrics-" + t.Name()

	write, err := locker.Acquire(key, 10)
	require.NoError(t, err)
	_, err = locker.Acquire(key, 10)
	var alreadyLocked *ErrAlreadyLocked
	require.ErrorAs(t, err, &alreadyLocked)
	_, err = rwLocker.AcquireRead(key, 10)
	require.ErrorAs(t, err, &alreadyLocked)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = locker.AcquireWithContext(canceled, key, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, write.Release())
	writeLock, ok := write.(*EtcdLock)
	require.True(t, ok)
	releaseErr := writeLock.release(t.Context(), ttlRelease)
	require.NoError(t, releaseErr)
	require.Error(t, write.Release())

	read, err := rwLocker.WaitAcquireRead(key, 10)
	require.NoError(t, err)
	require.NoError(t, read.Release())
	readLock, ok := read.(*EtcdRWLock)
	require.True(t, ok)
	releaseErr = readLock.release(t.Context(), ttlRelease)
	require.NoError(t, releaseErr)
	write, err = rwLocker.WaitAcquireWrite(key, 10)
	require.NoError(t, err)
	require.NoError(t, write.Release())

	var nilLock *EtcdLock
	require.Error(t, nilLock.Release())

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	metrics := map[string]metricdata.Metrics{}
	for _, scope := range collected.ScopeMetrics {
		for _, item := range scope.Metrics {
			metrics[item.Name] = item
		}
	}
	require.Len(t, metrics, 4)
	for _, name := range []string{
		"etcd_lock.acquire.count", "etcd_lock.release.count",
		"etcd_lock.acquire.duration", "etcd_lock.held",
	} {
		require.Contains(t, metrics, name)
	}

	expectedAcquires := map[acquireSeries]int64{
		{"write", false, "acquired"}:       1,
		{"write", false, "already_locked"}: 1,
		{"write", false, "error"}:          1,
		{"read", false, "already_locked"}:  1,
		{"read", true, "acquired"}:         1,
		{"write", true, "acquired"}:        1,
	}
	acquisitions, ok := metrics["etcd_lock.acquire.count"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.True(t, acquisitions.IsMonotonic)
	gotAcquires := map[acquireSeries]int64{}
	for _, point := range acquisitions.DataPoints {
		gotAcquires[acquisitionAttributes(t, point.Attributes)] = point.Value
	}
	require.Equal(t, expectedAcquires, gotAcquires)

	duration := metrics["etcd_lock.acquire.duration"]
	require.Equal(t, "s", duration.Unit)
	histogram, ok := duration.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	gotDurations := map[acquireSeries]int64{}
	for _, point := range histogram.DataPoints {
		series := acquisitionAttributes(t, point.Attributes)
		gotDurations[series] = int64(point.Count)
		require.GreaterOrEqual(t, point.Sum, float64(0))
	}
	require.Equal(t, expectedAcquires, gotDurations)

	releases, ok := metrics["etcd_lock.release.count"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.True(t, releases.IsMonotonic)
	gotReleases := map[releaseSeries]int64{}
	for _, point := range releases.DataPoints {
		gotReleases[releaseAttributes(t, point.Attributes)] = point.Value
	}
	require.Equal(t, map[releaseSeries]int64{
		{"write", "ok"}:    2,
		{"write", "error"}: 1,
		{"read", "ok"}:     1,
	}, gotReleases)

	held, ok := metrics["etcd_lock.held"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.False(t, held.IsMonotonic)
	gotHeld := map[string]int64{}
	for _, point := range held.DataPoints {
		require.Equal(t, 1, point.Attributes.Len())
		gotHeld[attributeValue(t, point.Attributes, "lock.type").AsString()] = point.Value
	}
	require.Equal(t, map[string]int64{"write": 0, "read": 0}, gotHeld)
}

func acquisitionAttributes(t *testing.T, attrs attribute.Set) acquireSeries {
	t.Helper()
	require.Equal(t, 3, attrs.Len())
	wait := attributeValue(t, attrs, "lock.wait")
	require.Equal(t, attribute.BOOL, wait.Type())
	return acquireSeries{
		typ:    attributeValue(t, attrs, "lock.type").AsString(),
		wait:   wait.AsBool(),
		result: attributeValue(t, attrs, "lock.result").AsString(),
	}
}

func releaseAttributes(t *testing.T, attrs attribute.Set) releaseSeries {
	t.Helper()
	require.Equal(t, 2, attrs.Len())
	return releaseSeries{
		typ:    attributeValue(t, attrs, "lock.type").AsString(),
		result: attributeValue(t, attrs, "lock.result").AsString(),
	}
}

func attributeValue(t *testing.T, attrs attribute.Set, key attribute.Key) attribute.Value {
	t.Helper()
	value, ok := attrs.Value(key)
	require.True(t, ok, "missing attribute %s", key)
	return value
}
