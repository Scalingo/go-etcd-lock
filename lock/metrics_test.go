package lock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLockMetrics(t *testing.T) {
	for _, test := range []struct {
		name          string
		typ           lockType
		customMetrics bool
	}{
		{name: "global provider write metrics", typ: writeLock},
		{name: "global provider read metrics", typ: readLock},
		{name: "custom write metrics", typ: writeLock, customMetrics: true},
		{name: "custom read metrics", typ: readLock, customMetrics: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
			cli := client()
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			var opts []EtcdLockerOpt
			if test.customMetrics {
				opts = append(opts, WithMetrics(provider))
			} else {
				previousProvider := otel.GetMeterProvider()
				otel.SetMeterProvider(provider)
				t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })
				opts = append(opts, WithMetrics(nil))
			}
			locker := newEtcdLocker(cli, opts...)
			acquire := locker.AcquireWithContext
			if test.typ == readLock {
				acquire = (&EtcdRWLocker{writer: locker}).AcquireReadWithContext
			}

			key := "/" + t.Name()
			held, err := acquire(t.Context(), key, 10)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = acquire(ctx, key, 10)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, held.Release())
			if test.typ == writeLock {
				require.Error(t, held.Release())
			} else {
				require.NoError(t, held.Release())
			}

			var collected metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &collected))
			require.Len(t, collected.ScopeMetrics, 1)
			metrics := map[string]metricdata.Metrics{}
			for _, item := range collected.ScopeMetrics[0].Metrics {
				metrics[item.Name] = item
			}
			require.Len(t, metrics, 3)
			attrs := attribute.NewSet(attribute.String("lock.type", string(test.typ)))
			for _, name := range []string{"etcd_lock.acquire.count", "etcd_lock.release.count"} {
				counter, ok := metrics[name].Data.(metricdata.Sum[int64])
				require.True(t, ok, "unexpected type for %s", name)
				require.Len(t, counter.DataPoints, 1)
				require.Equal(t, int64(1), counter.DataPoints[0].Value)
				require.Equal(t, attrs, counter.DataPoints[0].Attributes)
			}
			duration := metrics["etcd_lock.acquire.duration"]
			histogram, ok := duration.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			require.Equal(t, "s", duration.Unit)
			require.Len(t, histogram.DataPoints, 1)
			require.Equal(t, uint64(1), histogram.DataPoints[0].Count)
			require.Equal(t, attrs, histogram.DataPoints[0].Attributes)
		})
	}
}

func TestLockMetricsDisabledByDefault(t *testing.T) {
	locker := newEtcdLocker(nil)
	require.Nil(t, locker.metrics)
}
