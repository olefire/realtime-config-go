package konfig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdEndpoint = "localhost:2379"
	testPrefix   = "/test/config"
)

func TestRealTimeConfigHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	_, err = client.Delete(ctx, testPrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	type TestConfig struct {
		Timeout int    `etcd:"timeout"`
		Mode    string `etcd:"mode"`
	}

	cfg := &TestConfig{}
	rtc, err := NewRealTimeConfig(ctx, client, testPrefix, cfg)
	require.NoError(t, err)

	t.Run("History operations", func(t *testing.T) {
		t.Parallel()
		err = rtc.Set(ctx, "timeout", 30)
		require.NoError(t, err)
		err = rtc.Set(ctx, "mode", "dev")
		require.NoError(t, err)

		resp, err := client.Get(ctx, testPrefix+"/timeout")
		require.NoError(t, err)
		initialRev := resp.Header.Revision

		err = rtc.Set(ctx, "timeout", 60)
		require.NoError(t, err)
		err = rtc.Set(ctx, "mode", "prod")
		require.NoError(t, err)

		t.Run("Get full history", func(t *testing.T) {
			history, err := rtc.GetHistory(ctx, 0, 10)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(history), 2)

			var foundValues []any
			for _, entry := range history {
				foundValues = append(foundValues, entry.Value)
			}
			assert.Contains(t, foundValues, 60)
			assert.Contains(t, foundValues, "prod")
		})

		t.Run("Get key history", func(t *testing.T) {
			history, err := rtc.GetKeyHistory(ctx, "timeout", 0, 10)
			require.NoError(t, err)
			assert.Len(t, history, 1)

			assert.Equal(t, 60, history[0].Value)
		})

		t.Run("Rollback to initial revision", func(t *testing.T) {
			err = rtc.RollbackConfig(ctx, initialRev-1)
			require.NoError(t, err)

			err = rtc.RollbackConfig(ctx, initialRev)
			require.NoError(t, err)

			val, err := rtc.Get(ctx, "timeout")
			require.NoError(t, err)
			assert.Equal(t, 30, val)

			val, err = rtc.Get(ctx, "mode")
			require.NoError(t, err)
			assert.Equal(t, "dev", val)
		})

		t.Run("Error cases", func(t *testing.T) {
			_, err := rtc.GetKeyHistory(ctx, "nonexistent", 0, 10)
			assert.NoError(t, err)

			err = rtc.RollbackConfig(ctx, 999999)
			assert.Error(t, err)
		})
	})

	_, _ = client.Delete(context.Background(), testPrefix, clientv3.WithPrefix())
}
