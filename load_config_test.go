package konfig

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRealTimeConfig_Sync(t *testing.T) {

	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	prefix := "/test/config/sync"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	t.Run("Sync with empty etcd", func(t *testing.T) {
		type Config struct {
			Timeout int    `etcd:"timeout"`
			Mode    string `etcd:"mode"`
		}

		cfg := &Config{
			Timeout: 30,
			Mode:    "production",
		}

		rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
		require.NoError(t, err)

		resp, err := rtc.Get(ctx, "timeout")
		require.NoError(t, err)
		assert.Equal(t, 30, resp)

		resp, err = rtc.Get(ctx, "mode")
		require.NoError(t, err)
		assert.Equal(t, "production", resp)
	})

	t.Run("Sync with existing etcd data", func(t *testing.T) {
		type Config struct {
			Timeout int    `etcd:"timeout"`
			Mode    string `etcd:"mode"`
		}

		cfg := &Config{
			Timeout: 30,
			Mode:    "production",
		}

		rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
		require.NoError(t, err)

		err = rtc.Set(ctx, "timeout", 60)
		require.NoError(t, err)
		_, err = client.Put(ctx, prefix+"/old_param", "value")
		require.NoError(t, err)

		resp, err := rtc.Get(ctx, "timeout")
		require.NoError(t, err)
		assert.Equal(t, 60, resp)

		resp, err = rtc.Get(ctx, "mode")
		require.NoError(t, err)
		assert.Equal(t, "production", resp)

		resp, err = rtc.Get(ctx, "old_param")
		require.Error(t, err)
		assert.Empty(t, resp)
	})
}

func TestRealTimeConfig_ConcurrentSync(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	prefix := "/test/config/concurrent"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	type Config struct {
		Counter int `etcd:"counter"`
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := &Config{Counter: 0}
			_, err := NewRealTimeConfig(ctx, client, prefix, cfg)
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}

	resp, err := client.Get(ctx, prefix+"/counter")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, "0", string(resp.Kvs[0].Value))
}

func TestRealTimeConfig_ComplexTypes(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	prefix := "/test/config/complex"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	t.Run("Slice type", func(t *testing.T) {
		type Config struct {
			Servers []string `etcd:"servers"`
		}

		cfg := &Config{
			Servers: []string{"server1", "server2"},
		}

		rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
		require.NoError(t, err)

		resp, err := rtc.Get(ctx, "servers")
		require.NoError(t, err)
		assert.Equal(t, []string{"server1", "server2"}, resp)

		updSlice := []string{"server3", "server4"}
		err = rtc.Set(ctx, "servers", updSlice)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return len(cfg.Servers) == 2 && cfg.Servers[0] == "server3"
		}, time.Second, 100*time.Millisecond)
	})

	t.Run("Map type", func(t *testing.T) {
		type Config struct {
			Settings map[string]int `etcd:"settings"`
		}

		cfg := &Config{
			Settings: map[string]int{"timeout": 30},
		}

		rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
		require.NoError(t, err)

		expected := map[string]int{"timeout": 30}
		resp, err := rtc.Get(ctx, "settings")
		require.NoError(t, err)
		assert.Equal(t, expected, resp)
	})
}
