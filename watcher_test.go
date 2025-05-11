package konfig

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	prefix := "/test/config/watch"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	type TestConfig struct {
		Value int `etcd:"value"`
	}

	cfg := &TestConfig{}
	rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		rtc.watch(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	_, err = client.Put(ctx, prefix+"/value", "42")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return cfg.Value == 42
	}, time.Second, 100*time.Millisecond, "Config value should be updated")

	_, err = client.Put(ctx, prefix+"/value", "100")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return cfg.Value == 100
	}, time.Second, 100*time.Millisecond, "Config value should be updated again")

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Watch goroutine didn't stop on context cancellation")
	}
}

func TestWatchWithMap(t *testing.T) {
	ctx := context.Background()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	prefix := "/test/config/watch_map"
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	type TestConfig struct {
		Settings map[string]int `etcd:"settings"`
	}

	cfg := &TestConfig{}
	rtc, err := NewRealTimeConfig(ctx, client, prefix, cfg)
	require.NoError(t, err)

	go rtc.watch(ctx)
	time.Sleep(100 * time.Millisecond)

	jsonValue, err := json.Marshal(map[string]int{"timeout": 30, "retries": 3})
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"/settings", string(jsonValue))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return cfg.Settings != nil && cfg.Settings["timeout"] == 30
	}, time.Second, 100*time.Millisecond)
}
