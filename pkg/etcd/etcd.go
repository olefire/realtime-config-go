package etcd

import (
	"context"
	"fmt"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

const timeout = 3 * time.Second

type KV interface {
	Put(ctx context.Context, key, val string, opts ...etcdv3.OpOption) (*etcdv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...etcdv3.OpOption) (*etcdv3.GetResponse, error)
}

type Watcher interface {
	Watch(ctx context.Context, key string, opts ...etcdv3.OpOption) etcdv3.WatchChan
	Close() error
}

type ETCD interface {
	KV
	Watcher
}

type Client struct {
	etcd    ETCD
	timeout time.Duration
}

func New(etcdClient ETCD) *Client {
	return &Client{
		etcd:    etcdClient,
		timeout: timeout,
	}
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.etcd.Get(ctx, key, etcdv3.WithLastRev()...)
	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("etcd get failed: key not found")
	}

	return resp.Kvs[0].Value, nil
}

func (c *Client) Put(ctx context.Context, key, val string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.etcd.Put(ctx, key, val)
	if err != nil {
		return fmt.Errorf("etcd put failed: %w", err)
	}

	return nil
}

func (c *Client) Watch(ctx context.Context, key string) etcdv3.WatchChan {
	return c.etcd.Watch(ctx, key)
}

func (c *Client) Close() error {
	return c.etcd.Close()
}
