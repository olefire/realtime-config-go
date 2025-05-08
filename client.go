package konfig

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type RealTimeConfig interface {
	GetInt(name string) (int, bool)
	GetUint(name string) (uint, bool)
	GetString(name string) (string, bool)
	GetBool(name string) (bool, bool)
	Close() error
}

// Client ...
type Client struct {
	cli    *clientv3.Client
	prefix string

	intMap      map[string]int
	uintMap     map[string]uint
	strMap      map[string]string
	boolMap     map[string]bool
	durationMap map[string]time.Duration
}

// Options — параметры для подключения к etcd
type Options struct {
	Endpoints   []string
	Prefix      string
	DialTimeout time.Duration
	Zap         *zap.Logger
}

// NewConfigClient создаёт клиента, заполняя дефолты и запуская watch.
// defaultsInt, defaultsUint, defaultsStr, defaultsBool, defaultsDuration — соответствующие мапы
func NewConfigClient(ctx context.Context, opts Options,
	defaultsInt map[string]int,
	defaultsUint map[string]uint,
	defaultsStr map[string]string,
	defaultsBool map[string]bool,
	defaultsDuration map[string]time.Duration,
) (*Client, error) {
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   opts.Endpoints,
		DialTimeout: opts.DialTimeout,
		Logger:      opts.Zap,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd connect: %w", err)
	}

	c := &Client{
		cli:         cli,
		prefix:      opts.Prefix,
		intMap:      make(map[string]int),
		uintMap:     make(map[string]uint),
		strMap:      make(map[string]string),
		boolMap:     make(map[string]bool),
		durationMap: make(map[string]time.Duration),
	}

	for k, v := range defaultsInt {
		c.intMap[k] = v
	}
	for k, v := range defaultsUint {
		c.uintMap[k] = v
	}
	for k, v := range defaultsStr {
		c.strMap[k] = v
	}
	for k, v := range defaultsBool {
		c.boolMap[k] = v
	}
	for k, v := range defaultsDuration {
		c.durationMap[k] = v
	}

	if err = c.loadAll(ctx, defaultsInt, defaultsUint, defaultsStr, defaultsBool, defaultsDuration); err != nil {
		return nil, err
	}

	go c.watch(ctx)

	return c, nil
}

// GetInt возвращает int-значение
func (c *Client) GetInt(name string) (int, bool) {
	v, ok := c.intMap[name]
	return v, ok
}

// GetUint возвращает uint-значение
func (c *Client) GetUint(name string) (uint, bool) {
	v, ok := c.uintMap[name]
	return v, ok
}

// GetString возвращает string-значение
func (c *Client) GetString(name string) (string, bool) {
	v, ok := c.strMap[name]
	return v, ok
}

// GetBool возвращает bool-значение
func (c *Client) GetBool(name string) (bool, bool) {
	v, ok := c.boolMap[name]
	return v, ok
}

// Close закрывает соединение с etcd
func (c *Client) Close() error {
	return c.cli.Close()
}
