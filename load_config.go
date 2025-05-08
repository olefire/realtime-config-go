package konfig

import (
	"context"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *Client) loadAll(ctx context.Context,
	defaultsInt map[string]int,
	defaultsUint map[string]uint,
	defaultsStr map[string]string,
	defaultsBool map[string]bool,
	defaultsDuration map[string]time.Duration,
) error {
	validKeys := c.collectValidKeys(defaultsInt, defaultsUint, defaultsStr, defaultsBool, defaultsDuration)

	if err := c.uploadDefaults(ctx, defaultsInt, defaultsUint, defaultsStr, defaultsBool, defaultsDuration); err != nil {
		return err
	}

	resp, err := c.cli.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("etcd get: %w", err)
	}

	for _, kv := range resp.Kvs {
		name := string(kv.Key[len(c.prefix):])
		raw := string(kv.Value)

		if _, ok := validKeys[name]; !ok {
			_, _ = c.cli.Delete(ctx, c.prefix+name)
			continue
		}

		c.updateCacheValue(name, raw)
	}

	return nil
}

func (c *Client) collectValidKeys(
	ints map[string]int,
	uints map[string]uint,
	strs map[string]string,
	bools map[string]bool,
	durations map[string]time.Duration,
) map[string]struct{} {
	keys := make(map[string]struct{})
	for k := range ints {
		keys[k] = struct{}{}
	}
	for k := range uints {
		keys[k] = struct{}{}
	}
	for k := range strs {
		keys[k] = struct{}{}
	}
	for k := range bools {
		keys[k] = struct{}{}
	}
	for k := range durations {
		keys[k] = struct{}{}
	}
	return keys
}

func (c *Client) uploadDefaults(
	ctx context.Context,
	ints map[string]int,
	uints map[string]uint,
	strs map[string]string,
	bools map[string]bool,
	durations map[string]time.Duration,
) error {
	for k, v := range ints {
		if err := c.putIfMissing(ctx, k, strconv.Itoa(v)); err != nil {
			return err
		}
	}
	for k, v := range uints {
		if err := c.putIfMissing(ctx, k, strconv.FormatUint(uint64(v), 10)); err != nil {
			return err
		}
	}
	for k, v := range strs {
		if err := c.putIfMissing(ctx, k, v); err != nil {
			return err
		}
	}
	for k, v := range bools {
		if err := c.putIfMissing(ctx, k, strconv.FormatBool(v)); err != nil {
			return err
		}
	}
	for k, v := range durations {
		if err := c.putIfMissing(ctx, k, v.String()); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) putIfMissing(ctx context.Context, name, value string) error {
	key := c.prefix + name
	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get before put: %w", err)
	}
	if len(resp.Kvs) == 0 {
		_, err = c.cli.Put(ctx, key, value)
		if err != nil {
			return fmt.Errorf("put key %s: %w", key, err)
		}
	}
	return nil
}

func (c *Client) updateCacheValue(name, raw string) {
	if _, ok := c.intMap[name]; ok {
		if v, err := strconv.Atoi(raw); err == nil {
			c.intMap[name] = v
		}
	}
	if _, ok := c.uintMap[name]; ok {
		if v, err := strconv.ParseUint(raw, 10, 0); err == nil {
			c.uintMap[name] = uint(v)
		}
	}
	if _, ok := c.boolMap[name]; ok {
		if v, err := strconv.ParseBool(raw); err == nil {
			c.boolMap[name] = v
		}
	}
	if _, ok := c.durationMap[name]; ok {
		if v, err := time.ParseDuration(raw); err == nil {
			c.durationMap[name] = v
		}
	}
	if _, ok := c.strMap[name]; ok {
		c.strMap[name] = raw
	}
}
