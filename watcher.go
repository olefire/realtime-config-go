package konfig

import (
	"context"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// watch отслеживание изменений, обновление кеша
func (c *Client) watch(ctx context.Context) {
	rch := c.cli.Watch(ctx, c.prefix, clientv3.WithPrefix())
	for wr := range rch {
		for _, ev := range wr.Events {
			name := string(ev.Kv.Key[len(c.prefix):])
			raw := string(ev.Kv.Value)
			switch ev.Type {
			case clientv3.EventTypePut:
				if _, ok := c.intMap[name]; ok {
					if v, err := strconv.Atoi(raw); err == nil {
						c.intMap[name] = v
					}
				}
				if _, ok := c.uintMap[name]; ok {
					if u64, err := strconv.ParseUint(raw, 10, 0); err == nil {
						c.uintMap[name] = uint(u64)
					}
				}
				if _, ok := c.boolMap[name]; ok {
					if b, err := strconv.ParseBool(raw); err == nil {
						c.boolMap[name] = b
					}
				}
				if _, ok := c.strMap[name]; ok {
					c.strMap[name] = raw
				}
			case clientv3.EventTypeDelete:
			}
		}
	}
}
