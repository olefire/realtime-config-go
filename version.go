package konfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrRevisionNotFound = errors.New("revision not found")
)

// HistoryEntry представляет ревизию поля конфига
type HistoryEntry struct {
	Revision  int64       `json:"revision"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	CreateRev int64       `json:"create_rev"`
	ModRev    int64       `json:"mod_rev"`
	Version   int64       `json:"version"`
}

// GetHistory возвращает историю изменений для всех ключей
func (rtc *RealTimeConfig) GetHistory(ctx context.Context, fromRev int64, limit int64) ([]HistoryEntry, error) {
	return rtc.getKeyHistory(ctx, rtc.prefix, fromRev, limit)
}

func (rtc *RealTimeConfig) GetHistoryByRevisions(ctx context.Context, fromRev, toRev, limit int64) ([]HistoryEntry, error) {
	if toRev == 0 {
		resp, err := rtc.client.Get(ctx, rtc.prefix, clientv3.WithPrefix())
		if err != nil {
			return nil, fmt.Errorf("get current revision failed: %w", err)
		}
		toRev = resp.Header.Revision
	}

	var entries []HistoryEntry
	seen := make(map[string]map[int64]struct{})

	for rev := toRev; rev >= fromRev; rev-- {
		resp, err := rtc.client.Get(ctx, rtc.prefix,
			clientv3.WithPrefix(),
			clientv3.WithRev(rev),
		)
		if err != nil {
			return nil, fmt.Errorf("get at revision %d failed: %w", rev, err)
		}

		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			if _, ok := seen[key]; !ok {
				seen[key] = make(map[int64]struct{})
			}
			if _, duplicate := seen[key][kv.ModRevision]; duplicate {
				continue
			}
			seen[key][kv.ModRevision] = struct{}{}

			entries = append(entries, HistoryEntry{
				Key:       key,
				Value:     string(kv.Value),
				Revision:  resp.Header.Revision,
				CreateRev: kv.CreateRevision,
				ModRev:    kv.ModRevision,
				Version:   kv.Version,
			})

			if limit > 0 && int64(len(entries)) >= limit {
				return entries, nil
			}
		}
	}

	return entries, nil
}

// GetKeyHistory возвращает историю изменений для конкретного ключа
func (rtc *RealTimeConfig) GetKeyHistory(ctx context.Context, key string, fromRev int64, limit int64) ([]HistoryEntry, error) {
	fullKey := rtc.prefix + "/" + key
	return rtc.getKeyHistory(ctx, fullKey, fromRev, limit)
}

func (rtc *RealTimeConfig) parseHistoryResponse(resp *clientv3.GetResponse) ([]HistoryEntry, error) {
	var history []HistoryEntry

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		name := ConfigName(strings.TrimPrefix(key, rtc.prefix+"/"))

		meta, ok := rtc.schema[name]
		if !ok {
			continue
		}

		ptr := reflect.New(meta.Type)
		if err := json.Unmarshal(kv.Value, ptr.Interface()); err != nil {
			continue
		}

		history = append(history, HistoryEntry{
			Revision: kv.ModRevision,
			Key:      string(name),
			Value:    ptr.Elem().Interface(),
		})
	}

	return history, nil
}

// RollbackConfig откатывает конфиг к указанной ревизии
func (rtc *RealTimeConfig) RollbackConfig(ctx context.Context, rev int64) error {
	history, err := rtc.GetHistory(ctx, rev, 1)
	if err != nil {
		return err
	}

	if len(history) == 0 {
		return ErrRevisionNotFound
	}

	if err = rtc.Set(ctx, ConfigName(history[0].Key), history[0].Value); err != nil {
		return fmt.Errorf("rollback failed for key %s: %w", history[0].Key, err)
	}

	return nil
}

func (rtc *RealTimeConfig) getKeyHistory(ctx context.Context, prefix string, fromRev int64, limit int64) ([]HistoryEntry, error) {
	resp, err := rtc.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get prefix failed: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var allEntries []HistoryEntry

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		modRev := kv.ModRevision
		createRev := kv.CreateRevision

		startRev := modRev
		if fromRev > 0 && fromRev < modRev {
			startRev = fromRev
		}
		if startRev < createRev {
			startRev = createRev
		}

		for rev := startRev; rev >= createRev; rev-- {
			entry, err := rtc.getRevAsEntry(ctx, key, rev)
			if err != nil {
				return nil, err
			}
			if entry != nil {
				allEntries = append(allEntries, *entry)
			}
		}
	}

	if limit > 0 && int64(len(allEntries)) > limit {
		allEntries = allEntries[:limit]
	}

	return allEntries, nil
}

func (rtc *RealTimeConfig) getRevAsEntry(ctx context.Context, key string, rev int64) (*HistoryEntry, error) {
	resp, err := rtc.client.Get(ctx, key, clientv3.WithRev(rev))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	kv := resp.Kvs[0]
	return &HistoryEntry{
		Key:       string(kv.Key),
		Value:     string(kv.Value),
		Revision:  resp.Header.Revision,
		CreateRev: kv.CreateRevision,
		ModRev:    kv.ModRevision,
		Version:   kv.Version,
	}, nil
}
