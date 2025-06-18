package konfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrRevisionNotFound = errors.New("revision not found")
	ErrKeyNotFound      = errors.New("key not found")
	ErrVersionNotFound  = errors.New("version not found")
)

// HistoryEntry представляет ревизию поля конфига
type HistoryEntry struct {
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
			Key:   string(name),
			Value: ptr.Elem().Interface(),
		})
	}

	return history, nil
}

// RollbackKeyByRevision откатывает значение ключа к указанной ревизии
func (rtc *RealTimeConfig) RollbackKeyByRevision(ctx context.Context, key ConfigName, revision int64) error {
	fullKey := rtc.prefix + "/" + string(key)

	histResp, err := rtc.client.Get(ctx, fullKey, clientv3.WithRev(revision))
	if err != nil {
		return fmt.Errorf("etcd get at revision %d for key %s failed: %w", revision, key, err)
	}
	if len(histResp.Kvs) == 0 {
		return fmt.Errorf("%w: revision %d not found for key %s", ErrRevisionNotFound, revision, key)
	}

	histKV := histResp.Kvs[0]

	field, ok := rtc.schema[key]
	if !ok {
		return fmt.Errorf("field metadata for key %s not found", key)
	}

	var rawVal any
	if err = json.Unmarshal(histKV.Value, &rawVal); err != nil {
		return fmt.Errorf("failed to unmarshal value for key %s at revision %d: %w", key, revision, err)
	}

	convertedVal, err := convertType(rawVal, field.Type)
	if err != nil {
		return fmt.Errorf("type conversion failed for field %s: %w", key, err)
	}

	if err = rtc.Set(ctx, key, convertedVal); err != nil {
		return fmt.Errorf("rollback to revision %d failed for key %s: %w", revision, key, err)
	}

	return nil
}

// RollbackKeyByVersion откатывает конфиг к указанной версии
func (rtc *RealTimeConfig) RollbackKeyByVersion(ctx context.Context, key ConfigName, version int64) error {
	fullKey := rtc.prefix + "/" + string(key)

	getResp, err := rtc.client.Get(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("etcd get failed for key %s: %w", key, err)
	}
	if len(getResp.Kvs) == 0 {
		return fmt.Errorf("%w: key %s not found", ErrKeyNotFound, key)
	}

	kv := getResp.Kvs[0]
	modRev := kv.ModRevision
	createRev := kv.CreateRevision

	field, ok := rtc.schema[key]
	if !ok {
		return fmt.Errorf("field metadata for key %s not found", key)
	}

	for rev := modRev; rev >= createRev; rev-- {
		histResp, err := rtc.client.Get(ctx, fullKey, clientv3.WithRev(rev))
		if err != nil {
			return fmt.Errorf("etcd get rev %d for key %s failed: %w", rev, key, err)
		}
		if len(histResp.Kvs) == 0 {
			continue
		}

		histKV := histResp.Kvs[0]
		if histKV.Version != version {
			continue
		}

		var rawVal any
		if err := json.Unmarshal(histKV.Value, &rawVal); err != nil {
			return fmt.Errorf("failed to unmarshal value for key %s: %w", key, err)
		}

		convertedVal, err := convertType(rawVal, field.Type)
		if err != nil {
			return fmt.Errorf("type conversion failed for field %s: %w", key, err)
		}

		if err = rtc.Set(ctx, key, convertedVal); err != nil {
			return fmt.Errorf("rollback failed for key %s: %w", key, err)
		}

		return nil
	}

	return fmt.Errorf("%w: version %d not found for key %s", ErrVersionNotFound, version, key)
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
	seenEntries := make(map[string]struct{})

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
				entryID := fmt.Sprintf("%s@%d", entry.Key, entry.ModRev)

				if _, exists := seenEntries[entryID]; !exists {
					seenEntries[entryID] = struct{}{}
					allEntries = append(allEntries, *entry)
				}
			}
		}
	}

	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].ModRev > allEntries[j].ModRev
	})

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
		CreateRev: kv.CreateRevision,
		ModRev:    kv.ModRevision,
		Version:   kv.Version,
	}, nil
}
