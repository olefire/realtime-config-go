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

// HistoryEntry представляет одну версию изменения конфига
type HistoryEntry struct {
	Revision int64       `json:"revision"`
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
}

// GetHistory возвращает историю изменений для всех ключей
func (rtc *RealTimeConfig) GetHistory(ctx context.Context, fromRev int64, limit int64) ([]HistoryEntry, error) {
	resp, err := rtc.client.Get(ctx, rtc.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(fromRev),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf("etcd get history failed: %w", err)
	}

	return rtc.parseHistoryResponse(resp)
}

// GetKeyHistory возвращает историю изменений для конкретного ключа
func (rtc *RealTimeConfig) GetKeyHistory(ctx context.Context, key string, fromRev int64, limit int64) ([]HistoryEntry, error) {
	fullKey := rtc.prefix + "/" + key
	resp, err := rtc.client.Get(ctx, fullKey,
		clientv3.WithRev(fromRev),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf("etcd get key history failed: %w", err)
	}

	return rtc.parseHistoryResponse(resp)
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
