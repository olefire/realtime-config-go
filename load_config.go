package konfig

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// syncWithDefaults синхронизирует etcd со значениями по умолчанию из структуры
func (rtc *RealTimeConfig) syncWithDefaults(ctx context.Context) error {
	current, err := rtc.getCurrentValues(ctx)
	if err != nil {
		return err
	}

	defaults := rtc.getDefaultValues()

	return rtc.applySync(ctx, defaults, current)
}

// getDefaultValues извлекает значения по умолчанию из структуры
func (rtc *RealTimeConfig) getDefaultValues() map[ConfigName]any {
	defaults := make(map[ConfigName]any)
	v := reflect.ValueOf(rtc.cfg).Elem()

	for name, field := range rtc.schema {
		defaults[name] = v.Field(field.FieldIdx).Interface()
	}

	return defaults
}

// getCurrentValues получает текущие значения из etcd
func (rtc *RealTimeConfig) getCurrentValues(ctx context.Context) (map[ConfigName][]byte, error) {
	resp, err := rtc.client.Get(ctx, rtc.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}

	values := make(map[ConfigName][]byte)
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), rtc.prefix+"/")
		values[ConfigName(key)] = kv.Value
	}

	return values, nil
}

func (rtc *RealTimeConfig) applySync(ctx context.Context, defaults map[ConfigName]any, current map[ConfigName][]byte) error {
	txn := rtc.client.Txn(ctx)

	var putOps []clientv3.Op
	for name, defVal := range defaults {
		if _, exists := current[name]; !exists {
			value, err := json.Marshal(defVal)
			if err != nil {
				return fmt.Errorf("marshal error: %w", err)
			}

			putOps = append(putOps, clientv3.OpPut(rtc.prefix+"/"+string(name), string(value)))
		}
	}

	var delOps []clientv3.Op
	for name := range current {
		if _, exists := rtc.schema[name]; !exists {
			delOps = append(delOps, clientv3.OpDelete(rtc.prefix+"/"+string(name)))
		}
	}

	if len(putOps) == 0 && len(delOps) == 0 {
		return nil
	}

	var ops []clientv3.Op
	ops = append(ops, putOps...)
	ops = append(ops, delOps...)

	txnResp, err := txn.Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("sync transaction failed: %w", err)
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("sync transaction conflict")
	}

	return nil
}

// encodeValue преобразует значение в строку для хранения в etcd
func (rtc *RealTimeConfig) encodeValue(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		jsonVal, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(jsonVal), nil
	}
}
