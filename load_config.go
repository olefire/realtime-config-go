package konfig

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

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

// getCurrentValues получает последние версии значений из etcd
func (rtc *RealTimeConfig) getCurrentValues(ctx context.Context) (map[ConfigName][]byte, error) {
	resp, err := rtc.client.Get(ctx, rtc.prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend))

	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}

	values := make(map[ConfigName][]byte)
	processedKeys := make(map[string]bool)

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		configName := ConfigName(strings.TrimPrefix(key, rtc.prefix+"/"))

		if processedKeys[key] {
			continue
		}
		processedKeys[key] = true

		values[configName] = kv.Value
	}

	return values, nil
}

func (rtc *RealTimeConfig) applySync(ctx context.Context, defaults map[ConfigName]any, current map[ConfigName][]byte) error {
	txn := rtc.client.Txn(ctx)
	cfgValue := reflect.ValueOf(rtc.cfg).Elem()
	fmt.Println(current)
	var putOps []clientv3.Op

	for name, currentValBytes := range current {
		field, ok := rtc.schema[name]
		if !ok {
			continue
		}

		var etcdVal any
		if err := json.Unmarshal(currentValBytes, &etcdVal); err != nil {
			return fmt.Errorf("unmarshal error for %s: %w", name, err)
		}
		fmt.Println(etcdVal)

		fieldValue := cfgValue.Field(field.FieldIdx)
		currentCfgVal := fieldValue.Interface()

		convertedVal, err := convertType(etcdVal, fieldValue.Type())
		if err != nil {
			return fmt.Errorf("type conversion failed for %s: %w", name, err)
		}
		if !reflect.DeepEqual(currentCfgVal, convertedVal) {
			fieldValue.Set(reflect.ValueOf(convertedVal))
			log.Println(rtc.cfg)
		}
	}

	for name, defVal := range defaults {
		if _, exists := current[name]; !exists {
			if field, ok := rtc.schema[name]; ok {
				fieldValue := cfgValue.Field(field.FieldIdx)
				convertedVal, err := convertType(defVal, fieldValue.Type())
				if err != nil {
					return fmt.Errorf("type conversion failed for default %s: %w", name, err)
				}

				fieldValue.Set(reflect.ValueOf(convertedVal))

				value, err := json.Marshal(defVal)
				if err != nil {
					return fmt.Errorf("marshal error: %w", err)
				}
				putOps = append(putOps, clientv3.OpPut(rtc.prefix+"/"+string(name), string(value)))
			}
		}
	}

	var delOps []clientv3.Op
	for name := range current {
		if _, exists := rtc.schema[name]; !exists {
			delOps = append(delOps, clientv3.OpDelete(rtc.prefix+"/"+string(name)))
		}
	}

	if len(putOps) > 0 || len(delOps) > 0 {
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
	}

	return nil
}

func convertType(val any, targetType reflect.Type) (any, error) {
	sourceVal := reflect.ValueOf(val)
	if targetType == reflect.TypeOf(map[string]struct{}{}) {
		if m, ok := val.(map[string]interface{}); ok {
			result := make(map[string]struct{})
			for k := range m {
				result[k] = struct{}{}
			}
			return result, nil
		}
	}

	if targetType == reflect.TypeOf(map[string]int{}) {
		if m, ok := val.(map[string]interface{}); ok {
			result := make(map[string]int)
			for k, v := range m {
				if f, ok := v.(float64); ok {
					result[k] = int(f)
				} else {
					return nil, fmt.Errorf("cannot convert map value %v to int", v)
				}
			}
			return result, nil
		}
	}

	if targetType == reflect.TypeOf(time.Duration(0)) {
		switch v := val.(type) {
		case float64:
			return time.Duration(v), nil
		case string:
			return time.ParseDuration(v)
		case int, int64:
			return time.Duration(reflect.ValueOf(v).Int()), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to time.Duration", val)
		}
	}

	if sourceVal.CanConvert(targetType) {
		return sourceVal.Convert(targetType).Interface(), nil
	}

	return nil, fmt.Errorf("cannot convert %T to %v", val, targetType)
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
