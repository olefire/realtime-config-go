package konfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrWrongType = errors.New("cfg must be a pointer to struct")
)

type ConfigName string

type fieldSchema struct {
	Type     reflect.Type
	FieldIdx int
}

type RealTimeConfig struct {
	client *clientv3.Client
	prefix string
	schema map[ConfigName]fieldSchema
	cfg    any
}

func NewRealTimeConfig(ctx context.Context, cli *clientv3.Client, prefix string, cfg any) (*RealTimeConfig, error) {
	t := reflect.TypeOf(cfg)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, ErrWrongType
	}

	schema, err := buildSchema(cfg)
	if err != nil {
		return nil, err
	}

	rtc := &RealTimeConfig{
		client: cli,
		prefix: prefix,
		schema: schema,
		cfg:    cfg,
	}

	if err = rtc.syncWithDefaults(ctx); err != nil {
		return nil, err
	}

	go rtc.watch(ctx)

	return rtc, nil
}

func (rtc *RealTimeConfig) Get(ctx context.Context, name ConfigName) (any, error) {
	meta, ok := rtc.schema[name]
	if !ok {
		return nil, fmt.Errorf("unknown config field: %s", name)
	}

	key := rtc.prefix + "/" + string(name)
	resp, err := rtc.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("key not found in etcd: %s", key)
	}

	ptr := reflect.New(meta.Type)
	if err = json.Unmarshal(resp.Kvs[0].Value, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return ptr.Elem().Interface(), nil
}

func (rtc *RealTimeConfig) Set(ctx context.Context, name ConfigName, value any) error {
	meta, ok := rtc.schema[name]
	if !ok {
		return fmt.Errorf("unknown config field: %s", name)
	}

	convertedVal, err := convertType(value, meta.Type)
	if err != nil {
		return fmt.Errorf("type conversion failed for field %s: %w", name, err)
	}

	val := reflect.ValueOf(convertedVal)
	if val.Type() != meta.Type {
		return fmt.Errorf("invalid type after conversion for field %s: expected %s, got %s",
			name, meta.Type, val.Type())
	}

	key := rtc.prefix + "/" + string(name)
	data, err := json.Marshal(convertedVal)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	_, err = rtc.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("etcd put failed: %w", err)
	}

	fieldValue := reflect.ValueOf(rtc.cfg).Elem().Field(meta.FieldIdx)
	fieldValue.Set(val)

	return nil
}

func buildSchema(cfg any) (map[ConfigName]fieldSchema, error) {
	schema := make(map[ConfigName]fieldSchema)
	t := reflect.TypeOf(cfg).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		etcdName := field.Tag.Get("etcd")
		if etcdName == "" {
			return nil, fmt.Errorf("field %s is missing etcd tag", field.Name)
		}

		schema[ConfigName(etcdName)] = fieldSchema{
			Type:     field.Type,
			FieldIdx: i,
		}
	}

	return schema, nil
}
