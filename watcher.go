package konfig

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// watch отслеживание изменений
func (rtc *RealTimeConfig) watch(ctx context.Context) {
	rch := rtc.client.Watch(ctx, rtc.prefix, clientv3.WithPrefix())

	for wr := range rch {
		for _, ev := range wr.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				name := strings.TrimPrefix(string(ev.Kv.Key), rtc.prefix+"/")
				field, ok := rtc.schema[ConfigName(name)]
				if !ok {
					continue
				}

				var rawVal any
				if err := json.Unmarshal(ev.Kv.Value, &rawVal); err != nil {
					log.Printf("Failed to unmarshal value for %s: %v", name, err)
					continue
				}

				convertedVal, err := convertType(rawVal, field.Type)
				if err != nil {
					log.Printf("Type conversion failed for %s: %v", name, err)
					continue
				}

				fieldValue := reflect.ValueOf(rtc.cfg).Elem().Field(field.FieldIdx)
				valToSet := reflect.ValueOf(convertedVal)

				switch fieldValue.Kind() {
				case reflect.Slice:
					newSlice := reflect.MakeSlice(fieldValue.Type(), valToSet.Len(), valToSet.Len())
					reflect.Copy(newSlice, valToSet)
					fieldValue.Set(newSlice)
				case reflect.Map:
					newMap := reflect.MakeMap(fieldValue.Type())
					for _, key := range valToSet.MapKeys() {
						newMap.SetMapIndex(key, valToSet.MapIndex(key))
					}
					fieldValue.Set(newMap)
				default:
					fieldValue.Set(valToSet)
				}

				log.Printf("Config updated: %s = %v", name, convertedVal)
			}
		}
	}
}
