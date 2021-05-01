package keyvaluestore

import "encoding/json"

type JSONStore struct {
	Store Store
}

func (store *JSONStore) Put(key string, value interface{}) (err error) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	return store.Store.Put(key, data)
}

func (store *JSONStore) Get(key string, value interface{}) (err error) {
	data, err := store.Store.Get(key)
	if err != nil {
		return
	}
	return json.Unmarshal(data, value)
}

func (store *JSONStore) Remove(key string) (err error) {
	return store.Store.Remove(key)
}
