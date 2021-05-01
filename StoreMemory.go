package keyvaluestore

var _ Store = (StoreMemory)(nil)

type StoreMemory map[string][]byte

func (store StoreMemory) Put(key string, value []byte) (err error) {
	store[key] = value
	return
}

func (store StoreMemory) Get(key string) (value []byte, err error) {
	value, ok := store[key]
	if !ok {
		err = ErrNotFound
		return
	}
	return
}

func (store StoreMemory) Remove(key string) (err error) {
	delete(store, key)
	return
}
