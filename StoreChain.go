package keyvaluestore

var _ Store = (*StoreChain)(nil)

type StoreChain []Store

func (storeChain StoreChain) Put(key string, value []byte) (err error) {
	for _, store := range storeChain {
		err = store.Put(key, value)
		if err != nil {
			return
		}
	}
	return
}

func (storeChain StoreChain) Get(key string) (value []byte, err error) {
	for i, store := range storeChain {
		value, err = store.Get(key)
		switch err {
		case nil:
			for j := i - 1; j >= 0; j-- {
				storeChain[j].Put(key, value)
			}
			return
		case ErrNotFound:
		default:
			return
		}
	}
	return
}

func (storeChain StoreChain) Remove(key string) (err error) {
	for _, store := range storeChain {
		err = store.Remove(key)
		if err != nil {
			return
		}
	}
	return
}
