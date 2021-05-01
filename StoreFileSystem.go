package keyvaluestore

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

var _ Store = (*StoreFileSystem)(nil)

type StoreFileSystem struct {
	Path string
}

func (store *StoreFileSystem) GetFilePath(key string) string {
	return filepath.Join(store.Path, filepath.Clean(key))
}

func (store *StoreFileSystem) Put(key string, value []byte) (err error) {
	path := store.GetFilePath(key)
	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return
	}
	file, err := os.Create(path)
	if err != nil {
		return
	}
	defer file.Close()
	_, err = file.Write(value)
	if err != nil {
		return
	}
	return
}

func (store *StoreFileSystem) Get(key string) (value []byte, err error) {
	path := store.GetFilePath(key)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotFound
			return
		}
		return
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}

func (store *StoreFileSystem) Remove(key string) (err error) {
	path := store.GetFilePath(key)
	return os.Remove(path)
}
