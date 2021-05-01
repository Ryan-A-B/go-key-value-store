package keyvaluestore

import (
	"errors"
)

var ErrNotFound = errors.New("not found")

type Store interface {
	Put(key string, value []byte) (err error)
	Get(key string) (value []byte, err error)
	Remove(key string) (err error)
}
