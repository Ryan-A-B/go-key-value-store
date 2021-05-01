package keyvaluestore_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	keyvaluestore "github.com/Ryan-A-B/go-key-value-store"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func EnvStringOrFatal(t *testing.T, key string) string {
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("expected %s to be set", key)
	}
	return value
}

func GetRandomString(t *testing.T) string {
	nonce := make([]byte, 20)
	_, err := rand.Read(nonce)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(nonce)
}

func testStore(t *testing.T, store keyvaluestore.Store) {
	key := GetRandomString(t)
	expectedValue := make([]byte, 2048)
	_, err := rand.Read(expectedValue)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(key)
	if err != keyvaluestore.ErrNotFound {
		t.Fatal("expected not found")
	}
	err = store.Put(key, expectedValue)
	if err != nil {
		t.Fatal(err)
	}
	actualValue, err := store.Get(key)
	if !bytes.Equal(expectedValue, actualValue) {
		t.Fatal("expected equal")
	}
	err = store.Remove(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(key)
	if err != keyvaluestore.ErrNotFound {
		t.Fatal("expected not found")
	}
}

func TestStoreMemory(t *testing.T) {
	testStore(t, make(keyvaluestore.StoreMemory))
}

func TestStoreChain(t *testing.T) {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	testStore(t, keyvaluestore.StoreChain{
		make(keyvaluestore.StoreMemory),
		&keyvaluestore.StoreDynamoDB{
			DynamoDB:     dynamodb.New(sess),
			TableName:    EnvStringOrFatal(t, "KEY_VALUE_STORE_TABLE_NAME"),
			PartitionKey: EnvStringOrFatal(t, "KEY_VALUE_STORE_PARTITION_KEY"),
			ValueKey:     EnvStringOrFatal(t, "KEY_VALUE_STORE_VALUE_KEY"),
			StoreName:    "testing",
		},
		&keyvaluestore.StoreS3{
			S3:         svc,
			Uploader:   s3manager.NewUploaderWithClient(svc),
			Downloader: s3manager.NewDownloaderWithClient(svc),
			Bucket:     EnvStringOrFatal(t, "KEY_VALUE_STORE_BUCKET"),
			StoreName:  "testing",
		},
	})
}

func TestStoreChainCascade(t *testing.T) {
	sess := session.Must(session.NewSession())
	store := keyvaluestore.StoreChain{
		make(keyvaluestore.StoreMemory),
		&keyvaluestore.StoreDynamoDB{
			DynamoDB:     dynamodb.New(sess),
			TableName:    EnvStringOrFatal(t, "KEY_VALUE_STORE_TABLE_NAME"),
			PartitionKey: EnvStringOrFatal(t, "KEY_VALUE_STORE_PARTITION_KEY"),
			ValueKey:     EnvStringOrFatal(t, "KEY_VALUE_STORE_VALUE_KEY"),
			StoreName:    "testing",
		},
	}
	key := GetRandomString(t)
	expectedValue := make([]byte, 2048)
	_, err := rand.Read(expectedValue)
	if err != nil {
		t.Fatal(err)
	}
	err = store[1].Put(key, expectedValue)
	if err != nil {
		t.Fatal(err)
	}
	actualValue, err := store.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expectedValue, actualValue) {
		t.Fatal("expected equal")
	}
	actualValue, err = store[0].Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expectedValue, actualValue) {
		t.Fatal("expected equal")
	}
	err = store.Remove(key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreFileSystem(t *testing.T) {
	testStore(t, &keyvaluestore.StoreFileSystem{
		Path: "testdata",
	})
}

func TestStoreDynamoDB(t *testing.T) {
	sess := session.Must(session.NewSession())
	testStore(t, &keyvaluestore.StoreDynamoDB{
		DynamoDB:     dynamodb.New(sess),
		TableName:    EnvStringOrFatal(t, "KEY_VALUE_STORE_TABLE_NAME"),
		PartitionKey: EnvStringOrFatal(t, "KEY_VALUE_STORE_PARTITION_KEY"),
		ValueKey:     EnvStringOrFatal(t, "KEY_VALUE_STORE_VALUE_KEY"),
		StoreName:    "testing",
	})
}

func TestStoreS3(t *testing.T) {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	testStore(t, &keyvaluestore.StoreS3{
		S3:         s3.New(sess),
		Uploader:   s3manager.NewUploaderWithClient(svc),
		Downloader: s3manager.NewDownloaderWithClient(svc),
		Bucket:     EnvStringOrFatal(t, "KEY_VALUE_STORE_BUCKET"),
		StoreName:  "testing",
	})
}
