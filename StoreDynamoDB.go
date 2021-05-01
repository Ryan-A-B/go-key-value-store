package keyvaluestore

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var _ Store = (*StoreDynamoDB)(nil)

type StoreDynamoDB struct {
	DynamoDB     *dynamodb.DynamoDB
	TableName    string
	PartitionKey string
	ValueKey     string
	StoreName    string
}

func (store *StoreDynamoDB) GetKey(key string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		S: aws.String(strings.Join([]string{store.StoreName, key}, ":")),
	}
}

func (store *StoreDynamoDB) Put(key string, value []byte) (err error) {
	_, err = store.DynamoDB.PutItem(&dynamodb.PutItemInput{
		TableName: &store.TableName,
		Item: map[string]*dynamodb.AttributeValue{
			store.PartitionKey: store.GetKey(key),
			store.ValueKey:     &dynamodb.AttributeValue{B: value},
		},
	})
	if err != nil {
		return
	}
	return
}

func (store *StoreDynamoDB) Get(key string) (value []byte, err error) {
	output, err := store.DynamoDB.GetItem(&dynamodb.GetItemInput{
		TableName: &store.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			store.PartitionKey: store.GetKey(key),
		},
	})
	if err != nil {
		return
	}
	if len(output.Item) == 0 {
		err = ErrNotFound
		return
	}
	value = output.Item[store.ValueKey].B
	return
}

func (store *StoreDynamoDB) Remove(key string) (err error) {
	_, err = store.DynamoDB.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: &store.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			store.PartitionKey: store.GetKey(key),
		},
	})
	if err != nil {
		return
	}
	return
}
