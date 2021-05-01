package keyvaluestore

import (
	"bytes"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var _ Store = (*StoreS3)(nil)

type StoreS3 struct {
	S3         *s3.S3
	Uploader   *s3manager.Uploader
	Downloader *s3manager.Downloader
	Bucket     string
	StoreName  string
}

func (store *StoreS3) GetKey(key string) *string {
	return aws.String(strings.Join([]string{store.StoreName, key}, "/"))
}

func (store *StoreS3) Put(key string, value []byte) (err error) {
	_, err = store.Uploader.Upload(&s3manager.UploadInput{
		Bucket: &store.Bucket,
		Key:    store.GetKey(key),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		return
	}
	return
}

func (store *StoreS3) Get(key string) (value []byte, err error) {
	var buf aws.WriteAtBuffer
	_, err = store.Downloader.Download(&buf, &s3.GetObjectInput{
		Bucket: &store.Bucket,
		Key:    store.GetKey(key),
	})
	if err != nil {
		switch nerr := err.(type) {
		case awserr.Error:
			switch nerr.Code() {
			case s3.ErrCodeNoSuchKey:
				err = ErrNotFound
				return
			}
		}
		return
	}
	value = buf.Bytes()
	return
}

func (store *StoreS3) Remove(key string) (err error) {
	_, err = store.S3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &store.Bucket,
		Key:    store.GetKey(key),
	})
	if err != nil {
		return
	}
	return
}
