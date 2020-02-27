package awsgo

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type (
	AwsService struct {
		Session *session.Session
	}
)

func NewAwsService(endpoint, region, awsKey, awsSecret string, s3ForcePathStyle, disableSSL bool) (*AwsService, error) {
	s, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		Credentials:      credentials.NewStaticCredentials(awsKey, awsSecret, ""),
		S3ForcePathStyle: aws.Bool(s3ForcePathStyle),
		DisableSSL:       aws.Bool(disableSSL),
	})
	if err != nil {
		return nil, err
	}
	return &AwsService{Session: s}, nil
}

func (a *AwsService) UploadDir(path, bucket, key string) error {

	u := s3manager.NewUploader(a.Session)
	var (
		objects []s3manager.BatchUploadObject
		obj     s3manager.BatchUploadObject
		s3Path  string
	)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range files {
		s3Path = filepath.Join(key, f.Name())
		if f.IsDir() {
			err = a.UploadDir(filepath.Join(path, f.Name()), bucket, s3Path)
			if err != nil {
				return err
			}
			continue
		}
		file, err := os.Open(filepath.Join(path, f.Name()))
		if err != nil {
			return err
		}
		obj = s3manager.BatchUploadObject{
			Object: &s3manager.UploadInput{
				Key:    aws.String(s3Path),
				Bucket: aws.String(bucket),
				Body:   file,
			},
		}
		objects = append(objects, obj)

		file.Close()
	}

	return u.UploadWithIterator(aws.BackgroundContext(), &s3manager.UploadObjectsIterator{Objects: objects})
}

func (a *AwsService) ListObjects(path, bucket string, fn func(*s3.ListObjectsV2Output, bool) bool) error {
	svc := s3.New(a.Session)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(path),
	}

	return svc.ListObjectsV2Pages(input, fn)
}

func (a *AwsService) ObjectExists(bucket, path string) (bool, error) {
	_, err := a.ObjectInfo(bucket, path)

	exists := err == nil
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && (aerr.Code() == "NotFound" || aerr.Code() == s3.ErrCodeNoSuchKey) {
			err = nil
		}
	}

	return exists, err
}

func (a *AwsService) ObjectInfo(bucket, path string) (*s3.HeadObjectOutput, error) {
	svc := s3.New(a.Session)
	return svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
}

func (a *AwsService) DownloadObject(bucket, key string, retry int) (data []byte, n int64, err error) {

	d := s3manager.NewDownloader(a.Session)

	b := aws.NewWriteAtBuffer([]byte{})
	n, err = d.Download(
		b,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		},
		func(dr *s3manager.Downloader) {
			dr.RequestOptions = append(dr.RequestOptions, func(r *request.Request) {
				r.RetryCount = retry
			})
		})

	data = b.Bytes()

	return
}

func (a *AwsService) DownloadObjectToFile(file *os.File, bucket, key string, retry int) (n int64, err error) {

	d := s3manager.NewDownloader(a.Session)

	n, err = d.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		},
		func(dr *s3manager.Downloader) {
			dr.RequestOptions = append(dr.RequestOptions, func(r *request.Request) {
				r.RetryCount = retry
			})
		})

	return n, err
}

func (a *AwsService) GetObject(ctx context.Context, bucket, key string) (*s3.GetObjectOutput, error) {

	svc := s3.New(a.Session)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	return svc.GetObjectWithContext(ctx, input)
}

func (a *AwsService) UploadInput(obj *s3manager.UploadInput, retry int) (uploadOutput *s3manager.UploadOutput, err error) {

	return s3manager.NewUploader(a.Session).Upload(obj, func(u *s3manager.Uploader) {
		u.RequestOptions = append(u.RequestOptions, func(r *request.Request) {
			r.RetryCount = retry
		})
	})
}

func (a *AwsService) UploadObject(bucket, key string, data []byte, retry int) (uploadOutput *s3manager.UploadOutput, err error) {

	obj := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	return a.UploadInput(obj, retry)
}
