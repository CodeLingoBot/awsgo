package awsgo

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
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

func NewAwsService(region, awsKey, awsSecret string) (*AwsService, error) {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(awsKey, awsSecret, ""),
	})
	if err != nil {
		return nil, err
	}
	return &AwsService{Session: s}, nil
}

func (a *AwsService) S3UploadDir(path, bucket, key string) error {

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
			err = a.S3UploadDir(filepath.Join(path, f.Name()), bucket, s3Path)
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
	}

	return u.UploadWithIterator(aws.BackgroundContext(), &s3manager.UploadObjectsIterator{Objects: objects})
}

func (a *AwsService) S3ListObjects(path, bucket string, fn func(*s3.ListObjectsV2Output, bool) bool) error {
	svc := s3.New(a.Session)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(path),
	}

	return svc.ListObjectsV2Pages(input, fn)
}

func (a *AwsService) S3DownloadObject(path, objName, bucket, key string, retry int) (int64, error) {

	d := s3manager.NewDownloader(a.Session)

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return 0, err
	}

	file, err := os.Create(filepath.Join(path, objName))
	if err != nil {
		return 0, err
	}

	defer file.Close()
	return d.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key + "/" + objName),
		},
		func(dr *s3manager.Downloader) {
			dr.RequestOptions = append(dr.RequestOptions, func(r *request.Request) {
				r.RetryCount = retry
			})
		})
}

func (a *AwsService) S3UploadObject(bucket, key string, data []byte, retry int) (uploadOutput *s3manager.UploadOutput, err error) {

	u := s3manager.NewUploader(a.Session)
	obj := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	return u.Upload(obj, func(u *s3manager.Uploader) {
		u.RequestOptions = append(u.RequestOptions, func(r *request.Request) {
			r.RetryCount = retry
		})
	})
}
