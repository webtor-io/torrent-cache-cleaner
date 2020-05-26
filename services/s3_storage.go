package services

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

type S3Storage struct {
	bucket string
	cl     *S3Client
}

const (
	AWS_BUCKET = "aws-bucket"
)

func RegisterS3StorageFlags(c *cli.App) {
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:   AWS_BUCKET,
		Usage:  "AWS Bucket",
		Value:  "",
		EnvVar: "AWS_BUCKET",
	})
}

func NewS3Storage(c *cli.Context, cl *S3Client) *S3Storage {
	return &S3Storage{
		bucket: c.String(AWS_BUCKET),
		cl:     cl,
	}
}

func (s *S3Storage) GetTouches(ctx context.Context, startAfter string) ([]*s3.Object, bool, error) {
	log.Infof("Loading touches after=%v", startAfter)
	input := &s3.ListObjectsV2Input{
		Prefix: aws.String("touch/"),
		Bucket: aws.String(s.bucket),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}

	list, err := s.cl.Get().ListObjectsV2WithContext(ctx, input)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to get touches")
	}
	t := *list.IsTruncated
	return list.Contents, t, nil
}

func (s *S3Storage) DeleteTorrentData(ctx context.Context, h string) (int, error) {
	nn := 0
	for {
		n, t, err := s.deleteTorrentDataChunk(ctx, h)
		if err != nil {
			return n, err
		}
		nn = nn + n
		if !t {
			break
		}
	}
	k := h + ".torrent"
	log.Infof("Deleting torrent key=%v", k)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	return nn, nil
}

func (s *S3Storage) deleteTorrentDataChunk(ctx context.Context, h string) (int, bool, error) {
	list, err := s.cl.Get().ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Prefix: aws.String(h),
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return 0, false, err
	}
	ch := make(chan *s3.Object)
	mux := &sync.Mutex{}
	c := 20
	n := 0
	var wg sync.WaitGroup
	dctx, cancel := context.WithCancel(ctx)
	var derr error
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			for o := range ch {
				k := *o.Key
				// log.Infof("Deleting key=%v", k)
				_, err := s.cl.Get().DeleteObjectWithContext(dctx, &s3.DeleteObjectInput{
					Key:    o.Key,
					Bucket: aws.String(s.bucket),
				})
				if err != nil {
					log.WithError(err).Errorf("Failed to delete key=%v", k)
					derr = err
					cancel()
					break
				}
				mux.Lock()
				n = n + 1
				mux.Unlock()
			}
			wg.Done()
		}()
	}
	for _, o := range list.Contents {
		if dctx.Err() != nil {
			break
		}
		ch <- o
	}
	close(ch)
	wg.Wait()
	isTruncated := *list.IsTruncated
	return n, isTruncated, derr
}

func (s *S3Storage) DeleteTouch(ctx context.Context, h string) error {
	k := "touch/" + h
	log.Infof("Deleting touch key=%v")
	_, err := s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return err
	}
	return err
}
