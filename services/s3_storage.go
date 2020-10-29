package services

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

type S3Storage struct {
	bucket       string
	bucketSpread bool
	cl           *S3Client
}

const (
	AWS_BUCKET        = "aws-bucket"
	AWS_BUCKET_SPREAD = "aws-bucket-spread"
)

func RegisterS3StorageFlags(c *cli.App) {
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:   AWS_BUCKET,
		Usage:  "AWS Bucket",
		Value:  "",
		EnvVar: "AWS_BUCKET",
	})
	c.Flags = append(c.Flags, cli.BoolFlag{
		Name:   AWS_BUCKET_SPREAD,
		Usage:  "AWS Bucket Spread",
		EnvVar: "AWS_BUCKET_SPREAD",
	})
}

func NewS3Storage(c *cli.Context, cl *S3Client) *S3Storage {
	return &S3Storage{
		bucket:       c.String(AWS_BUCKET),
		bucketSpread: c.Bool(AWS_BUCKET_SPREAD),
		cl:           cl,
	}
}

func (s *S3Storage) IsDone(ctx context.Context, hash string) (bool, error) {
	key := fmt.Sprintf("done/%v", hash)
	log.Infof("Check done bucket=%v key=%v", s.bucket, key)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	_, err := s.cl.Get().GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == aerr.Code() {
			return false, nil
		}
		return false, errors.Wrapf(err, "Failed to get done status")
	}
	return true, nil
}

func (s *S3Storage) GetTouches(ctx context.Context, startAfter string) ([]*s3.Object, bool, error) {
	log.Infof("Loading touches bucket=%v after=%v", s.bucket, startAfter)
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
	k := "torrents/" + h
	log.Infof("Deleting torrent bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "completed_pieces/" + h
	log.Infof("Deleting completed pieces bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "done/" + h
	log.Infof("Deleting done marker bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	return nn, nil
}

func (s *S3Storage) deleteTorrentDataChunk(ctx context.Context, h string) (int, bool, error) {
	bucket := s.bucket
	if s.bucketSpread {
		bucket += "-" + h[0:2]
	}
	list, err := s.cl.Get().ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Prefix: aws.String(h),
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return 0, false, err
	}
	isTruncated := *list.IsTruncated
	if len(list.Contents) == 0 && !isTruncated {
		return 0, false, nil
	}
	ch := make(chan *s3.Object)
	mux := &sync.Mutex{}
	c := len(list.Contents)/100 + 1
	n := 0
	deletePieceCount := 100
	var wg sync.WaitGroup
	dctx, cancel := context.WithCancel(ctx)
	var derr error
	for i := 0; i < c; i++ {
		wg.Add(1)
		log.Infof("Start torrent cleaning thread=%v/%v infohash=%v", i, c, h)
		go func(i int) {
			for o := range ch {
				k := *o.Key
				// log.Infof("Deleting key=%v", k)
				_, err := s.cl.Get().DeleteObjectWithContext(dctx, &s3.DeleteObjectInput{
					Key:    o.Key,
					Bucket: aws.String(bucket),
				})
				if err != nil {
					log.WithError(err).Errorf("Failed to delete bucket=%v key=%v infohash=%v thread=%v/%v", bucket, k, h, i, c)
					derr = err
					cancel()
					break
				}
				mux.Lock()
				n++
				if n > deletePieceCount {
					log.Infof("Finish cleaning pieces=%v thread=%v/%v infohash=%v", deletePieceCount, i, c, h)
					deletePieceCount += 100
				}
				mux.Unlock()
			}
			log.Infof("Finish torrent cleaning thread=%v/%v infohash=%v", i, c, h)
			wg.Done()
		}(i)
	}
	for _, o := range list.Contents {
		if dctx.Err() != nil {
			break
		}
		ch <- o
	}
	close(ch)
	wg.Wait()
	cancel()
	return n, isTruncated, derr
}

func (s *S3Storage) DeleteTouch(ctx context.Context, h string) error {
	k := "touch/" + h
	log.Infof("Deleting touch bucket=%v key=%v infohash=%v", s.bucket, k, h)
	_, err := s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return err
	}
	return err
}
