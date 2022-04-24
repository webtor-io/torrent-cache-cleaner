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
	cs "github.com/webtor-io/common-services"
)

type S3Storage struct {
	bucket       string
	bucketSpread bool
	cl           *cs.S3Client
}

const (
	AWS_BUCKET        = "aws-bucket"
	AWS_BUCKET_SPREAD = "aws-bucket-spread"
)

func RegisterS3StorageFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{
			Name:   AWS_BUCKET,
			Usage:  "AWS Bucket",
			Value:  "",
			EnvVar: "AWS_BUCKET",
		},
		cli.BoolFlag{
			Name:   AWS_BUCKET_SPREAD,
			Usage:  "AWS Bucket Spread",
			EnvVar: "AWS_BUCKET_SPREAD",
		},
	)
}

func NewS3Storage(c *cli.Context, cl *cs.S3Client) *S3Storage {
	return &S3Storage{
		bucket:       c.String(AWS_BUCKET),
		bucketSpread: c.Bool(AWS_BUCKET_SPREAD),
		cl:           cl,
	}
}

func (s *S3Storage) GetTouch(ctx context.Context, hash string) (*s3.GetObjectOutput, error) {
	key := fmt.Sprintf("touch/%v", hash)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	o, err := s.cl.Get().GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to get check touch")
	}
	defer o.Body.Close()
	return o, nil
}

func (s *S3Storage) IsDone(ctx context.Context, hash string) (bool, error) {
	key := fmt.Sprintf("done/%v", hash)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	o, err := s.cl.Get().GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to get done status")
	}
	defer o.Body.Close()
	return true, nil
}

func (s *S3Storage) IsTranscoded(ctx context.Context, hash string) (bool, error) {
	key := fmt.Sprintf("%v/index.m3u8", hash)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	o, err := s.cl.Get().GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to get transcoded status")
	}
	defer o.Body.Close()
	return true, nil
}

func (s *S3Storage) GetAllObjects(ctx context.Context, prefix string, startAfter string) ([]*s3.Object, bool, error) {
	// log.Infof("loading objects bucket=%v after=%v", s.bucket, startAfter)
	input := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.Marker = aws.String(startAfter)
	}

	list, err := s.cl.Get().ListObjectsWithContext(ctx, input)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to get objects")
	}
	t := *list.IsTruncated
	return list.Contents, t, nil
}

func (s *S3Storage) GetTouches(ctx context.Context, startAfter string, hash string) ([]*s3.Object, bool, error) {
	log.Infof("loading touches bucket=%v after=%v", s.bucket, startAfter)
	prefix := "touch/"
	if hash != "" {
		prefix += hash
	}
	input := &s3.ListObjectsInput{
		Prefix: aws.String(prefix),
		Bucket: aws.String(s.bucket),
	}
	if startAfter != "" {
		input.Marker = aws.String(startAfter)
	}

	list, err := s.cl.Get().ListObjectsWithContext(ctx, input)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to get touches")
	}
	t := *list.IsTruncated
	return list.Contents, t, nil
}

func (s *S3Storage) DeleteTorrentData(ctx context.Context, h string, cc int) (int, error) {
	nn := 0
	for {
		n, t, err := s.deleteTorrentDataChunk(ctx, h, cc)
		if err != nil {
			return n, err
		}
		nn = nn + n
		// log.Infof("finish cleaning pieces=%v infohash=%v", nn, h)
		if !t {
			break
		}
	}
	k := "torrents/" + h
	// log.Infof("deleting torrent bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "completed_pieces/" + h
	// log.Infof("deleting completed pieces bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "downloaded_size/" + h
	// log.Infof("deleting downloaded size bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "done/" + h
	// log.Infof("deleting done marker bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	k = "touch/" + h
	// log.Infof("deleting touch bucket=%v key=%v infohash=%v", s.bucket, k, h)
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	return nn, nil
}

func (s *S3Storage) deleteTorrentDataChunk(ctx context.Context, h string, cc int) (int, bool, error) {
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
	if len(list.Contents) < c {
		c = len(list.Contents)
	}
	if cc < c {
		c = cc
	}
	n := 0
	var wg sync.WaitGroup
	wg.Add(c)
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var derr error
	for i := 0; i < c; i++ {
		// log.Infof("start torrent cleaning thread=%v/%v infohash=%v", i, c, h)
		go func(i int) {
			defer wg.Done()
			for o := range ch {
				k := *o.Key
				// log.Infof("Deleting key=%v", k)
				_, err := s.cl.Get().DeleteObjectWithContext(dctx, &s3.DeleteObjectInput{
					Key:    o.Key,
					Bucket: aws.String(bucket),
				})
				if err != nil {
					log.WithError(err).Errorf("failed to delete bucket=%v key=%v infohash=%v thread=%v/%v", bucket, k, h, i, c)
					derr = err
					cancel()
					break
				}
				mux.Lock()
				n++
				mux.Unlock()
			}
			// log.Infof("finish torrent cleaning thread=%v/%v infohash=%v", i, c, h)
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
	return n, isTruncated, derr
}
