package services

import (
	"context"

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
	s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(h + ".torrent"),
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
	objects := []*s3.ObjectIdentifier{}
	k := ""
	for _, o := range list.Contents {
		k = *o.Key
		objects = append(objects, &s3.ObjectIdentifier{
			Key: aws.String(k),
		})
	}
	_, err = s.cl.Get().DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Delete: &s3.Delete{
			Objects: objects,
		},
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete batch with key=%v", k)
	}
	isTruncated := *list.IsTruncated
	return len(list.Contents), isTruncated, err
}

func (s *S3Storage) DeleteTouch(ctx context.Context, h string) error {
	k := "touch/" + h
	_, err := s.cl.Get().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return err
	}
	return err
}
