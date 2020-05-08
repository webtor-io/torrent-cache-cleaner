package services

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"

	log "github.com/sirupsen/logrus"
)

type Cleaner struct {
	st *S3Storage
}

func NewCleaner(st *S3Storage) *Cleaner {
	return &Cleaner{st: st}
}

func (s *Cleaner) cleanTorrentData(ctx context.Context, hash string) (int, error) {
	n, err := s.st.DeleteTorrentData(ctx, hash)
	if err != nil {
		return n, err
	}
	err = s.st.DeleteTouch(ctx, hash)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (s *Cleaner) Clean() error {
	for {
		t, err := s.cleanChunk()
		if err != nil {
			return err
		}
		if !t {
			break
		}
	}
	return nil
}

func (s *Cleaner) cleanChunk() (bool, error) {
	ctx := context.Background()
	touches, trunc, err := s.st.GetTouches(ctx)
	if err != nil {
		return trunc, err
	}
	ch := make(chan *s3.Object)
	c := 5
	var wg sync.WaitGroup
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			for t := range ch {
				k := *t.Key
				k = strings.TrimPrefix(k, "touch/")
				log.Infof("Start cleaning hash=%v modification date=%v", k, t.LastModified)
				start := time.Now()
				n, err := s.cleanTorrentData(ctx, k)
				if err != nil {
					log.WithError(err).Infof("Failed to clean hash=%v", k)
				} else {
					log.Infof("Done cleaning hash=%v pieces=%v elapsed time=%v", k, n, time.Since(start))
				}
			}
			wg.Done()
		}()
	}
	for _, t := range touches {
		if t.LastModified.Before(time.Now().Add(-36 * time.Hour)) {
			ch <- t
		}
	}
	close(ch)
	wg.Wait()

	return trunc, nil
}
