package services

import (
	"context"
	"errors"
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
	start := time.Now()
	log.Info("Start cleaning...")
	c := make(chan error)
	go func() {
		last := ""
		for {
			t, l, err := s.cleanChunk(last)
			if err != nil {
				c <- err
			}
			if !t {
				break
			}
			last = l
		}
		c <- nil
	}()
	select {
	case err := <-c:
		log.Infof("Finish cleaning elapsed time=%v", time.Since(start))
		return err
	case <-time.After(10 * time.Minute):
		return errors.New("Timeout occured")
	}
}

func (s *Cleaner) cleanChunk(marker string) (bool, string, error) {
	ctx := context.Background()
	touches, trunc, err := s.st.GetTouches(ctx, marker)
	if err != nil {
		return trunc, "", err
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
	last := ""
	for _, t := range touches {
		if t.LastModified.Before(time.Now().Add(-12 * time.Hour)) {
			ch <- t
		}
		last = *t.Key
	}
	close(ch)
	wg.Wait()

	return trunc, last, nil
}
