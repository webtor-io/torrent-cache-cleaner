package services

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/urfave/cli"

	log "github.com/sirupsen/logrus"
)

const (
	DONE_TORRENTS_EXPIRE_HOURS    = "done-torrents-expire-hours"
	PARTIAL_TORRENTS_EXPIRE_HOURS = "partial-torrents-expire-hours"
)

func RegisterCleanerFlags(c *cli.App) {
	c.Flags = append(c.Flags, cli.IntFlag{
		Name:   DONE_TORRENTS_EXPIRE_HOURS,
		Usage:  "Expiration period for completly downloaded torrents (in hours)",
		Value:  36,
		EnvVar: "DONE_TORRENTS_EXPIRE_HOURS",
	})
	c.Flags = append(c.Flags, cli.IntFlag{
		Name:   PARTIAL_TORRENTS_EXPIRE_HOURS,
		Usage:  "Expiration period for partialy downloaded torrents (in hours)",
		Value:  12,
		EnvVar: "PARTIAL_TORRENTS_EXPIRE_HOURS",
	})
}

type Cleaner struct {
	st            *S3Storage
	doneExpire    int
	partialExpire int
}

func NewCleaner(c *cli.Context, st *S3Storage) *Cleaner {
	return &Cleaner{
		st:            st,
		doneExpire:    c.Int(DONE_TORRENTS_EXPIRE_HOURS),
		partialExpire: c.Int(PARTIAL_TORRENTS_EXPIRE_HOURS),
	}
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
	ctx := context.Background()
	c := make(chan error)
	ch := make(chan *s3.Object, 10000)
	tc := 10
	var wg sync.WaitGroup
	wg.Add(tc)
	for i := 0; i < tc; i++ {
		log.Infof("Start cleaning thread=%v", i)
		go func(i int) {
			defer wg.Done()
			for t := range ch {
				k := *t.Key
				k = strings.TrimPrefix(k, "touch/")
				log.Infof("Start cleaning hash=%v modification date=%v thread=%v", k, t.LastModified, i)
				start := time.Now()
				n, err := s.cleanTorrentData(ctx, k)
				if err != nil {
					log.WithError(err).Infof("Failed to clean hash=%v thread=%v", k, i)
				} else {
					log.Infof("Done cleaning hash=%v pieces=%v elapsed time=%v thread=%v", k, n, time.Since(start), i)
				}
			}
			log.Infof("Finish cleaning thread=%v", i)
		}(i)
	}
	go func() {
		go func() {
			last := ""
			for {
				t, l, err := s.cleanChunk(ctx, ch, last)
				if err != nil {
					c <- err
				}
				if !t {
					break
				}
				last = l
			}
			close(ch)
		}()
		wg.Wait()
		c <- nil
	}()
	select {
	case err := <-c:
		log.Infof("Finish cleaning elapsed time=%v", time.Since(start))
		return err
	case <-time.After(1 * time.Hour):
		return errors.New("Timeout occured")
	}
}

func (s *Cleaner) cleanChunk(ctx context.Context, ch chan *s3.Object, marker string) (bool, string, error) {
	touches, trunc, err := s.st.GetTouches(ctx, marker)
	if err != nil {
		return trunc, "", err
	}
	last := ""
	for _, t := range touches {
		hash := *t.Key
		hash = strings.TrimPrefix(hash, "touch/")
		done, err := s.st.IsDone(ctx, hash)
		if err != nil {
			log.WithError(err).Infof("Failed get done status for hash=%v", hash)
		}
		log.Infof("Checking hash=%v touch=%v done=%v", hash, t.LastModified, done)
		if (done && t.LastModified.Before(time.Now().Add(-time.Duration(s.doneExpire)*time.Hour))) ||
			(!done && t.LastModified.Before(time.Now().Add(-time.Duration(s.partialExpire)*time.Hour))) {
			log.Infof("Adding torrent to clean queue hash=%v touch=%v done=%v ", hash, t.LastModified, done)
			ch <- t
		}
		last = *t.Key
	}

	return trunc, last, nil
}
