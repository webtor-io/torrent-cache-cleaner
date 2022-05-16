package services

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/urfave/cli"

	log "github.com/sirupsen/logrus"
)

const (
	TIMEOUT_HOURS                 = "timeout-hours"
	DONE_TORRENTS_EXPIRE_HOURS    = "done-torrents-expire-hours"
	PARTIAL_TORRENTS_EXPIRE_HOURS = "partial-torrents-expire-hours"
	TRANSCODED_EXPIRE_HOURS       = "transcoded-expire-hours"
	MAX_SIZE                      = "max-size"
	HASH                          = "hash"
	FORCE                         = "force"
	CONCURRENCY                   = "concurrency"
)

var (
	shaExp = regexp.MustCompile("[0-9a-f]{40}")
)

func RegisterCleanerFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.IntFlag{
			Name:   TIMEOUT_HOURS,
			Usage:  "Timeout (in hours)",
			Value:  1,
			EnvVar: "TIMEOUT_HOURS",
		},
		cli.IntFlag{
			Name:   TRANSCODED_EXPIRE_HOURS,
			Usage:  "Expiration period for transcoded content (in hours)",
			Value:  96,
			EnvVar: "TRANSCODED_EXPIRE_HOURS",
		},
		cli.IntFlag{
			Name:   DONE_TORRENTS_EXPIRE_HOURS,
			Usage:  "Expiration period for completly downloaded torrents (in hours)",
			Value:  48,
			EnvVar: "DONE_TORRENTS_EXPIRE_HOURS",
		},
		cli.IntFlag{
			Name:   PARTIAL_TORRENTS_EXPIRE_HOURS,
			Usage:  "Expiration period for partialy downloaded torrents (in hours)",
			Value:  12,
			EnvVar: "PARTIAL_TORRENTS_EXPIRE_HOURS",
		},
		cli.StringFlag{
			Name:   MAX_SIZE,
			Usage:  "Maximum cache size",
			Value:  "3T",
			EnvVar: "MAX_SIZE",
		},
		cli.StringFlag{
			Name:  HASH,
			Usage: "Check specific hash",
		},
		cli.BoolFlag{
			Name:  FORCE,
			Usage: "Forces clearing",
		},
		cli.IntFlag{
			Name:   CONCURRENCY,
			Usage:  "Concurrency",
			Value:  64,
			EnvVar: "CONCURRENCY",
		},
	)
}

type Resource struct {
	Hash       string
	Done       bool
	Transcoded bool
	Size       int
	TouchedAt  time.Time
}

type Cleaner struct {
	st               *S3Storage
	doneExpire       time.Duration
	partialExpire    time.Duration
	transcodedExpire time.Duration
	hash             string
	force            bool
	maxSize          int
	concurrency      int
	timeout          time.Duration
}

func NewCleaner(c *cli.Context, st *S3Storage) *Cleaner {
	maxSize, err := bytefmt.ToBytes(c.String(MAX_SIZE))
	if err != nil {
		log.WithError(err).Fatal("failed to parse max size")
	}
	return &Cleaner{
		st:               st,
		doneExpire:       time.Duration(c.Int(DONE_TORRENTS_EXPIRE_HOURS)) * time.Hour,
		partialExpire:    time.Duration(c.Int(PARTIAL_TORRENTS_EXPIRE_HOURS)) * time.Hour,
		transcodedExpire: time.Duration(c.Int(TRANSCODED_EXPIRE_HOURS)) * time.Hour,
		timeout:          time.Duration(c.Int(TIMEOUT_HOURS)) * time.Hour,
		maxSize:          int(maxSize),
		hash:             c.String(HASH),
		force:            c.Bool(FORCE),
		concurrency:      c.Int(CONCURRENCY),
	}
}

func (s *Cleaner) Clean() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	err := make(chan error)
	go func() {
		st := s.getStats(ctx, "")
		for _, r := range s.getStats(ctx, "touch/") {
			st, _ = s.appendTo(st, r)
		}
		m := s.mark(st)
		s.sweep(ctx, m)
		err <- nil
	}()
	select {
	case <-err:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Cleaner) sweep(ctx context.Context, rr []Resource) {
	ch := make(chan Resource)
	tc := 10
	if len(rr) < tc {
		tc = len(rr)
	}

	total := 0

	for _, r := range rr {
		total += r.Size
	}

	var size uint64
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(tc)
	for i := 0; i < tc; i++ {
		go func(i int) {
			defer wg.Done()
			for r := range ch {
				k := r.Hash
				// log.Infof("sweep start %+v", r)
				n, err := s.st.DeleteTorrentData(ctx, k, s.concurrency)
				if ctx.Err() != nil {
					break
				}
				if err != nil {
					log.WithError(err).Infof("failed to sweep %v", r)
				} else {
					atomic.AddUint64(&size, uint64(r.Size))
					log.Infof("sweep done %+v pieces=%v elapsed time=%v sweeped size=%.2f/%.2fG", r, n, time.Since(start), float64(size)/1024/1024/1024, float64(total)/1024/1024/1024)
				}
			}
		}(i)
	}
	for _, r := range rr {
		if ctx.Err() != nil {
			break
		}
		ch <- r
	}
	close(ch)
	wg.Wait()
}

func (s *Cleaner) mark(rr []Resource) []Resource {
	sort.Slice(rr, func(i, j int) bool {
		return rr[i].TouchedAt.Unix() < rr[j].TouchedAt.Unix()
	})
	size := 0
	mm := []Resource{}
	for _, r := range rr {
		if (r.Done && !r.Transcoded && r.TouchedAt.Before(time.Now().Add(-s.doneExpire))) ||
			(!r.Done && r.TouchedAt.Before(time.Now().Add(-s.partialExpire))) ||
			(r.Transcoded && r.TouchedAt.Before(time.Now().Add(-s.transcodedExpire))) {
			mm, _ = s.appendTo(mm, r)
		}
		size += r.Size
	}

	var ok bool
	for _, r := range rr {
		if size <= s.maxSize {
			break
		}
		if r.Transcoded {
			continue
		}
		if mm, ok = s.appendTo(mm, r); ok {
			size -= r.Size
		}
	}
	for _, r := range rr {
		if size <= s.maxSize {
			break
		}
		if mm, ok = s.appendTo(mm, r); ok {
			size -= r.Size
		}
	}
	total := 0
	for _, m := range mm {
		total += m.Size
	}
	log.Infof("marked count=%v size=%2.fG", len(mm), float64(total)/1024/1024/1024)
	return mm
}

func (s *Cleaner) appendTo(mm []Resource, r Resource) ([]Resource, bool) {
	found := false
	for _, m := range mm {
		if m.Hash == r.Hash {
			found = true
			break
		}
	}
	if !found {
		return append(mm, r), true
	} else {
		return mm, false
	}
}

func (s *Cleaner) getStats(ctx context.Context, base string) []Resource {
	ch := make(chan Resource)
	letters := "0123456789abcdef"
	// letters := "01
	prefixes := []string{}
	prefixesCh := make(chan string)
	for _, a := range []byte(letters) {
		for _, b := range []byte(letters) {
			prefixes = append(prefixes, string(a)+string(b))
		}
	}
	go func() {
		for _, p := range prefixes {
			if ctx.Err() != nil {
				break
			}
			prefixesCh <- p
		}
		close(prefixesCh)
	}()
	tc := s.concurrency
	if len(prefixes) < tc {
		tc = len(prefixes)
	}
	var wg sync.WaitGroup
	wg.Add(tc)
	for i := 0; i < tc; i++ {
		go func(i int) {
			defer wg.Done()
			for pr := range prefixesCh {
				// log.Infof("starts prefix=%v worker=%v", pr, i)
				err := s.getStatsWithPrefix(ctx, base, pr, ch)
				if ctx.Err() != nil {
					break
				}
				if err != nil {
					log.WithError(err).Error("got error")
				}
			}
		}(i)
	}
	rr := []Resource{}
	size := 0
	go func() {
		for r := range ch {
			log.Infof("got stats %+v", r)
			rr = append(rr, r)
			size += r.Size
		}
	}()
	wg.Wait()
	log.Infof("total count=%v size=%.2fG target size=%.2fG", len(rr), float64(size)/1024/1024/1024, float64(s.maxSize)/1024/1024/1024)
	return rr
}

func (s *Cleaner) getStatsWithPrefix(ctx context.Context, base string, prefix string, ch chan Resource) error {
	last := ""
	mlast := ""
	size := 0
	for {
		t, l, ml, si, err := s.getStatsChunk(ctx, ch, last, mlast, size, base, prefix)
		if err != nil {
			return err
		}
		if !t {
			break
		}
		size = si
		last = l
		mlast = ml
	}
	return nil
}

func (s *Cleaner) getStatsChunk(ctx context.Context, ch chan Resource, marker string, ml string, size int, base string, prefix string) (bool, string, string, int, error) {
	objs, trunc, err := s.st.GetAllObjects(ctx, base+prefix, marker)
	if err != nil {
		return trunc, "", "", 0, err
	}
	last := ""
	for _, o := range objs {
		key := strings.TrimPrefix(*o.Key, base)
		last = key
		i := shaExp.FindIndex([]byte(key))
		if i == nil || i[0] != 0 {
			continue
		}
		m := string(shaExp.Find([]byte(key)))
		size += int(*o.Size)
		if ml == m {
			continue
		}
		ml = m

		touch, err := s.st.GetTouch(ctx, ml)
		if err != nil {
			return trunc, "", "", 0, err
		}
		done, err := s.st.IsDone(ctx, ml)
		if err != nil {
			return trunc, "", "", 0, err
		}
		transcoded, err := s.st.IsTranscoded(ctx, ml)
		if err != nil {
			return trunc, "", "", 0, err
		}
		touchedAt := time.Time{}
		if touch != nil {
			touchedAt = *touch.LastModified
		}
		ch <- Resource{
			Hash:       ml,
			Size:       size,
			Done:       done,
			Transcoded: transcoded,
			TouchedAt:  touchedAt,
		}
		size = 0
	}

	return trunc, last, ml, size, nil
}
