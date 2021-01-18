package main

import (
	"net/http"
	"time"

	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
	s "github.com/webtor-io/torrent-cache-cleaner/services"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	cs.RegisterS3ClientFlags(app)
	s.RegisterS3StorageFlags(app)
	s.RegisterCleanerFlags(app)

	app.Action = run
}

func run(c *cli.Context) error {
	// Setting S3 Client
	s3cl := cs.NewS3Client(c, &http.Client{
		Timeout: time.Second * 60,
	})

	// Setting S3 Storage
	s3st := s.NewS3Storage(c, s3cl)

	// Setting Cleaner
	cl := s.NewCleaner(c, s3st)

	// Clean!
	return cl.Clean()
}
