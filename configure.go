package main

import (
	"github.com/urfave/cli"
	s "github.com/webtor-io/torrent-cache-cleaner/services"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	s.RegisterS3ClientFlags(app)
	s.RegisterS3StorageFlags(app)

	app.Action = run
}

func run(c *cli.Context) error {

	// Setting S3 Session
	s3cl := s.NewS3Client(c)

	// Setting S3 Storage
	s3st := s.NewS3Storage(c, s3cl)

	// Setting Cleaner
	cl := s.NewCleaner(s3st)

	// Clean!
	return cl.Clean()
}
