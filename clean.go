package main

import (
	"net/http"
	"time"

	"github.com/urfave/cli"
	s "github.com/webtor-io/cache-keeper/services"
	cs "github.com/webtor-io/common-services"
)

func makeCleanCMD() cli.Command {
	cleanCmd := cli.Command{
		Name:    "clean",
		Aliases: []string{"c"},
		Usage:   "cleans cache",
		Action:  clean,
	}
	configureClean(&cleanCmd)
	return cleanCmd
}

func configureClean(c *cli.Command) {
	c.Flags = cs.RegisterS3ClientFlags(c.Flags)
	c.Flags = s.RegisterS3StorageFlags(c.Flags)
	c.Flags = s.RegisterCleanerFlags(c.Flags)
}

func clean(c *cli.Context) error {
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
