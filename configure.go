package main

import (
	"github.com/urfave/cli"
)

func configure(app *cli.App) {
	app.Commands = []cli.Command{
		makeCleanCMD(),
	}
}
