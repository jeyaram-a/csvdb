package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var CLI struct {
	Path  string `arg:"" short:"p"`
	Query string `arg:"" short:"q"`
}

func main() {

	log.SetLevel(logrus.InfoLevel)

	ctx := kong.Parse(&CLI,
		kong.Name("csvdb"),
		kong.Description("An sql interface for csv"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
			Summary: false,
		}))

	switch ctx.Command() {
	case "<path> <query>":
		parser, _ := NewSelectParser()
		ast, err := parser.ParseString("", CLI.Query)
		if err != nil {
			log.Error("error in parsing select statement ", err.Error())
			os.Exit(1)
		}
		reader, err := NewCSVReader(CLI.Path)
		if err != nil {
			log.Error(err.Error())
			os.Exit(1)
		}
		cSink := NewConsoleSink()
		reader.Execute(*ast, cSink)
		cSink.print()
	}

}
