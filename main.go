package main

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(logrus.DebugLevel)
	parser, _ := NewSelectParser()
	ast, _ := parser.ParseString("", "select a where a=a1 AND b=b1, c=c4")
	spew.Dump(*ast)
	reader, _ := NewCSVReader("/home/j/development/csvdb/a.csv")
	cSink := NewConsoleSink()

	reader.Execute(*ast, cSink)
	cSink.print()

}
