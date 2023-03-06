package main

import (
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(logrus.DebugLevel)
	parser, _ := NewSelectParser()
	ast, _ := parser.ParseString("", "select c,b where a!=a1")
	reader, _ := NewCSVReader("/home/j/development/csvdb/a.csv")

	cSink := NewConsoleSink()

	reader.Execute(*ast, cSink)
	cSink.print()

}
