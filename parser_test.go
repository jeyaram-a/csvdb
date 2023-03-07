package main

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func Test(t *testing.T) {
	parser, _ := NewSelectParser()
	ast, _ := parser.ParseString("", "select a where a=a1 OR b=b2")
	spew.Dump(*ast)
	reader, _ := NewCSVReader("/home/j/development/csvdb/a.csv")
	cSink := NewConsoleSink()

	reader.Execute(*ast, cSink)
	cSink.print()
}
