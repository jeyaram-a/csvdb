package parser

import (
	"testing"
)

func Test(t *testing.T) {
	parser, _ := NewSelectParser()
	_, err := parser.ParseString("", "select a where a=a1 OR b=b2")
	if err != nil {
		t.Error("Parsing failed")
	}
}
