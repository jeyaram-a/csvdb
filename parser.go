package main

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type Filter struct {
	Field string `@Ident`
	Op    string `@Operators`
	Val   string `(@Number | @Ident)`
}

type ParsedOrder struct {
	Field string  `@Ident`
	Order *string ` (@Order)? `
}

type SelectStatment struct {
	Fields  []string  `"select" @Ident ("," @Ident)*`
	Filters []*Filter `("where" @@ ("," @@)*)?`
	Order   []*ParsedOrder  `("order" "by" @@("," @@)*)?`
}

func NewSelectParser() (*participle.Parser[SelectStatment], error) {
	sqlLexer := lexer.MustSimple([]lexer.SimpleRule{
		{`Keyword`, `(?i)\b(SELECT)\b`},
		{`Order`, `(ASC|DESC|asc|desc)`},
		{`Ident`, `[a-zA-Z_][a-zA-Z0-9_]*`},
		{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`},
		{`String`, `'[^']*'|"[^"]*"`},
		{`Operators`, `<>|!=|<=|>=|[-+*/%,.()=<>]`},
		{"whitespace", `\s+`},
	})
	return participle.Build[SelectStatment](
		participle.Lexer(sqlLexer),
	)

}
