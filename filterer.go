package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type ColumnFilter struct {
	col       int
	evaluator func(string) bool
}

type Filterer struct {
	filters []ColumnFilter
	inChan  chan []string
}

func NewFilterer(filters []ColumnFilter) *Filterer {
	return &Filterer{
		filters: filters,
		inChan:  make(chan []string),
	}

}

func (filterer *Filterer) filter(outChan chan []string) {
	for row := range filterer.inChan {
		log.Debug("Filterer ", row)
		passed := true
		for _, filter := range filterer.filters {
			if !filter.evaluator(row[filter.col]) {
				passed = false
				break
			}
		}
		if passed {
			fmt.Println("passed ", row)
			outChan <- row
		}
	}
	close(outChan)
}
