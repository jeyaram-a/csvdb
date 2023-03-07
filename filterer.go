package main

import (
	log "github.com/sirupsen/logrus"
)


const AND = "AND"
const OR = "OR"


type Filterer struct {
	predicates []Predicate
	inChan     chan []string
}

func NewFilterer(predicates []Predicate) *Filterer {
	return &Filterer{
		predicates: predicates,
		inChan:     make(chan []string),
	}
}

func (filterer *Filterer) filter(outChan chan []string) {
	for row := range filterer.inChan {
		log.Debug("Filterer ", row)
		passed := true
		for _, predicate := range filterer.predicates {
			if !predicate(row) {
				passed = false
				break
			}
		}
		if passed {
			outChan <- row
		}
	}
	close(outChan)
}
