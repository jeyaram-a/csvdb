package main

import (
	"sort"
	"strings"
)

type Ordering struct {
	col int
	asc bool
}

type Less = func(int, int) bool

type Orderer struct {
	inChan chan []string
	rows   [][]string
	less   Less
}

type LessFunc = func(int, int) bool

func (orderer *Orderer) setLessFuncFromOrderings(orderings []Ordering) {
	defaultComparator := func(i, j int) bool {
		return i < j
	}
	if len(orderings) == 0 {
		orderer.less = defaultComparator
		return
	}

	var customComparator = func(i, j int) bool {
		row1 := orderer.rows[i]
		row2 := orderer.rows[j]
		for _, ordering := range orderings {
			comp := strings.Compare(row1[ordering.col], row2[ordering.col])
			if comp == 0 {
				continue
			}
			if ordering.asc {
				return comp < 0
			} else {
				return comp > 0
			}
		}
		return false
	}

	orderer.less = customComparator
}

func NewOrderer(orderings []Ordering) *Orderer {
	orderer := &Orderer{
		inChan: make(chan []string),
		rows:   make([][]string, 0),
	}
	orderer.setLessFuncFromOrderings(orderings)
	return orderer
}

func (orderer *Orderer) order(outChan chan []string) {
	for row := range orderer.inChan {
		orderer.rows = append(orderer.rows, row)
	}

	sort.SliceStable(orderer.rows, orderer.less)
	for _, row := range orderer.rows {
		outChan <- row
	}

	close(outChan)
}
