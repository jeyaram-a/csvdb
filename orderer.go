package main

import (
	log "github.com/sirupsen/logrus"
)

type Ordering struct {
	col int
	asc bool
}

type ResultContainer struct {
	result [][]string
	less   func(i, j int, results [][]string) bool
}

func (container ResultContainer) Len() int { return len(container.result) }

func (container ResultContainer) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return container.less(i, j, container.result)
}

func (container ResultContainer) Swap(i, j int) {
	container.result[i], container.result[j] = container.result[j], container.result[i]
}

func (container *ResultContainer) Push(x any) {
	item := x.([]string)
	container.result = append(container.result, item)
}

func (container *ResultContainer) Pop() any {
	old := container.result
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	container.result = old[0 : n-1]
	return item
}

type Orderer struct {
	inChan    chan []string
	container ResultContainer
}

func NewOrderer(less *func(i, j int, results [][]string) bool) *Orderer {
	return &Orderer{
		inChan: make(chan []string),
		container: ResultContainer{
			result: make([][]string, 0),
			less:   *less,
		},
	}
}

func (orderer *Orderer) order(outChan chan []string) {
	for row := range orderer.inChan {
		log.Debug("Orderer ", row)
		orderer.container.Push(row)
	}

	for len(orderer.container.result) > 0 {
		outChan <- orderer.container.Pop().([]string)
	}

	close(outChan)
}
