package main

import (
	log "github.com/sirupsen/logrus"
)

type Sink interface {
	sinkChannel() chan []string
	print()
}

func NewConsoleSink() Sink {
	return &ConsoleSink{
		sinkChan: make(chan []string),
	}
}

type ConsoleSink struct {
	sinkChan chan []string
}

func (csink *ConsoleSink) sinkChannel() chan []string {
	return csink.sinkChan
}

func (csink *ConsoleSink) print() {
	for result := range csink.sinkChannel() {
		log.Info(result)
	}
}

type ListSink struct {
	sinkChan  chan []string
	container [][]string
}

func NewListSink() Sink {
	return &ListSink{
		sinkChan:  make(chan []string),
		container: make([][]string, 0),
	}
}

func (lsink *ListSink) sinkChannel() chan []string {
	return lsink.sinkChan
}

func (lsink *ListSink) print() {
	for result := range lsink.sinkChannel() {
		lsink.container = append(lsink.container, result)
	}
}
