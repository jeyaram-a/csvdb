package main

import log "github.com/sirupsen/logrus"

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
