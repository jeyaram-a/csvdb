package processors

import (
	log "github.com/sirupsen/logrus"
)

type Sink interface {
	ConsumeFrom(ProcessingNode)
	Done() chan interface{}
}

func NewConsoleSink() Sink {
	return &ConsoleSink{
		sinkChan: make(chan []string),
		doneChan: make(chan interface{}),
	}
}

type ConsoleSink struct {
	sinkChan chan []string
	doneChan chan interface{}
}

func (csink *ConsoleSink) ConsumeFrom(node ProcessingNode) {
	for result := range node.Channel() {
		log.Info(result)
	}
	close(csink.Done())
}

func (csink *ConsoleSink) Done() chan interface{} {
	return csink.doneChan
}

type ListSink struct {
	SinkChan  chan []string
	Container [][]string
	DoneChan  chan interface{}
}

func NewListSink() Sink {
	return &ListSink{
		SinkChan:  make(chan []string),
		Container: make([][]string, 0),
		DoneChan:  make(chan interface{}),
	}
}

func (lsink *ListSink) ConsumeFrom(node ProcessingNode) {
	for result := range node.Channel() {
		lsink.Container = append(lsink.Container, result)
	}
	close(lsink.Done())
}

func (lsink *ListSink) Done() chan interface{} {
	return lsink.DoneChan
}
