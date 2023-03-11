package main

import (
	"reflect"
	"testing"
)

func TestResultContainerSortingAsc(t *testing.T) {
	rows := [][]string{
		{"a1", "3", "c1"},
		{"a2", "2", "c2"},
		{"a3", "1", "c3"},
	}

	orderings := []Ordering{
		{
			1, true,
		},
	}

	orderer := NewOrderer(orderings)

	go func() {
		for _, s := range rows {
			orderer.inChan <- s
		}
		close(orderer.inChan)
	}()

	actual := make([]string, 0)
	sink := NewListSink()
	go orderer.order(sink.sinkChannel())
	sink.print()
	for _, row := range sink.(*ListSink).container {
		actual = append(actual, row[1])
	}

	expected := []string{"1", "2", "3"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Sorting not working as expected")
	}

}

func TestResultContainerSortingDesc(t *testing.T) {
	rows := [][]string{
		{"a1", "1", "c1"},
		{"a2", "2", "c2"},
		{"a3", "3", "c3"},
	}

	orderings := []Ordering{
		{
			1, false,
		},
	}

	orderer := NewOrderer(orderings)

	go func() {
		for _, s := range rows {
			orderer.inChan <- s
		}
		close(orderer.inChan)
	}()

	actual := make([]string, 0)
	sink := NewListSink()
	go orderer.order(sink.sinkChannel())
	sink.print()
	for _, row := range sink.(*ListSink).container {
		actual = append(actual, row[1])
	}

	expected := []string{"3", "2", "1"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Sorting not working as expected")
	}

}
