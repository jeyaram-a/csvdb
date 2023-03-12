package processors

import (
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestResultContainerSortingAsc(t *testing.T) {
	input := [][]string{
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
	source := NewTestSource(input)

	orderer.SetPrev(source)
	sink := NewListSink()

	go source.Process()
	go orderer.Process()
	go sink.ConsumeFrom(orderer)

	logrus.Info("GG")
	<-sink.Done()

	ordered := sink.(*ListSink).Container

	expected := [][]string{
		{"a3", "1", "c3"},
		{"a2", "2", "c2"},
		{"a1", "3", "c1"},
	}

	if !reflect.DeepEqual(ordered, expected) {
		t.Errorf("Sorting not working as expected")
	}

}

func TestResultContainerSortingDesc(t *testing.T) {
	input := [][]string{
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
	source := NewTestSource(input)
	orderer.SetPrev(source)

	go source.Process()
	go orderer.Process()

	sink := NewListSink()
	sink.ConsumeFrom(orderer)
	<-sink.Done()

	ordered := sink.(*ListSink).Container

	expected := [][]string{
		{"a3", "3", "c3"},
		{"a2", "2", "c2"},
		{"a1", "1", "c1"},
	}

	if !reflect.DeepEqual(ordered, expected) {
		t.Errorf("Sorting not working as expected")
	}

}
