package processors

import (
	"reflect"
	"testing"
)

type TestSource struct {
	OutChan chan []string
	Rows    [][]string
}

func (source *TestSource) Prev() ProcessingNode {
	return nil
}

func (source *TestSource) Channel() chan []string {
	return source.OutChan
}

func (source *TestSource) SetPrev(node ProcessingNode) {

}

func (source *TestSource) Process() {
	for _, row := range source.Rows {
		source.OutChan <- row
	}
	close(source.OutChan)
}

func NewTestSource(in [][]string) *TestSource {
	return &TestSource{
		OutChan: make(chan []string),
		Rows:    in,
	}
}

func TestFitlererWithFilters(t *testing.T) {
	filters := []Predicate{
		func(row []string) bool {
			return row[0] != "a1"
		},
		func(row []string) bool {
			return row[1] == "b1"
		},
	}

	filterer := NewFilterer(filters)
	input := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	source := NewTestSource(input)
	filterer.SetPrev(source)

	go source.Process()
	go filterer.Process()

	sink := NewListSink()
	sink.ConsumeFrom(filterer)
	<-sink.Done()

	filtered := sink.(*ListSink).Container
	expected := [][]string{
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	if !reflect.DeepEqual(filtered, expected) {
		t.Errorf("filtering not as expected")
	}

}

func TestFitlererWithoutFilters(t *testing.T) {
	filters := []Predicate{}

	filterer := NewFilterer(filters)

	input := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	source := NewTestSource(input)
	filterer.SetPrev(source)
	go source.Process()
	go filterer.Process()

	sink := NewListSink()
	sink.ConsumeFrom(filterer)
	<-sink.Done()

	filtered := sink.(*ListSink).Container
	expected := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	if !reflect.DeepEqual(filtered, expected) {
		t.Errorf("filtering not as expected")
	}

}
