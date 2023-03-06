package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestResultContainerSorting(t *testing.T) {
	results := [][]string{
		{"a1", "3", "c1"},
		{"a2", "2", "c2"},
		{"a3", "1", "c3"},
	}
	container := ResultContainer{
		result: make([][]string, 0),
		less: func(i, j int, results [][]string) bool {
			return strings.Compare(results[i][1], results[j][1]) < 0
		},
	}

	for _, s := range results {
		container.Push(s)
	}
	actual := make([]string, 0)
	for i := 0; i < len(results); i++ {
		actual = append(actual, container.Pop().([]string)[1])
	}

	expected := []string{"1", "2", "3"}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Sorting not working as expected")
	}

}
