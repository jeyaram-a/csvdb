package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestFitlererWithFilters(t *testing.T) {
	filters := []ColumnFilter{
		{
			0, func(s string) bool {
				return s != "a1"
			},
		},
		{
			1, func(s string) bool {
				return s == "b1"
			},
		},
	}

	filterer := Filterer{
		filters,
		make(chan []string),
	}

	outChan := make(chan []string)

	input := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	go filterer.filter(outChan)

	filtered := make([][]string, 2)
	wait := make(chan string)
	go func() {
		i := 0
		for o := range outChan {
			fmt.Println(o)
			filtered[i] = o
			i += 1
		}
		wait <- "done"
	}()

	for _, in := range input {
		filterer.inChan <- in
	}

	close(filterer.inChan)

	expected := [][]string{
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	<-wait

	if !reflect.DeepEqual(filtered, expected) {
		t.Errorf("filtering not as expected")
	}

}

func TestFitlererWithoutFilters(t *testing.T) {
	filters := []ColumnFilter{}

	filterer := Filterer{
		filters,
		make(chan []string),
	}

	outChan := make(chan []string)

	input := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	go filterer.filter(outChan)

	filtered := make([][]string, 3)
	wait := make(chan string)
	go func() {
		i := 0
		for o := range outChan {
			fmt.Println(o)
			filtered[i] = o
			i += 1
		}
		wait <- "done"
	}()

	for _, in := range input {
		filterer.inChan <- in
	}

	close(filterer.inChan)

	expected := [][]string{
		{"a1", "b1", "c1"},
		{"a2", "b1", "c2"},
		{"a3", "b1", "c3"},
	}

	<-wait

	if !reflect.DeepEqual(filtered, expected) {
		t.Errorf("filtering not as expected")
	}

}
