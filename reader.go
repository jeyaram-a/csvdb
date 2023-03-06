package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type LineProvider interface {
	readLine() string
}

type BufferedDiskLineProvider struct {
	fileScanner bufio.Scanner
}

func NewBufferedDiskLineProvider(path string) (LineProvider, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	return &BufferedDiskLineProvider{
		fileScanner: *fileScanner,
	}, nil
}

func (provider *BufferedDiskLineProvider) readLine() string {
	if !provider.fileScanner.Scan() {
		return ""
	}

	return provider.fileScanner.Text()
}

type CSVReader struct {
	lineProvider   LineProvider
	columnIndexMap map[string]int
}

func NewCSVReader(filepath string) (*CSVReader, error) {
	lineProvider, err := NewBufferedDiskLineProvider(filepath)
	if err != nil {
		return nil, errors.New("error in opening file")
	}
	colNameIndexMap, err := readColumns(lineProvider)

	if err != nil {
		return nil, err
	}

	return &CSVReader{
		lineProvider:   lineProvider,
		columnIndexMap: colNameIndexMap,
	}, nil
}

func readColumns(lineProvider LineProvider) (map[string]int, error) {
	first_row := lineProvider.readLine()
	cols := strings.Split(string(first_row), ",")

	colNameIndexMap := make(map[string]int)

	for i, c := range cols {
		colNameIndexMap[strings.TrimSpace(c)] = i
	}
	return colNameIndexMap, nil
}

func (csvReader CSVReader) getColsToBeSelected(statement SelectStatment) map[int]bool {
	indices := make(map[int]bool)
	for _, selected_col := range statement.Fields {
		index := csvReader.columnIndexMap[selected_col]
		indices[index] = true
	}
	return indices
}

func (csvReader CSVReader) getColumnIndex(col string) (int, error) {
	index, ok := csvReader.columnIndexMap[col]
	if !ok {
		return 0, fmt.Errorf("column %s doesn't exist", col)
	}

	return index, nil
}

func (csvReader CSVReader) getFiltersFromStatement(statment SelectStatment) []ColumnFilter {
	columnFilterers := make([]ColumnFilter, 0)
	for _, filter := range statment.Filters {
		var predicate func(string) bool
		switch op := filter.Op; op {
		case "=":
			{
				predicate = func(val string) bool {
					return val == filter.Val
				}

			}

		case "!=":
			{
				predicate = func(val string) bool {
					return val != filter.Val
				}
			}

		case ">":
			{
				predicate = func(s string) bool {
					return strings.Compare(filter.Val, s) > 0
				}
			}

		case ">=":
			{
				predicate = func(s string) bool {
					return strings.Compare(filter.Val, s) >= 0
				}
			}

		case "<":
			{
				predicate = func(s string) bool {
					return strings.Compare(filter.Val, s) < 0
				}
			}

		case "<=":
			{
				predicate = func(s string) bool {
					return strings.Compare(filter.Val, s) <= 0
				}
			}

		}

		index, err := csvReader.getColumnIndex(filter.Field)
		if err != nil {
			log.Errorf(err.Error())
			continue
		}
		columnFilterers = append(columnFilterers, ColumnFilter{
			index,
			predicate,
		})

	}
	return columnFilterers
}

func (csvReader CSVReader) getOrderingFromStatement(statement SelectStatment) []Ordering {
	orderings := make([]Ordering, 0)

	for _, order := range statement.Order {
		colIndex, err := csvReader.getColumnIndex(order.Field)
		if err != nil {
			log.Errorf("column %s doesn't exist", order.Field)
			continue
		}
		order := Ordering{
			col: colIndex,
			asc: order.Order == nil || *order.Order == "asc" || *order.Order == "desc",
		}
		orderings = append(orderings, order)
	}
	return orderings
}

func (csvReader CSVReader) read(colIndicesToBeSelected map[int]bool, filterer *Filterer) {
	for {
		row := csvReader.lineProvider.readLine()
		if row == "" {
			break
		}
		log.Debug("reading ", row)
		splittedRows := strings.Split(row, ",")
		filterer.inChan <- splittedRows
	}
	close(filterer.inChan)
}

func getOrderingFunction(orderings []Ordering) *func(int, int, [][]string) bool {
	defaultComparator := func(i, j int, results [][]string) bool {
		return i < j
	}
	if len(orderings) == 0 {
		return &defaultComparator
	}
	// TODO
	return &defaultComparator
}

func (csvReader CSVReader) Execute(statement SelectStatment, sink Sink) error {
	colIndicesToBeSelected := csvReader.getColsToBeSelected(statement)
	filters := csvReader.getFiltersFromStatement(statement)
	orderings := csvReader.getOrderingFromStatement(statement)

	filterer := NewFilterer(filters)
	orderer := NewOrderer(getOrderingFunction(orderings))

	go filterer.filter(orderer.inChan)
	go orderer.order(sink.sinkChannel())

	csvReader.read(colIndicesToBeSelected, filterer)

	return nil
}
