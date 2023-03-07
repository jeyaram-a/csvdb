package main

import (
	"bufio"
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
		return nil, fmt.Errorf("error in opening file %s ", filepath)
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

type Predicate func([]string) bool

func (predicate Predicate) and(other Predicate) Predicate {
	return func(row []string) bool {
		return predicate(row) && other(row)
	}
}

func (predicate Predicate) or(other Predicate) Predicate {
	return func(row []string) bool {
		return predicate(row) || other(row)
	}
}

func (csvReader CSVReader) getPredicateFromFilter(filter Filter) (Predicate, error) {
	var predicate Predicate
	index, err := csvReader.getColumnIndex(filter.Field)
	if err != nil {
		log.Errorf(err.Error())
	}
	switch op := filter.Op; op {
	case "=":
		{
			predicate = func(row []string) bool {
				return row[index] == filter.Val
			}

		}

	case "!=":
		{
			predicate = func(row []string) bool {
				return row[index] != filter.Val
			}
		}

	case ">":
		{
			predicate = func(row []string) bool {
				return strings.Compare(filter.Val, row[index]) > 0
			}
		}

	case ">=":
		{
			predicate = func(row []string) bool {
				return strings.Compare(filter.Val, row[index]) >= 0
			}
		}

	case "<":
		{
			predicate = func(row []string) bool {
				return strings.Compare(filter.Val, row[index]) < 0
			}
		}

	case "<=":
		{
			predicate = func(row []string) bool {
				return strings.Compare(filter.Val, row[index]) <= 0
			}
		}

	}

	if filter.Other != nil {
		otherFilter := filter.Other
		if otherFilter.Filter == nil {
			return nil, fmt.Errorf("Invalid statement. Missing other filter")
		}
		otherPredicate, err := csvReader.getPredicateFromFilter(*otherFilter.Filter)

		if err != nil {
			return nil, err
		}

		if otherFilter.LogicalOp == nil {
			return nil, fmt.Errorf("Invalid statement. Missing logical Op for multiple filters")
		}

		switch *filter.Other.LogicalOp {
		case AND:
			{
				predicate = predicate.and(otherPredicate)
			}
		case OR:
			{
				predicate = predicate.or(otherPredicate)
			}
		}
	}

	return predicate, nil
}

func (csvReader CSVReader) getFiltersFromStatement(statment SelectStatment) ([]Predicate, error) {
	predicates := make([]Predicate, 0)
	for _, filter := range statment.Filters {
		if filter == nil {
			continue
		}
		predicate, err := csvReader.getPredicateFromFilter(*filter)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, predicate)

	}
	return predicates, nil
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
	filters, err := csvReader.getFiltersFromStatement(statement)
	if err != nil {
		return err
	}
	orderings := csvReader.getOrderingFromStatement(statement)
	columnSelector, err := NewColumnSelector(statement.Fields, csvReader.columnIndexMap)
	if err != nil {
		return err
	}

	filterer := NewFilterer(filters)
	orderer := NewOrderer(getOrderingFunction(orderings))

	go filterer.filter(orderer.inChan)
	go orderer.order(columnSelector.inChan)
	go columnSelector.selectColumn(sink.sinkChannel())

	csvReader.read(colIndicesToBeSelected, filterer)

	return nil
}
