package main

import (
	"bufio"
	"csvdb/parser"
	. "csvdb/processors"
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

func (csvReader CSVReader) getColsToBeSelected(statement parser.SelectStatment) map[int]bool {
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

func (csvReader CSVReader) getOrderingFromStatement(statement parser.SelectStatment) []Ordering {
	orderings := make([]Ordering, 0)

	for _, order := range statement.Order {
		colIndex, err := csvReader.getColumnIndex(order.Field)
		if err != nil {
			log.Errorf("column %s doesn't exist", order.Field)
			continue
		}
		order := Ordering{
			Col: colIndex,
			Asc: order.Order == nil || *order.Order == "asc" || *order.Order == "desc",
		}
		orderings = append(orderings, order)
	}
	return orderings
}

func (csvReader CSVReader) read(colIndicesToBeSelected map[int]bool, node ProcessingNode) {
	for {
		row := csvReader.lineProvider.readLine()
		if row == "" {
			break
		}
		log.Debug("reading ", row)
		splittedRows := strings.Split(row, ",")
		node.Channel() <- splittedRows
	}
	close(node.Channel())
}

type InputProvider struct {
	lineProvider LineProvider
	outC         chan []string
}

func (inputProvider *InputProvider) Prev() ProcessingNode {
	return nil
}

func (inputProvider *InputProvider) Channel() chan []string {
	return inputProvider.outC
}

func (inputProvider *InputProvider) SetPrev(node ProcessingNode) {
}

func (inputProvider *InputProvider) Process() {
	for {
		row := inputProvider.lineProvider.readLine()
		if row == "" {
			break
		}
		log.Debug("reading ", row)
		splittedRows := strings.Split(row, ",")
		inputProvider.Channel() <- splittedRows
	}
	close(inputProvider.Channel())
}

func buildProcessingChain(sink Sink, nodes ...ProcessingNode) ProcessingNode {

	if len(nodes) == 0 {
		return nil
	}

	for i := 1; i < len(nodes); i += 1 {
		nodes[i].SetPrev(nodes[i-1])
		go nodes[i].Process()
	}

	go sink.ConsumeFrom(nodes[len(nodes)-1])
	// Returning head
	return nodes[0]
}

func (csvReader CSVReader) Execute(statement parser.SelectStatment, sink Sink) error {
	// colIndicesToBeSelected := csvReader.getColsToBeSelected(statement)
	filters, err := GetFiltersFromStatement(statement, csvReader.columnIndexMap)
	if err != nil {
		return err
	}
	orderings := csvReader.getOrderingFromStatement(statement)
	columnSelector, err := NewColumnSelector(statement.Fields, csvReader.columnIndexMap)
	if err != nil {
		return err
	}
	inputProvider := &InputProvider{
		csvReader.lineProvider,
		make(chan []string),
	}
	filterer := NewFilterer(filters)
	orderer := NewOrderer(orderings)
	limit := 0
	if statement.Limit != nil {
		limit = *statement.Limit
	}
	limiter := NewLimiter(limit)

	headNode := buildProcessingChain(sink, inputProvider, filterer, orderer, columnSelector, limiter)

	if headNode == nil {
		fmt.Errorf("error in creating processing chain. Pass atleast one processing node")
	}
	headNode.Process()
	<-sink.Done()
	return nil
}
