package main

import "fmt"

type ColumnSelector struct {
	all    bool
	cols   []int
	inChan chan []string
}

func (colSelector *ColumnSelector) selectColumn(outChan chan []string) {

	for row := range colSelector.inChan {
		if colSelector.all {
			outChan <- row
		} else {
			newRow := make([]string, len(colSelector.cols))
			i := 0
			for _, index := range colSelector.cols {
				newRow[i] = row[index]
				i += 1
			}
			outChan <- newRow
		}
	}

	close(outChan)
}

func NewColumnSelector(cols []string, colIndexMap map[string]int) (*ColumnSelector, error) {
	colsToBeSelected := make([]int, 0)
	all := false
	for _, col := range cols {
		if col == "*" {
			all = true
			break
		}
		index, ok := colIndexMap[col]
		if !ok {
			return nil, fmt.Errorf("Not a valid col %s in selection", col)
		}
		colsToBeSelected = append(colsToBeSelected, index)
	}
	return &ColumnSelector{
		all,
		colsToBeSelected,
		make(chan []string),
	}, nil
}
