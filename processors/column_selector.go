package processors

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type ColumnSelector struct {
	prevNode ProcessingNode
	all      bool
	cols     []int
	channel   chan []string
}

func (colSelector *ColumnSelector) Process() {
	defer close(colSelector.Channel())
	for row := range colSelector.Prev().Channel() {
		log.Debug("ColSelector ", row)
		if colSelector.all {
			colSelector.Channel() <- row
		} else {
			newRow := make([]string, len(colSelector.cols))
			i := 0
			for _, index := range colSelector.cols {
				newRow[i] = row[index]
				i += 1
			}
			colSelector.Channel() <- newRow
		}
	}

}

func (colSelector *ColumnSelector) Prev() ProcessingNode {
	return colSelector.prevNode
}

func (colSelector *ColumnSelector) Channel() chan []string {
	return colSelector.channel
}

func (colSelector *ColumnSelector) SetPrev(node ProcessingNode) {
	colSelector.prevNode = node
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
		nil,
		all,
		colsToBeSelected,
		make(chan []string),
	}, nil
}
