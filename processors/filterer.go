package processors

import (
	"fmt"
	"strings"

	"csvdb/parser"

	log "github.com/sirupsen/logrus"
)

const AND = "AND"
const OR = "OR"

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

type Filterer struct {
	prevNode   ProcessingNode
	predicates []Predicate
	channel        chan []string
}

func NewFilterer(predicates []Predicate) *Filterer {
	return &Filterer{
		predicates: predicates,
		channel:        make(chan []string),
	}
}

func (filterer *Filterer) Channel() chan []string {
	return filterer.channel
}

func (filterer *Filterer) Prev() ProcessingNode {
	return filterer.prevNode
}

func (filterer *Filterer) SetPrev(node ProcessingNode) {
	filterer.prevNode = node
}

func (filterer *Filterer) Process() {

	defer close(filterer.Channel())

	for row := range filterer.Prev().Channel() {
		log.Debug("Filterer ", row)
		passed := true
		for _, predicate := range filterer.predicates {
			if !predicate(row) {
				passed = false
				break
			}
		}
		if passed {
			filterer.Channel() <- row
		}
	}
}

func GetFiltersFromStatement(statment parser.SelectStatment, columnIndexMap map[string]int) ([]Predicate, error) {
	predicates := make([]Predicate, 0)
	for _, filter := range statment.Filters {
		if filter == nil {
			continue
		}
		predicate, err := getPredicateFromFilter(*filter, columnIndexMap)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, predicate)

	}
	return predicates, nil
}

func getColumnIndex(col string, columnIndexMap map[string]int) (int, error) {
	index, ok := columnIndexMap[col]
	if !ok {
		return 0, fmt.Errorf("column %s doesn't exist", col)
	}

	return index, nil
}

func getPredicateFromFilter(filter parser.Filter, columnIndexMap map[string]int) (Predicate, error) {
	var predicate Predicate
	index, err := getColumnIndex(filter.Field, columnIndexMap)
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
		otherPredicate, err := getPredicateFromFilter(*otherFilter.Filter, columnIndexMap)

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
