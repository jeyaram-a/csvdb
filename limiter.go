package main

type Limiter struct {
	active bool
	limit  int
	inChan chan []string
}

func NewLimiter(limit int) *Limiter {
	return &Limiter{
		active: limit > 0,
		limit:  limit,
		inChan: make(chan []string),
	}
}

func (limiter *Limiter) take(outChan chan []string) {
	for row := range limiter.inChan {
		if limiter.active && limiter.limit == 0 {
			break
		}
		limiter.limit -= 1
		outChan <- row
	}
	close(outChan)
}
