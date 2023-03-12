package processors

type Limiter struct {
	prevNode ProcessingNode
	active   bool
	limit    int
	// TODO remove name
	inChan chan []string
}

func NewLimiter(limit int) *Limiter {
	return &Limiter{
		active: limit > 0,
		limit:  limit,
		inChan: make(chan []string),
	}
}

func (limiter *Limiter) Prev() ProcessingNode {
	return limiter.prevNode
}

func (limiter *Limiter) Channel() chan []string {
	return limiter.inChan
}

func (limiter *Limiter) SetPrev(node ProcessingNode) {
	limiter.prevNode = node
}

func (limiter *Limiter) Process() {
	defer close(limiter.Channel())

	for row := range limiter.Prev().Channel() {
		if limiter.active && limiter.limit == 0 {
			break
		}
		limiter.limit -= 1
		limiter.Channel() <- row
	}
}
