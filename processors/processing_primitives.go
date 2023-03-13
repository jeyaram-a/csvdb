package processors

type ProcessingNode interface {
	Prev() ProcessingNode
	Channel() chan []string

	SetPrev(ProcessingNode)

	Process()
}
