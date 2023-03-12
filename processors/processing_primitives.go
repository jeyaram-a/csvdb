package processors

type ProcessingNode interface {
	Prev() ProcessingNode
	// TODO rename properly
	Channel() chan []string

	SetPrev(ProcessingNode)

	Process()
}
