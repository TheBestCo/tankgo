package event

import "github.com/TheBestCo/tankgo/limit"

type Event interface {
	Type() string
}

// EventHandler interface.
type EventHandler interface {
	Handle(event Event)
}

// Consumer interface.
type Consumer interface {
	RegisterHandler(eventType string, h EventHandler)
}

type InternalEventBus struct {
	ch          chan Event
	handlersMap map[string]EventHandler
	limit       limit.ConcurrentLimit
}

// NewInternalEventBus constructor.
func NewInternalEventBus(ch chan Event, limit limit.ConcurrentLimit) *InternalEventBus {
	return &InternalEventBus{
		ch:          ch,
		handlersMap: make(map[string]EventHandler),
		limit:       limit,
	}
}

// Produce function implementation.
func (i *InternalEventBus) Produce(event Event) error {
	i.ch <- event
	return nil
}

// RegisterHandler function implementation.
func (i *InternalEventBus) RegisterHandler(eventType string, h EventHandler) {
	i.handlersMap[eventType] = h
}

// Close function.
func (i *InternalEventBus) Close() error {
	close(i.ch)
	return nil
}

// Start function.
func (i *InternalEventBus) Start() {
	go i.consume()
}

func (i *InternalEventBus) consume() {
	for e := range i.ch {
		i.limit.Request()
		go i.handleEvent(e)
	}
}

func (i *InternalEventBus) handleEvent(e Event) {
	defer i.limit.Done()

	if h, found := i.handlersMap[e.Type()]; found {
		h.Handle(e)
	}
}
