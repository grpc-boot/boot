package container

type Handler func(value interface{}) (handled bool)

type Chain struct {
	items []Handler
}

func NewChain(handlers ...Handler) *Chain {
	chain := &Chain{
		items: make([]Handler, len(handlers)),
	}

	for index := 0; index < len(handlers); index++ {
		chain.items[index] = handlers[index]
	}

	return chain
}

func (c *Chain) Use(handler Handler) {
	c.items = append(c.items, handler)
}

func (c *Chain) Next(value interface{}) {
	if len(c.items) < 1 {
		return
	}

	for index := 0; index < len(c.items); index++ {
		if c.items[index](value) {
			return
		}
	}
}
