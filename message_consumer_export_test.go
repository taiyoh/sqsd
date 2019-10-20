package sqsd

func (c *MessageConsumer) ChangeHandler(h Handler) {
	c.handler = h
}
