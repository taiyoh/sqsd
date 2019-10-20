package sqsd

func (c *MessageConsumer) ChangeInvoker(invoker WorkerInvoker) {
	c.invoker = invoker
}
