package sqsd

import (
	"context"
	"sync/atomic"
)

// this messageBroker wraps channel.
// why use this ?
// panic occurres when sending Message to closed channel.
// `isClosed` blocks to send Message to channel which is closed.
type messageBroker struct {
	ch       chan Message
	isClosed uint32
}

func newMessageBroker(ctx context.Context, capacity int) *messageBroker {
	b := &messageBroker{ch: make(chan Message, capacity)}
	go b.watch(ctx)
	return b
}

func (b *messageBroker) watch(ctx context.Context) {
	<-ctx.Done()
	atomic.StoreUint32(&b.isClosed, 1)
	close(b.ch)
}

func (b *messageBroker) Append(msg Message) {
	if atomic.LoadUint32(&b.isClosed) == 0 {
		b.ch <- msg
	}
}

func (b *messageBroker) Receive() <-chan Message {
	return b.ch
}
