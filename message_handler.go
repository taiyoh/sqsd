package sqsd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type MessageHandler struct {
	Resource        *Resource
	Tracker         *JobTracker
	Conf            *WorkerConf
	QueueURL        string
	HandleEmptyFunc func()
	ShouldStop      bool
}

func NewMessageHandler(resource *Resource, tracker *JobTracker, conf *Conf) *MessageHandler {
	return &MessageHandler{
		Resource:   resource,
		Tracker:    tracker,
		Conf:       &conf.Worker,
		ShouldStop: false,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
	}
}

func (h *MessageHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("MessageHandler start.")
	defer wg.Done()
	syncWait := &sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			log.Println("context cancelled. stop MessageHandler.")
			h.ShouldStop = true
			break
		default:
			if !h.Tracker.Acceptable() {
				h.HandleEmpty()
			} else if h.Tracker.HasWaitings() {
				h.HandleWaitings(ctx, syncWait)
			} else {
				results, err := h.Resource.GetMessages(ctx)
				if err != nil {
					log.Println("Error", err)
					h.HandleEmpty()
				} else if len(results) == 0 {
					log.Println("received no messages")
					h.HandleEmpty()
				} else {
					log.Printf("received %d messages. run jobs.\n", len(results))
					h.HandleMessages(ctx, results, syncWait)
				}
			}
		}
		if h.ShouldStop {
			break
		}
	}
	syncWait.Wait()
	log.Println("MessageHandler closed.")
}

func (h *MessageHandler) HandleMessages(ctx context.Context, messages []*sqs.Message, wg *sync.WaitGroup) {
	for _, msg := range messages {
		job := NewJob(msg, h.Conf)
		if h.Tracker.Add(job) {
			wg.Add(1)
			go h.HandleMessage(ctx, job, wg)
		}
	}
}

func (h *MessageHandler) HandleEmpty() {
	h.HandleEmptyFunc()
}

func (h *MessageHandler) HandleWaitings(ctx context.Context, wg *sync.WaitGroup) {
	for _, job := range h.Tracker.GetAndClearWaitings() {
		if h.Tracker.Add(job) {
			wg.Add(1)
			go h.HandleMessage(ctx, job, wg)
		}
	}
}

func (h *MessageHandler) HandleMessage(ctx context.Context, job *Job, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("job[%s] HandleMessage start.\n", job.ID())
	ok, err := job.Run(ctx)
	if err != nil {
		log.Printf("job[%s] HandleMessage request error: %s\n", job.ID(), err)
	}
	if ok {
		h.Resource.DeleteMessage(job.Msg)
	}
	h.Tracker.Delete(job)
	log.Printf("job[%s] HandleMessage finished.\n", job.ID())
}
