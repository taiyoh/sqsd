package sqsd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type MessageHandler struct {
	Resource *Resource
	Tracker  *JobTracker
	Conf     *WorkerConf
	QueueURL string
	HandleMessagesFunc func(context.Context, []*sqs.Message, *sync.WaitGroup)
	HandleEmptyFunc func()
	ShouldStop bool
}

func NewMessageHandler(resource *Resource, tracker *JobTracker, conf *Conf) *MessageHandler {
	h := &MessageHandler{
		Resource: resource,
		Tracker:  tracker,
		Conf:     &conf.Worker,
		ShouldStop: false,
	}
	h.HandleMessagesFunc = func(ctx context.Context, messages []*sqs.Message, wg *sync.WaitGroup) {
		log.Println("original HandleMessages start")
		for _, msg := range messages {
			if job := h.SetupJob(msg); job != nil {
				wg.Add(1)
				go h.HandleMessage(ctx, job, wg)
			}
		}
		log.Println("original HandleMessages ends")
	}
	h.HandleEmptyFunc = func() {
		time.Sleep(1 * time.Second)
	}
	return h
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
			if !h.Tracker.IsWorking() {
				h.HandleEmpty()
			} else {
				results, err := h.Resource.GetMessages()
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

func (h *MessageHandler) SetupJob(msg *sqs.Message) *Job {
	job := NewJob(msg, h.Conf)
	if !h.Tracker.Add(job) {
		return nil
	}
	return job
}

func (h *MessageHandler) HandleMessages(ctx context.Context, messages []*sqs.Message, wg *sync.WaitGroup) {
	h.HandleMessagesFunc(ctx, messages, wg)
}

func (h *MessageHandler) HandleEmpty() {
	h.HandleEmptyFunc()
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
