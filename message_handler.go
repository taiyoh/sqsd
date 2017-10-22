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

func (h *MessageHandler) convertMessagesTojobs(messages []*sqs.Message) []*Job {
	jobs := make([]*Job, len(messages))
	for idx, msg := range messages {
		jobs[idx] = NewJob(msg, h.Conf)
	}
	return jobs
}

func (h *MessageHandler) HandleEmpty() {
	h.HandleEmptyFunc()
}

func (h *MessageHandler) HandleMessages(ctx context.Context, messages []*sqs.Message, wg *sync.WaitGroup) {
	h.HandleJobs(ctx, h.convertMessagesTojobs(messages), wg)
}

func (h *MessageHandler) HandleWaitings(ctx context.Context, wg *sync.WaitGroup) {
	h.HandleJobs(ctx, h.Tracker.GetAndClearWaitings(), wg)
}

func (h *MessageHandler) HandleJobs(ctx context.Context, jobs []*Job, wg *sync.WaitGroup) {
	for _, job := range jobs {
		if h.Tracker.Add(job) {
			wg.Add(1)
			go h.HandleJob(ctx, job, wg)
		}
	}

}

func (h *MessageHandler) HandleJob(ctx context.Context, job *Job, wg *sync.WaitGroup) {
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
