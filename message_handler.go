package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"sync"
	"time"
)

type MessageHandler struct {
	Resource     *Resource
	Tracker      *JobTracker
	SleepSeconds time.Duration
	Conf         *WorkerConf
	QueueURL     string
}

func NewMessageHandler(resource *Resource, tracker *JobTracker, conf *Conf) *MessageHandler {
	return &MessageHandler{
		Resource: resource,
		Tracker:  tracker,
		Conf:     &conf.Worker,
	}
}

func (h *MessageHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("Worker start.")
	defer wg.Done()
	cancelled := false
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				cancelled = true
				return
			}
		}
	}()
	syncWait := &sync.WaitGroup{}
	for {
		if cancelled {
			break
		}
		if !h.Tracker.IsWorking() {
			time.Sleep(1 * time.Second)
			continue
		}
		results, err := h.Resource.GetMessages()
		if err != nil {
			log.Println("Error", err)
		} else if len(results) == 0 {
			log.Println("received no messages")
		} else {
			log.Printf("received %d messages. run jobs.\n", len(results))
			h.HandleMessages(ctx, results, syncWait)
		}
		time.Sleep(1 * time.Second)
	}
	syncWait.Wait()
	log.Println("Worker closed.")
}

func (h *MessageHandler) SetupJob(msg *sqs.Message) *Job {
	job := NewJob(msg, h.Conf)
	if !h.Tracker.Add(job) {
		return nil
	}
	return job
}

func (h *MessageHandler) HandleMessages(ctx context.Context, messages []*sqs.Message, wg *sync.WaitGroup) {
	for _, msg := range messages {
		if job := h.SetupJob(msg); job != nil {
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
