package sqsd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type MessageHandler struct {
<<<<<<< HEAD
	Resource *Resource
	Tracker  *JobTracker
	Conf     *WorkerConf
	QueueURL string
=======
	Resource     *Resource
	Tracker      *JobTracker
	SleepSeconds time.Duration
	Conf         *WorkerConf
	QueueURL     string
	mu           sync.RWMutex
>>>>>>> race condition等々対策
}

func NewMessageHandler(resource *Resource, tracker *JobTracker, conf *Conf) *MessageHandler {
	return &MessageHandler{
		Resource: resource,
		Tracker:  tracker,
		Conf:     &conf.Worker,
		mu:       sync.RWMutex{},
	}
}

func (h *MessageHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("Worker start.")
	defer wg.Done()
	syncWait := &sync.WaitGroup{}
	stopLoop := false
	for {
		select {
		case <-ctx.Done():
			log.Println("context cancelled. stop worker.")
			stopLoop = true
			break
		default:
			if !h.Tracker.IsWorking() {
				time.Sleep(1 * time.Second)
			} else {
				results, err := h.Resource.GetMessages()
				if err != nil {
					log.Println("Error", err)
				} else if len(results) == 0 {
					log.Println("received no messages")
				} else {
					log.Printf("received %d messages. run jobs.\n", len(results))
					h.HandleMessages(ctx, results, syncWait)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
		if stopLoop {
			break
		}
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
