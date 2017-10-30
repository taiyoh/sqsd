package sqsd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type MessageHandler struct {
	Resource         *Resource
	Tracker          *JobTracker
	Conf             *WorkerConf
	QueueURL         string
	HandleEmptyFunc  func()
	ShouldStop       bool
	OnJobAddedFunc   func(h *MessageHandler, job *Job, ctx context.Context, wg *sync.WaitGroup)
	OnJobDeletedFunc func(h *MessageHandler, ctx context.Context, wg *sync.WaitGroup)
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
		OnJobAddedFunc: func(h *MessageHandler, job *Job, ctx context.Context, wg *sync.WaitGroup) {
			if job == nil {
				return
			}
			wg.Add(1)
			go h.HandleJob(ctx, job, wg)
		},
		OnJobDeletedFunc: func(h *MessageHandler, ctx context.Context, wg *sync.WaitGroup) {
			tracker := h.Tracker
			if !tracker.Acceptable() {
				return
			}
			if job := tracker.ShiftWaitingJobs(); job != nil {
				tracker.Add(job)
			}
		},
	}
}

func (h *MessageHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("MessageHandler start.")
	defer wg.Done()
	syncWait := &sync.WaitGroup{}
	syncWait.Add(2)
	go h.RunTrackerEventListener(ctx, syncWait)
	go h.RunMainLoop(ctx, syncWait)
	syncWait.Wait()
	log.Println("MessageHandler closed.")
}

func (h *MessageHandler) RunTrackerEventListener(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	tracker := h.Tracker
	for {
		select {
		case <-ctx.Done():
			log.Println("context cancelled. stop RunTrackerEventListener.")
			return
		case <-tracker.JobDeleted():
			h.OnJobDeleted(ctx, wg)
		case job := <-tracker.JobAdded():
			h.OnJobAdded(job, ctx, wg)
		default:
			continue
		}
	}
}

func (h *MessageHandler) OnJobAdded(job *Job, ctx context.Context, wg *sync.WaitGroup) {
	h.OnJobAddedFunc(h, job, ctx, wg)
}

func (h *MessageHandler) OnJobDeleted(ctx context.Context, wg *sync.WaitGroup) {
	h.OnJobDeletedFunc(h, ctx, wg)
}

func (h *MessageHandler) RunMainLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Println("context cancelled. stop RunMainLoop.")
			return
		default:
			h.DoHandle(ctx, wg)
		}
	}
}

func (h *MessageHandler) DoHandle(ctx context.Context, wg *sync.WaitGroup) {
	if !h.Tracker.Acceptable() {
		log.Println("tracker not acceptable")
		h.HandleEmpty()
		return
	}
	results, err := h.Resource.GetMessages(ctx)
	if err != nil {
		log.Println("Error", err)
		h.HandleEmpty()
		return
	}
	if len(results) == 0 {
		log.Println("received no messages")
		h.HandleEmpty()
		return
	}
	log.Printf("received %d messages. run jobs.\n", len(results))
	h.HandleMessages(ctx, results, wg)
}

func (h *MessageHandler) HandleMessages(ctx context.Context, msgs []*sqs.Message, wg *sync.WaitGroup) {
	for _, msg := range msgs {
		job := NewJob(msg, h.Conf)
		h.Tracker.Add(job)
	}
}

func (h *MessageHandler) HandleEmpty() {
	h.HandleEmptyFunc()
}

func (h *MessageHandler) HandleJob(ctx context.Context, job *Job, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("job[%s] HandleJob start.\n", job.ID())
	ok, err := job.Run(ctx)
	if err != nil {
		log.Printf("job[%s] HandleJob request error: %s\n", job.ID(), err)
	}
	if ok {
		h.Resource.DeleteMessage(job.Msg)
	}
	h.Tracker.Delete(job)
	log.Printf("job[%s] HandleJob finished.\n", job.ID())
}
