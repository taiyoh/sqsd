package sqsd

import (
	"context"
	"log"
	"sync"
	"time"
)

type MessageHandler struct {
	Resource         *Resource
	Tracker          *JobTracker
	Conf             *WorkerConf
	QueueURL         string
	HandleEmptyFunc  func()
	ShouldStop       bool
	OnJobAddedFunc   func(h *MessageHandler, job *Job, ctx context.Context, wg *sync.WaitGroup)
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
			wg.Add(1)
			go h.HandleJob(ctx, job, wg)
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
		case job := <-tracker.NextJob():
			h.OnJobAdded(job, ctx, wg)
		}
	}
}

func (h *MessageHandler) OnJobAdded(job *Job, ctx context.Context, wg *sync.WaitGroup) {
	h.OnJobAddedFunc(h, job, ctx, wg)
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
	for _, msg := range results {
		h.Tracker.Add(NewJob(msg, h.Conf))
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
	h.Tracker.DeleteAndNextJob(job)
	log.Printf("job[%s] HandleJob finished.\n", job.ID())
}
