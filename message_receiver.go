package sqsd

import (
	"context"
	"log"
	"sync"
	"time"
)

type MessageReceiver struct {
	Resource        *Resource
	Tracker         *JobTracker
	Conf            *WorkerConf
	QueueURL        string
	HandleEmptyFunc func()
}

func NewMessageReceiver(resource *Resource, tracker *JobTracker, conf *Conf) *MessageReceiver {
	return &MessageReceiver{
		Resource:   resource,
		Tracker:    tracker,
		Conf:       &conf.Worker,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
	}
}

func (r *MessageReceiver) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("MessageReceiver start.")
	defer wg.Done()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println("context cancelled. stop RunMainLoop.")
				return
			default:
				r.DoHandle(ctx)
			}
		}
	}()
	log.Println("MessageReceiver closed.")
}

func (r *MessageReceiver) HandleEmpty() {
	r.HandleEmptyFunc()
}

func (r *MessageReceiver) DoHandle(ctx context.Context) {
	if !r.Tracker.IsWorking() {
		log.Println("tracker not working")
		r.HandleEmpty()
		return
	}
	results, err := r.Resource.GetMessages(ctx)
	if err != nil {
		log.Println("Error", err)
		r.HandleEmpty()
		return
	}
	if len(results) == 0 {
		log.Println("received no messages")
		r.HandleEmpty()
		return
	}
	log.Printf("received %d messages. run jobs.\n", len(results))
	jobs := []*Job{}
	for _, msg := range results {
		jobs = append(jobs, NewJob(msg, r.Conf))
	}
	for _, job := range jobs {
		r.Tracker.Register(job)
	}
}
