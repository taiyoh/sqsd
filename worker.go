package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"time"
)

type SQSWorker struct {
	Resource     *SQSResource
	Tracker      *SQSJobTracker
	SleepSeconds time.Duration
	Conf         *SQSDHttpWorkerConf
	QueueURL     string
}

func NewWorker(resource *SQSResource, tracker *SQSJobTracker, conf *SQSDConf) *SQSWorker {
	return &SQSWorker{
		Resource:     resource,
		Tracker:      tracker,
		SleepSeconds: time.Duration(conf.SleepSeconds),
		Conf:         &conf.HTTPWorker,
	}
}

func (w *SQSWorker) Run(ctx context.Context) {
	log.Println("SQSWorker start.")
	cancelled := false
	go func() {
		for {
			select {
			case <-ctx.Done():
				cancelled = true
				return
			}
		}
	}()
	for {
		if cancelled {
			break
		}
		if !w.Tracker.IsWorking() {
			time.Sleep(w.SleepSeconds * time.Second)
			continue
		}
		results, err := w.Resource.GetMessages()
		if err != nil {
			log.Println("Error", err)
		} else if len(results) == 0 {
			log.Println("received no messages")
		} else {
			log.Printf("received %d messages. run jobs.\n", len(results))
			w.HandleMessages(ctx, results)
		}
		time.Sleep(w.SleepSeconds * time.Second)
	}
}

func (w *SQSWorker) SetupJob(msg *sqs.Message) *SQSJob {
	job := NewJob(msg, w.Conf)
	if ok := w.Tracker.Add(job); !ok {
		return nil
	}
	return job
}

func (w *SQSWorker) HandleMessages(ctx context.Context, messages []*sqs.Message) {
	for _, msg := range messages {
		if !w.Tracker.IsWorking() {
			continue
		}
		job := w.SetupJob(msg)
		if job == nil {
			continue
		}
		go w.HandleMessage(ctx, job)
	}
}

func (w *SQSWorker) HandleMessage(ctx context.Context, job *SQSJob) {
	ok, err := job.Run(ctx)
	if err != nil {
		log.Printf("job[%s] HandleMessage request error: %s\n", job.ID(), err)
	}
	if ok {
		w.Resource.DeleteMessage(job.Msg)
	}
	w.Tracker.Delete(job)
	job.Done() <- struct{}{}
}
