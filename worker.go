package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"time"
)

type SQSWorker struct {
	Resource        *SQSResource
	SleepSeconds    time.Duration
	ProcessCount    int
	CurrentWorkings map[string]*SQSJob
	Conf            *SQSDHttpWorkerConf
	QueueURL        string
	Runnable        bool
	Pause           chan bool
}

func NewWorker(resource *SQSResource, conf *SQSDConf) *SQSWorker {
	return &SQSWorker{
		Resource:        resource,
		SleepSeconds:    time.Duration(conf.SleepSeconds),
		ProcessCount:    conf.ProcessCount,
		CurrentWorkings: make(map[string]*SQSJob),
		Conf:            &conf.HTTPWorker,
		Runnable:        true,
		Pause:           make(chan bool),
	}
}

func (w *SQSWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			break
		case shouldStop := <-w.Pause:
			w.Runnable = shouldStop == false
		default:
			if w.IsWorkerAvailable() {
				results, err := w.Resource.GetMessages()
				if err != nil {
					log.Println("Error", err)
				} else if len(results) == 0 {
					log.Println("received no messages")
				} else {
					log.Printf("received %d messages. run jobs", len(results))
					w.HandleMessages(results)
				}
			}
			time.Sleep(w.SleepSeconds * time.Second)
		}
	}
}

func (w *SQSWorker) SetupJob(msg *sqs.Message) *SQSJob {
	job := NewJob(msg, w.Conf)
	w.CurrentWorkings[job.ID()] = job
	return job
}

func (w *SQSWorker) HandleMessages(messages []*sqs.Message) {
	for _, msg := range messages {
		if !w.CanWork(msg) {
			continue
		}
		job := w.SetupJob(msg)
		go w.HandleMessage(job)
	}
}

func (w *SQSWorker) HandleMessage(job *SQSJob) {
	ok := job.Run()
	if ok {
		w.Resource.DeleteMessage(job.Msg)
	}
	delete(w.CurrentWorkings, job.ID())
}

func (w *SQSWorker) IsWorkerAvailable() bool {
	if !w.Runnable {
		return false
	}
	if len(w.CurrentWorkings) >= w.ProcessCount {
		return false
	}
	return true
}

func (w *SQSWorker) CanWork(msg *sqs.Message) bool {
	if !w.IsWorkerAvailable() {
		return false
	}
	_, exists := w.CurrentWorkings[*msg.MessageId]
	return exists == false
}
