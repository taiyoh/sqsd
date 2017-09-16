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
	pauseChan       chan bool
}

func NewWorker(resource *SQSResource, conf *SQSDConf) *SQSWorker {
	return &SQSWorker{
		Resource:        resource,
		SleepSeconds:    time.Duration(conf.SleepSeconds),
		ProcessCount:    conf.ProcessCount,
		CurrentWorkings: make(map[string]*SQSJob),
		Conf:            &conf.HTTPWorker,
		Runnable:        true,
		pauseChan:       make(chan bool),
	}
}

func (w *SQSWorker) Pause() chan bool {
	return w.pauseChan
}

func (w *SQSWorker) Run(ctx context.Context) {
	log.Println("SQSWorker start.")
	for {
		select {
		case <-ctx.Done():
			log.Println("context cancel signal caught. Run ends.")
			return
		case shouldStop := <-w.Pause():
			w.Runnable = shouldStop == false
		default:
			if !w.IsWorkerAvailable() {
				time.Sleep(w.SleepSeconds * time.Second)
				continue
			}
			results, err := w.Resource.GetMessages()
			if err != nil {
				log.Println("Error", err)
				time.Sleep(w.SleepSeconds * time.Second)
			} else if len(results) == 0 {
				log.Println("received no messages")
				time.Sleep(w.SleepSeconds * time.Second)
			} else {
				log.Printf("received %d messages. run jobs", len(results))
				w.HandleMessages(ctx, results)
			}
		}
	}
}

func (w *SQSWorker) SetupJob(msg *sqs.Message) *SQSJob {
	job := NewJob(msg, w.Conf)
	w.CurrentWorkings[job.ID()] = job
	return job
}

func (w *SQSWorker) HandleMessages(ctx context.Context, messages []*sqs.Message) {
	for _, msg := range messages {
		if !w.CanWork(msg) {
			continue
		}
		job := w.SetupJob(msg)
		go w.HandleMessage(ctx, job)
	}
}

func (w *SQSWorker) HandleMessage(ctx context.Context, job *SQSJob) {
	ok, err := job.Run(ctx)
	if err != nil {
		log.Printf("job[%s] HandleMessage request error: %s\n", job.ID(), err)
	}
	if ok {
		w.Resource.DeleteMessage(job.Msg())
	}
	delete(w.CurrentWorkings, job.ID())
	job.Done() <- struct{}{}
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
