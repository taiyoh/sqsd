package sqsd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"time"
	"fmt"
	"context"
)

type SQSWorker struct {
	Req *sqs.ReceiveMessageInput
	Svc *sqs.SQS
	SleepSeconds time.Duration
	ProcessCount int
	CurrentWorkings map[string]*SQSJob
	Conf *SQSDHttpWorkerConf
	QueueUrl string
}

func NewWorker(conf *SQSDConf) *SQSWorker {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
        SharedConfigState: session.SharedConfigEnable,
    }))
	svc := sqs.New(sess)
	req := &sqs.ReceiveMessageInput{
        AttributeNames: []*string{
            aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
        },
        MessageAttributeNames: []*string{
            aws.String(sqs.QueueAttributeNameAll),
        },
        QueueUrl:            &conf.QueueURL,
        MaxNumberOfMessages: aws.Int64(conf.MaxMessagesPerRequest),
        VisibilityTimeout:   aws.Int64(10),  // 10 sec
        WaitTimeSeconds:     aws.Int64(conf.WaitTimeSeconds),
    }

	return &SQSWorker{
		Req : req,
		Svc : svc,
		SleepSeconds : time.Duration(conf.SleepSeconds),
		ProcessCount : conf.ProcessCount,
		CurrentWorkings : make(map[string]*SQSJob),
		Conf : &conf.HTTPWorker,
		QueueUrl : conf.QueueURL,
	}
}

func (w *SQSWorker) Stop() {
}

func (w *SQSWorker) Run() {
	for {
		if w.IsWorkerAvailable() {
			result, err := w.Svc.ReceiveMessage(w.Req)
			if err != nil {
				fmt.Println("Error", err)
			} else if len(result.Messages) == 0 {
				fmt.Println("Received no messages")
			} else {
				w.HandleMessages(result.Messages)
			}
		}
		time.Sleep(w.SleepSeconds * time.Second)
	}
}

func (w *SQSWorker) HandleMessages(messages []*sqs.Message) {
	for _, msg := range messages {
		if w.CanWork(msg) {
			job := NewJob(msg, w.Conf)
			ctx := context.Background()
			w.CurrentWorkings[job.ID] = job
			go job.Run(ctx)
			select {
			case <- ctx.Done(): // 
				close(job.Finished)
				delete(w.CurrentWorkings, job.ID)
			case <- job.Finished:
				w.Svc.DeleteMessageRequest(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(w.QueueUrl),
					ReceiptHandle: aws.String(*msg.ReceiptHandle),
				})
				close(job.Finished)
				delete(w.CurrentWorkings, job.ID)
			}
		}
	}
}

func (w *SQSWorker) IsWorkerAvailable() bool {
	return len(w.CurrentWorkings) < w.ProcessCount
}

func (w *SQSWorker) CanWork(msg *sqs.Message) bool {
	if !w.IsWorkerAvailable() {
		return false
	}
	_, exists := w.CurrentWorkings[*msg.MessageId]
	return exists == false
}
