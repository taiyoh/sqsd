package benchmark_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	sqsd_v0 "github.com/taiyoh/sqsd"
	sqsd_actor "github.com/taiyoh/sqsd/actor"
)

var awsConf *aws.Config

const awsRegion = "ap-northeast-1"

func init() {
	if _, ok := os.LookupEnv("SQS_ENDPOINT_URL"); !ok {
		os.Setenv("SQS_ENDPOINT_URL", "http://localhost:4100")
	}
	awsConf = aws.NewConfig().
		WithCredentialsChainVerboseErrors(true).
		WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "")).
		WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")).
		WithRegion(awsRegion)
}

type invokerCommon struct {
}

func (i *invokerCommon) Run() error {
	time.Sleep(100 * time.Millisecond)
	return nil
}

type v0Invoker struct {
	invokerCommon
}

func (i v0Invoker) Invoke(context.Context, sqsd_v0.Queue) error {
	return i.Run()
}

var _ sqsd_v0.WorkerInvoker = (*v0Invoker)(nil)

type actorInvoker struct {
	invokerCommon
}

func (i actorInvoker) Invoke(context.Context, sqsd_actor.Message) error {
	return i.Run()
}

var _ sqsd_actor.Invoker = (*actorInvoker)(nil)

const parallel = 10

func setupSQS(queue *sqs.SQS, resourceName string) (string, func(), error) {
	out, err := queue.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &resourceName,
	})
	if err != nil {
		return "", nil, err
	}
	for i := 0; i < 1000; i++ {
		_, _ = queue.SendMessage(&sqs.SendMessageInput{
			MessageBody: aws.String(fmt.Sprintf(`{"index":%d,"message":"foo-bar"}`, i)),
			QueueUrl:    out.QueueUrl,
		})
	}
	cleanup := func() {
		_, err := queue.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: out.QueueUrl,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	return *out.QueueUrl, cleanup, nil
}

func BenchmarkSQSD_v0(b *testing.B) {
	resourceName := fmt.Sprintf("benchmark-v0-%d", time.Now().UnixNano())
	queue := sqs.New(session.Must(session.NewSession()), awsConf)

	queueURL, cleanup, err := setupSQS(queue, resourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	logger := sqsd_v0.NewLogger("INFO")

	tracker := sqsd_v0.NewQueueTracker(parallel, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ivk := v0Invoker{
		invokerCommon{},
	}

	resource := sqsd_v0.NewResource(queue, sqsd_v0.SQSConf{
		Concurrency: 1,
		URL:         queueURL,
		Region:      awsRegion,
		WaitTimeSec: 20,
	})
	msgConsumer := sqsd_v0.NewMessageConsumer(resource, tracker, ivk)
	msgProducer := sqsd_v0.NewMessageProducer(resource, tracker, 1)

	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			out, _ := queue.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueUrl:       &queueURL,
				AttributeNames: aws.StringSlice([]string{"All"}),
			})
			// log.Printf("out.Attributes: %#v", out.Attributes)
			mv := *out.Attributes["ApproximateNumberOfMessages"]
			if visible, _ := strconv.ParseInt(mv, 10, 64); visible == 0 {
				return
			}
			time.Sleep(time.Second)
		}
	}()

	b.ResetTimer()

	go func() {
		defer wg.Done()
		msgConsumer.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = msgProducer.Run(ctx)
	}()

	wg.Wait()
}

func BenchmarkSQSD_actor(b *testing.B) {
	resourceName := fmt.Sprintf("benchmark-actor-%d", time.Now().UnixNano())
	queue := sqs.New(session.Must(session.NewSession()), awsConf)

	queueURL, cleanup, err := setupSQS(queue, resourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ivk := actorInvoker{
		invokerCommon{},
	}

	sys := sqsd_actor.NewSystem(queue, ivk, sqsd_actor.SystemConfig{
		QueueURL:          queueURL,
		FetcherParallel:   1,
		InvokerParallel:   parallel,
		VisibilityTimeout: 60 * time.Second,
		MonitoringPort:    sqsd_actor.DisableMonitoring,
	})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			out, _ := queue.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueUrl:       &queueURL,
				AttributeNames: aws.StringSlice([]string{"All"}),
			})
			mv := *out.Attributes["ApproximateNumberOfMessages"]
			visible, _ := strconv.ParseInt(mv, 10, 64)
			if visible == 0 {
				return
			}
			time.Sleep(time.Second)
		}
	}()

	b.ResetTimer()
	if err := sys.Run(ctx); err != nil {
		log.Fatal(err)
	}

}

/*
BenchmarkSQSD_v0-8      63	  16348960 ns/op	   47347 B/op	     677 allocs/op
BenchmarkSQSD_actor-8   1	1542513600 ns/op	  420016 B/op	    5107 allocs/op
*/
