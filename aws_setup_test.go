package sqsd

import (
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/exp/slog"
)

var awsConf *aws.Config

const awsRegion = "ap-northeast-1"

func init() {
	if _, ok := os.LookupEnv("SQS_ENDPOINT_URL"); !ok {
		os.Setenv("SQS_ENDPOINT_URL", "http://localhost:9324")
	}

	awsConf = aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "")).
		WithRegion(awsRegion)

	slogHandlerOpts := slog.HandlerOptions{Level: slog.LevelDebug}
	SetWithHandlerOptions(slogHandlerOpts)
}

func setupSQS(t *testing.T, queue *sqs.SQS, resourceName string) (string, error) {
	out, err := queue.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &resourceName,
	})
	if err != nil {
		return "", err
	}
	t.Cleanup(func() {
		_, err := queue.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: out.QueueUrl,
		})
		if err != nil {
			log.Fatal(err)
		}
	})
	return *out.QueueUrl, nil
}
