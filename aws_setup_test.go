package sqsd

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"golang.org/x/exp/slog"
)

var awsConf aws.Config

var sqsEndpoint func(*sqs.Options)

const awsRegion = "ap-northeast-1"

func init() {
	if _, ok := os.LookupEnv("SQS_ENDPOINT_URL"); !ok {
		os.Setenv("SQS_ENDPOINT_URL", "http://localhost:9324")
	}

	var err error
	awsConf, err = config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		panic(err)
	}
	sqsEndpoint = sqs.WithEndpointResolver(sqs.EndpointResolverFromURL(os.Getenv("SQS_ENDPOINT_URL")))

	SetWithGlobalLevel(slog.LevelDebug)
}

func setupSQS(t *testing.T, queue *sqs.Client, resourceName string) (string, error) {
	ctx := context.Background()
	out, err := queue.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &resourceName,
	})
	if err != nil {
		return "", err
	}
	t.Cleanup(func() {
		_, err := queue.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: out.QueueUrl,
		})
		if err != nil {
			log.Fatal(err)
		}
	})
	return *out.QueueUrl, nil
}
