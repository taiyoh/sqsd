package sqsd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	resourceName := fmt.Sprintf("system-%d", time.Now().UnixNano())
	queue := sqs.NewFromConfig(awsConf, sqsEndpoint)
	queueURL, err := setupSQS(t, queue, resourceName)
	if err != nil {
		panic(err)
	}

	l, err := net.Listen("tcp4", ":0")
	assert.NoError(t, err)
	assert.NotNil(t, l)
	port, err := strconv.Atoi(strings.Split(l.Addr().String(), ":")[1])
	assert.NoError(t, err)
	l.Close()
	sys := NewSystem(
		GatewayBuilder(queue, queueURL, 1, time.Hour),
		ConsumerBuilder(nil, 3),
		MonitorBuilder(port),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- sys.Run(ctx)
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	assert.NoError(t, <-errCh)
}
