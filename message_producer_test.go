package sqsd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestNewReceiverAndDoHandle(t *testing.T) {
	mc := NewMockClient()
	rs := NewResource(mc, "http://example.com/foo/bar/queue")
	rs.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(5)
	l := NewLogger("DEBUG")
	pr := NewMessageProducer(rs, tr, l)
	if pr == nil {
		t.Error("receiver not loaded")
	}

	handleEmptyCalled := false

	pr.HandleEmptyFunc = func() {
		handleEmptyCalled = true
	}
	mc.ErrRequestCount = 0
	tr.Pause()
	if tr.JobWorking {
		t.Error("jobworking not changed")
	}
	t.Run("tracker is not working", func(t *testing.T) {
		pr.DoHandle(context.Background())

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}
		if mc.RecvRequestCount != 0 {
			t.Errorf("receive request count exists: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	tr.Resume()

	mc.Err = errors.New("hogehoge")
	mc.RecvRequestCount = 0
	mc.ErrRequestCount = 0
	handleEmptyCalled = false
	if !tr.JobWorking {
		t.Error("jobworking flag not changed")
	}
	t.Run("received but empty messages", func(t *testing.T) {
		pr.DoHandle(context.Background())

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}

		if mc.ErrRequestCount != 1 {
			t.Error("no error")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.Err = nil
	mc.RecvRequestCount = 0
	mc.ErrRequestCount = 0
	handleEmptyCalled = false
	t.Run("received but empty messages", func(t *testing.T) {
		pr.DoHandle(context.Background())

		if !handleEmptyCalled {
			t.Error("HandleEmpty not working")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})

	mc.Resp.Messages = []*sqs.Message{
		&sqs.Message{
			MessageId: aws.String("id:1"),
			Body:      aws.String(`foobar`),
		},
	}
	mc.Err = nil
	mc.RecvRequestCount = 0
	handleEmptyCalled = false
	t.Run("received 1 message", func(t *testing.T) {
		pr.DoHandle(context.Background())

		if handleEmptyCalled {
			t.Error("HandleEmpty worked")
		}
		if mc.ErrRequestCount != 0 {
			t.Error("error exists")
		}

		receivedqueue := <-tr.NextQueue()
		if receivedqueue.ID() != *mc.Resp.Messages[0].MessageId {
			t.Error("wrong queue received")
		}

		if mc.RecvRequestCount != 1 {
			t.Errorf("receive request count wrong: %d", mc.RecvRequestCount)
		}
		if mc.DelRequestCount > 0 {
			t.Errorf("delete request count exists: %d", mc.RecvRequestCount)
		}
	})
}

func TestReceiverRun(t *testing.T) {
	mc := NewMockClient()
	rs := NewResource(mc, "http://example.com/foo/bar/queue")
	rs.ReceiveParams.WaitTimeSeconds = aws.Int64(1)
	tr := NewJobTracker(5)
	l := NewLogger("DEBUG")
	pr := NewMessageProducer(rs, tr, l)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go pr.Run(ctx, wg)

	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()
}
