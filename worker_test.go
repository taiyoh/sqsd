package sqsd

import (
	"bytes"
	"encoding/json"
	"time"
	"fmt"
	"net/http"
	"net/http/httptest"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"testing"
)

func TestNewWorker(t *testing.T) {
	c := &SQSDConf{}
	r := &SQSResource{}
	w := NewWorker(r, c)
	if w == nil {
		t.Error("worker not loaded")
	}
}

func TestSetupJob(t *testing.T) {
	c := &SQSDConf{}
	r := &SQSResource{}
	w := NewWorker(r, c)

	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	job := w.SetupJob(sqsMsg)
	if job == nil {
		t.Error("job not created")
	}

	if _, exists := w.CurrentWorkings[job.ID()]; !exists {
		t.Error("job not registered")
	}

	delete(w.CurrentWorkings, job.ID())
	if _, exists := w.CurrentWorkings[job.ID()]; exists {
		t.Error("job not deleted")
	}
}

func TestIsRunnable(t *testing.T) {
	c := &SQSDConf{ProcessCount: 5}
	r := &SQSResource{}
	w := NewWorker(r, c)

	w.Runnable = false
	if w.IsWorkerAvailable() {
		t.Error("IsWorkerAvailable flag is wrong")
	}
	w.Runnable = true
	if !w.IsWorkerAvailable() {
		t.Error("IsWorkerAvailable flag is wrong")
	}

	for i := 1; i <= w.ProcessCount-1; i++ {
		w.SetupJob(&sqs.Message{
			MessageId: aws.String("id:" + strconv.Itoa(i)),
			Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
		})
		if !w.IsWorkerAvailable() {
			t.Errorf("flag disabled! idx: %d", i)
		}
	}

	w.SetupJob(&sqs.Message{
		MessageId: aws.String("id:" + strconv.Itoa(w.ProcessCount)),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	})
	if w.IsWorkerAvailable() {
		t.Errorf("flag disabled! idx: %d", w.ProcessCount)
	}
}

func TestCanWork(t *testing.T) {
	c := &SQSDConf{ProcessCount: 5}
	r := &SQSResource{}
	w := NewWorker(r, c)

	sqsMsg := &sqs.Message{
		MessageId: aws.String("id:100000"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	w.Runnable = false
	if w.CanWork(sqsMsg) {
		t.Error("CanWork not working")
	}

	w.Runnable = true
	if !w.CanWork(sqsMsg) {
		t.Error("CanWork not working")
	}
	w.SetupJob(sqsMsg)
	if w.CanWork(sqsMsg) {
		t.Error("Canwork not working")
	}
}

func TestHandleMessage(t *testing.T) {
	c := &SQSDConf{ProcessCount: 5}
	r := &SQSResource{Client: &SQSMockClient{}}
	w := NewWorker(r, c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := MockJobServer()
	defer ts.Close()

	t.Run("job failed", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/error"

		go w.HandleMessage(ctx, job)
		<-job.Done()
		if _, exists := w.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/long"
		parent, cancel := context.WithCancel(ctx)
		go w.HandleMessage(parent, job)
		cancel()
		<-job.Done()
		if _, exists := w.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})

	t.Run("success", func(t *testing.T) {
		job := w.SetupJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		job.URL = ts.URL + "/ok"
		go w.HandleMessage(ctx, job)
		<-job.Done()
		if _, exists := w.CurrentWorkings[job.ID()]; exists {
			t.Error("working job yet exists")
		}
	})
}

func TestHandleMessages(t *testing.T) {
	type JobPayload struct {
		ID string
	}
	msgs := []*sqs.Message{}
	for i := 1; i <= 10; i++ {
		idxStr := strconv.Itoa(i)
		p := &JobPayload{ID: "msgid:" +strconv.Itoa(i)}
		buf, _ := json.Marshal(p)
		msgs = append(msgs, &sqs.Message{
			MessageId: aws.String(p.ID),
			Body: aws.String(bytes.NewBuffer(buf).String()),
			ReceiptHandle: aws.String("receithandle-" + idxStr),
		})
	}
	caughtIds := map[string]bool{}
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			buf := new(bytes.Buffer)
			buf.ReadFrom(r.Body)
			var p JobPayload
			json.Unmarshal(buf.Bytes(), &p)
			caughtIds[p.ID] = true
			w.Header().Set("content-Type", "text")
			fmt.Fprintf(w, "goood")
			return
		},
	))
	defer ts.Close()

	c := &SQSDConf{ProcessCount: 5, HTTPWorker: SQSDHttpWorkerConf{URL: ts.URL}}
	r := &SQSResource{ Client: &SQSMockClient{} }
	w := NewWorker(r, c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.HandleMessages(ctx, msgs)
	time.Sleep(1 * time.Second)

	if len(caughtIds) != c.ProcessCount {
		t.Errorf("requests is wrong: %d", len(caughtIds))
	}

	for i := 6; i <= 10; i++ {
		id := "msgid:" + strconv.Itoa(i)
		if _, exists := caughtIds[id]; exists {
			t.Errorf("id: %s exists", id)
		}
	}
}