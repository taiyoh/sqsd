package sqsd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"net/http"
	"net/http/httptest"
	"time"
)

type SQSMockClient struct {
	sqsiface.SQSAPI
	Resp             *sqs.ReceiveMessageOutput
	RecvRequestCount int
	DelRequestCount  int
}

func (c *SQSMockClient) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	c.RecvRequestCount++
	return c.Resp, nil
}

func (c *SQSMockClient) DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	c.DelRequestCount++
	return &sqs.DeleteMessageOutput{}, nil
}

func MockJobServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/error", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-Type", "text")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "no goood")
	})
	mux.HandleFunc("/long", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-Type", "text")
		fmt.Fprintf(w, "goood")
		time.Sleep(1 * time.Second)
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-Type", "text")
		fmt.Fprintf(w, "goood")
	})
	return httptest.NewServer(mux)
}
