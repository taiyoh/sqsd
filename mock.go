package sqsd

import (
	"bytes"
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
	Err              error
}

func NewMockClient() *SQSMockClient {
	return &SQSMockClient{
		Resp: &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		},
	}
}

func (c *SQSMockClient) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	c.RecvRequestCount++
	return c.Resp, c.Err
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

type SQSMockResponseWriter struct {
	http.ResponseWriter
	header http.Header
	ResBytes []byte
	StatusCode int
	Err      error
}

func NewSQSMockResponseWriter() *SQSMockResponseWriter {
	return &SQSMockResponseWriter{
		header: http.Header{},
		ResBytes: []byte{},
		StatusCode: http.StatusOK,
	}
}

func (w *SQSMockResponseWriter) Header() http.Header {
	return w.header
}

func (w *SQSMockResponseWriter) Write(b []byte) (int, error) {
	w.ResBytes = b
	return len(b), w.Err
}

func (w *SQSMockResponseWriter) WriteHeader(s int) {
	w.StatusCode = s
}

func (w *SQSMockResponseWriter) ResponseString() string {
	return bytes.NewBuffer(w.ResBytes).String()
}
