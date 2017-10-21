package sqsd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"net/http"
	"strings"
	"time"
)

type Job struct {
	Msg     *sqs.Message
	StartAt time.Time
	URL     string
	blocker chan struct{}
}

type JobSummary struct {
	ID      string `json:"id"`
	StartAt int64  `json:"start_at"`
	Payload string `json:"payload"`
}

func NewJob(msg *sqs.Message, conf *WorkerConf) *Job {
	return &Job{
		Msg:     msg,
		StartAt: time.Now(),
		URL:     conf.JobURL,
	}
}

func (j *Job) MakeBlocker() {
	j.blocker = make(chan struct{})
}

func (j *Job) BreakBlocker() {
	j.blocker <- struct{}{}
}

func (j *Job) WaitUntilBreakBlocker() {
	if j.blocker != nil {
		<-j.blocker
		close(j.blocker)
	}
	return
}

func (j *Job) ID() string {
	return *j.Msg.MessageId
}

func (j *Job) Run(ctx context.Context) (bool, error) {
	j.WaitUntilBreakBlocker()

	req, err := http.NewRequest("POST", j.URL, strings.NewReader(*j.Msg.Body))
	if err != nil {
		return false, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "github.com/taiyoh/sqsd-"+GetVersion())
	req.Header.Set("X-Sqsd-Msgid", j.ID())
	req.Header.Set("X-Sqsd-First-Received-At", j.StartAt.Format("2006-01-02T15:04:05Z0700"))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if statusCode != 200 {
		log.Printf("job[%s] failed. status: %d, response: %s", j.ID(), statusCode, buf.String())
		return false, nil
	}

	return true, nil
}

func (j *Job) Summary() *JobSummary {
	return &JobSummary{
		ID:      j.ID(),
		StartAt: j.StartAt.Unix(),
		Payload: *j.Msg.Body,
	}
}
