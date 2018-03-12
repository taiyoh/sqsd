package sqsd

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type MessageConsumer struct {
	Tracker          *JobTracker
	Resource         *Resource
	URL              string
	OnHandleJobEnds  func(jobID string, ok bool, err error)
	OnHandleJobStart func(job *Job)
	Logger           Logger
}

func NewMessageConsumer(resource *Resource, tracker *JobTracker, logger Logger, url string) *MessageConsumer {
	return &MessageConsumer{
		Tracker:          tracker,
		Resource:         resource,
		URL:              url,
		OnHandleJobStart: func(job *Job) {},
		OnHandleJobEnds:  func(jobID string, ok bool, err error) {},
		Logger:           logger,
	}
}

func (c *MessageConsumer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	syncWait := new(sync.WaitGroup)
	loopEnds := false
	c.Logger.Info("MessageConsumer start.")
	for {
		select {
		case <-ctx.Done():
			syncWait.Wait()
			loopEnds = true
			break
		case job := <-c.Tracker.NextJob():
			syncWait.Add(1)
			go func() {
				defer syncWait.Done()
				c.HandleJob(ctx, job)
			}()
		}
		if loopEnds {
			break
		}
	}
	c.Logger.Info("MessageConsumer closed.")
}

func (c *MessageConsumer) HandleJob(ctx context.Context, job *Job) {
	c.OnHandleJobStart(job)
	c.Logger.Debug(fmt.Sprintf("job[%s] HandleJob start.\n", job.ID()))
	ok, err := c.CallWorker(ctx, job)
	if err != nil {
		c.Logger.Error(fmt.Sprintf("job[%s] HandleJob request error: %s\n", job.ID(), err))
	}
	if ok {
		c.Resource.DeleteMessage(job.Msg)
	}
	c.Tracker.Complete(job)
	c.Logger.Debug(fmt.Sprintf("job[%s] HandleJob finished.\n", job.ID()))
	c.OnHandleJobEnds(job.ID(), ok, err)
}

func (c *MessageConsumer) CallWorker(ctx context.Context, j *Job) (bool, error) {
	req, err := http.NewRequest("POST", c.URL, strings.NewReader(*j.Msg.Body))
	if err != nil {
		return false, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "github.com/taiyoh/sqsd-"+GetVersion())
	req.Header.Set("X-Sqsd-Msgid", j.ID())
	req.Header.Set("X-Sqsd-First-Received-At", j.ReceivedAt.Format("2006-01-02T15:04:05Z0700"))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if statusCode != 200 {
		return false, nil
	}

	return true, nil
}
