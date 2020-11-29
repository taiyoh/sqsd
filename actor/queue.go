package sqsd

import "time"

// QueueResultStatus represents status for Queue result.
type QueueResultStatus int

const (
	// NotRequested represents queue has no result.
	NotRequested QueueResultStatus = iota // default
	// RequestSuccess represents queue request has succeeded.
	RequestSuccess
	// RequestFail represents queue request has failed.
	RequestFail
)

// Queue provides transition from sqs.Message
type Queue struct {
	ID           string
	Payload      string
	Receipt      string
	ReceivedAt   time.Time
	ResultStatus QueueResultStatus
}

// ResultSucceeded returns Queue has RequestSuccess status.
func (q Queue) ResultSucceeded() Queue {
	qq := q
	qq.ResultStatus = RequestSuccess
	return qq
}

// ResultFailed returns Queue has RequestFail status.
func (q Queue) ResultFailed() Queue {
	qq := q
	qq.ResultStatus = RequestFail
	return qq
}
