package sqsd

import (
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

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
	return Queue{
		ID:           q.ID,
		Payload:      q.Payload,
		Receipt:      q.Receipt,
		ReceivedAt:   q.ReceivedAt,
		ResultStatus: RequestSuccess,
	}
}

// ResultFailed returns Queue has RequestFail status.
func (q Queue) ResultFailed() Queue {
	return Queue{
		ID:           q.ID,
		Payload:      q.Payload,
		Receipt:      q.Receipt,
		ReceivedAt:   q.ReceivedAt,
		ResultStatus: RequestFail,
	}
}

// QueueSummary provides transition from Queue for stat
type QueueSummary struct {
	ID         string `json:"id"`
	ReceivedAt int64  `json:"received_at"`
	Payload    string `json:"payload"`
}

// NewQueue returns Queue object from sqs.Message
func NewQueue(msg *sqs.Message) Queue {
	return Queue{
		ID:         *msg.MessageId,
		Payload:    *msg.Body,
		Receipt:    *msg.ReceiptHandle,
		ReceivedAt: time.Now(),
	}
}

// Summary returns QueueSummary object from Queue
func (q Queue) Summary() QueueSummary {
	return QueueSummary{
		ID:         q.ID,
		ReceivedAt: q.ReceivedAt.Unix(),
		Payload:    q.Payload,
	}
}
