package sqsd

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// Handler provides abstruct interface for worker invokation.
type Handler interface {
	Run(context.Context, Queue) error
}

type httpHandler struct {
	url string
	cli *http.Client
}

var _ Handler = (*httpHandler)(nil)

// NewHTTPHandler returns Handler object for HTTP invokation.
func NewHTTPHandler(url string) Handler {
	return &httpHandler{
		url: url,
		cli: &http.Client{},
	}
}

func (h *httpHandler) Run(ctx context.Context, q Queue) error {
	req, _ := http.NewRequest(http.MethodPost, h.url, strings.NewReader(q.Payload))
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "github.com/taiyoh/sqsd-"+GetVersion())
	req.Header.Set("X-Sqsd-Msgid", q.ID)
	req.Header.Set("X-Sqsd-First-Received-At", q.ReceivedAt.Format("2006-01-02T15:04:05Z0700"))
	resp, err := h.cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("worker response status: " + resp.Status)
	}

	return nil
}
