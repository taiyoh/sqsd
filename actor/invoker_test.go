package sqsd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type invokerTestPayload struct {
	Sleep  time.Duration `json:"sleep"`
	Status int           `json:"status"`
}

func TestHTTPInvoker(t *testing.T) {
	mux := &http.ServeMux{}
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		p := &invokerTestPayload{}
		_ = json.NewDecoder(r.Body).Decode(p)
		time.Sleep(p.Sleep)
		w.WriteHeader(p.Status)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	i, err := NewHTTPInvoker(srv.URL+"/echo", 300*time.Millisecond)
	assert.NoError(t, err)

	for _, tt := range []struct {
		label       string
		status      int
		sleep       int64
		expectedErr bool
	}{
		{
			label:  "200",
			status: http.StatusOK,
		},
		{
			label:  "400",
			status: http.StatusBadRequest,
		},
		{
			label:       "500",
			status:      http.StatusInternalServerError,
			expectedErr: true,
		},
		{
			label:       "timeout",
			sleep:       500,
			status:      http.StatusOK,
			expectedErr: true,
		},
	} {
		t.Run(tt.label, func(t *testing.T) {
			b, _ := json.Marshal(invokerTestPayload{
				Status: tt.status,
				Sleep:  time.Duration(tt.sleep) * time.Millisecond,
			})
			err := i.Invoke(context.Background(), Message{
				Payload: string(b),
			})
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
