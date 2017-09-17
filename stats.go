package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fukata/golang-stats-api-handler"
	"log"
	"net/http"
	"strconv"
)

type SQSStat struct {
	Port    int
	Tracker *SQSJobTracker
	Mux     *http.ServeMux
}

type SQSStatResponseIFace interface{}

type SQSStatCurrentListResponse struct {
	SQSStatResponseIFace
	CurrentList []*SQSJobSummary `json:"current_list"`
}

type SQSStatCurrentSizeResponse struct {
	SQSStatResponseIFace
	Size int `json:"size"`
}

type SQSStatSuccessResponse struct {
	SQSStatResponseIFace
	Success bool `json:"success"`
}

func ReqMethodValidate(w http.ResponseWriter, r *http.Request, m string) bool {
	if r.Method == m {
		return true
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusMethodNotAllowed)
	fmt.Fprint(w, "Method Not Allowed")
	return false
}

func RenderJSON(w http.ResponseWriter, res SQSStatResponseIFace) {
	buf, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, bytes.NewBuffer(buf).String())
}

func NewStat(tracker *SQSJobTracker, conf *SQSDConf) *SQSStat {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/current/list", func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		RenderJSON(w, &SQSStatCurrentListResponse{
			CurrentList: tracker.CurrentSummaries(),
		})
	})
	mux.HandleFunc("/worker/current/size", func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "GET") {
			return
		}
		RenderJSON(w, &SQSStatCurrentSizeResponse{
			Size: len(tracker.CurrentSummaries()),
		})
	})
	mux.HandleFunc("/worker/pause", func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		tracker.Pause()
		RenderJSON(w, &SQSStatSuccessResponse{
			Success: true,
		})
	})
	mux.HandleFunc("/worker/resume", func(w http.ResponseWriter, r *http.Request) {
		if !ReqMethodValidate(w, r, "POST") {
			return
		}
		tracker.Resume()
		RenderJSON(w, &SQSStatSuccessResponse{
			Success: true,
		})
	})

	return &SQSStat{
		Port:    conf.Stat.Port,
		Tracker: tracker,
		Mux:     mux,
	}
}

func (s *SQSStat) Run(ctx context.Context) {
	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(s.Port),
		Handler: s.Mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
    }

    log.Println("stat server closed.")
}
