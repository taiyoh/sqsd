package sqsd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fukata/golang-stats-api-handler"
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

func ResponseToJson(res SQSStatResponseIFace) string {
	buf, _ := json.Marshal(res)
	return bytes.NewBuffer(buf).String()
}

func NewStat(tracker *SQSJobTracker, conf *SQSDConf) *SQSStat {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/current/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, ResponseToJson(&SQSStatCurrentListResponse{
			CurrentList: tracker.CurrentSummaries(),
		}))
	})
	mux.HandleFunc("/worker/current/size", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, ResponseToJson(&SQSStatCurrentSizeResponse{
			Size: len(tracker.CurrentSummaries()),
		}))
	})
	mux.HandleFunc("/worker/pause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprint(w, "Method Not Allowed")
			return
		}
	})

	return &SQSStat{
		Port:    conf.Stat.Port,
		Tracker: tracker,
		Mux:     mux,
	}
}

func (s *SQSStat) Run(ctx context.Context) {
	http.ListenAndServe(":"+strconv.Itoa(s.Port), s.Mux)
}
