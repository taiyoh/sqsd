package sqsd

import (
	"github.com/fukata/golang-stats-api-handler"
	"net/http"
	"strconv"
)

type SQSStat struct {
	Port    int
	Tracker *SQSJobTracker
	Mux     *http.ServeMux
}

func NewStat(tracker *SQSJobTracker, conf *SQSDConf) *SQSStat {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats_api.Handler)
	return &SQSStat{
		Port:    conf.Stat.Port,
		Tracker: tracker,
		Mux:     mux,
	}
}

func (s *SQSStat) Run() {
	http.ListenAndServe(":"+strconv.Itoa(s.Port), s.Mux)
}
