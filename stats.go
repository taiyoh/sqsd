package sqsd

import (
	"github.com/fukata/golang-stats-api-handler"
	"net/http"
	"strconv"
)

type SQSStat struct {
	Port int
	Mux  *http.ServeMux
}

func NewStat(conf *SQSDConf) *SQSStat {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats_api.Handler)
	return &SQSStat{
		Port: conf.Stat.Port,
		Mux:  mux,
	}
}

func (s *SQSStat) Stop() {

}

func (s *SQSStat) Run() {
	http.ListenAndServe(":"+strconv.Itoa(s.Port), s.Mux)
}
