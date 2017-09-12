package sqsd

import (
	"net/http"
	"github.com/fukata/golang-stats-api-handler"
	"strconv"
)

type SQSStat struct {
	Port int
}

func NewStat(conf *SQSDConf) *SQSStat {
	s := &SQSStat{conf.Stat.Port}
	http.HandleFunc("/stats", stats_api.Handler)
	return s
}

func (s *SQSStat) Stop() {

}

func (s *SQSStat) Run() {
	http.ListenAndServe(":" + strconv.Itoa(s.Port), nil)
}