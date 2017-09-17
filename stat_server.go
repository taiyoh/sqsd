package sqsd

import (
	"context"
	"github.com/fukata/golang-stats-api-handler"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type SQSStatServer struct {
	Port int
	Mux  *http.ServeMux
}

func NewStatServer(handler *SQSStatHandler, conf *SQSDConf) *SQSStatServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats_api.Handler)
	mux.HandleFunc("/worker/current/list", handler.WorkerCurrentListHandler())
	mux.HandleFunc("/worker/pause", handler.WorkerPauseHandler())
	mux.HandleFunc("/worker/resume", handler.WorkerResumeHandler())
	return &SQSStatServer{
		Port: conf.Stat.Port,
		Mux:  mux,
	}
}

func (s *SQSStatServer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("stat server start.")
	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(s.Port),
		Handler: s.Mux,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("stat server closed.")
}
