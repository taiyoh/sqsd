package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var path string
	var port, waitms int
	var dumpBody bool

	flag.StringVar(&path, "path", "/", "endpoint path")
	flag.IntVar(&port, "port", 8000, "server port")
	flag.IntVar(&waitms, "waitms", 0, "response wait milliseconds")
	flag.BoolVar(&dumpBody, "dump-body", false, "show request body")
	flag.Parse()

	mux := &http.ServeMux{}
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if dumpBody {
			buf := bytes.NewBuffer([]byte{})
			_, _ = buf.ReadFrom(r.Body)
			log.Printf("body: %s", buf.String())
		}
		time.Sleep(time.Duration(waitms) * time.Millisecond)
		w.WriteHeader(200)
		w.Header().Add("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	})

	srv := &http.Server{
		Handler: mux,
		Addr:    fmt.Sprintf(":%d", port),
	}

	log.Print("start dummyhttpd")

	go func() {
		switch err := srv.ListenAndServe(); err {
		case nil, http.ErrServerClosed:
			log.Printf("dummyhttpd closed")
		default:
			panic(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Print("stop dummyhttpd")
	_ = srv.Shutdown(ctx)
}
