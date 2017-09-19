package main

import (
	"context"
	"flag"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

func waitSignal(cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGTERM,
		syscall.SIGINT,
		os.Interrupt)
	defer signal.Stop(sigCh)

	for {
		select {
		case sig := <-sigCh:
			switch sig {
			case syscall.SIGTERM:
				log.Println("SIGTERM caught. shutdown process...")
				return
			case syscall.SIGINT:
				log.Println("SIGINT caught. shutdown process...")
				return
			case os.Interrupt:
				log.Println("os.Interrupt caught. shutdown process...")
				return
			}
		}
	}
}

func main() {
	var confPath string
	flag.StringVar(&confPath, "config", "config.toml", "config path")
	if !filepath.IsAbs(confPath) {
		d, _ := os.Getwd()
		confPath = filepath.Join(d, confPath)
	}
	config, err := sqsd.NewConf(confPath)
	if err != nil {
		log.Fatalf("config file: %s, err: %s\n", confPath, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go waitSignal(cancel, wg)

	tracker := sqsd.NewJobTracker(config.Worker.MaxProcessCount)

	srv := sqsd.NewStatServer(tracker, config.Stat.ServerPort)
	wg.Add(1)
	go srv.Run(ctx, wg)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	worker := sqsd.NewWorker(
		&sqsd.Resource{Client: sqs.New(sess), URL: config.SQS.QueueURL},
		tracker,
		config,
	)
	wg.Add(1)
	go worker.Run(ctx, wg)

	wg.Wait()
	log.Println("sqsd ends.")
}
