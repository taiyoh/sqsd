package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
)

var (
	commit string
	date   string
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

func RunStatServer(tr *sqsd.QueueTracker, port int, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	handler := &sqsd.StatHandler{Tracker: tr}

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: handler.BuildServeMux(),
	}

	syncWait := &sync.WaitGroup{}
	syncWait.Add(1)
	go func() {
		defer syncWait.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	syncWait.Add(1)
	go func() {
		defer syncWait.Done()
		for {
			select {
			case <-ctx.Done():
				if err := srv.Shutdown(ctx); err != nil {
					log.Fatal(err)
				}
				return
			}
		}
	}()
	syncWait.Wait()

	log.Println("stat server closed.")

}

func main() {
	var confPath string
	var versionFlg bool
	flag.StringVar(&confPath, "config", "config.toml", "config path")
	flag.BoolVar(&versionFlg, "version", false, "version")
	flag.Parse()

	if versionFlg {
		fmt.Printf("version: %s\ncommit: %s\nbuild date: %s\n", sqsd.GetVersion(), commit, date)
		return
	}

	if !filepath.IsAbs(confPath) {
		d, _ := os.Getwd()
		confPath = filepath.Join(d, confPath)
	}
	config, err := sqsd.NewConf(confPath)
	if err != nil {
		log.Fatalf("config file: %s, err: %s\n", confPath, err)
	}

	logger := sqsd.NewLogger(config.Main.LogLevel)

	tracker := sqsd.NewQueueTracker(config.Worker.MaxProcessCount, logger)
	if !tracker.HealthCheck(config.Worker) {
		logger.Error("healthcheck failed.")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(2)
	go waitSignal(cancel, wg)
	go RunStatServer(tracker, config.Main.StatServerPort, ctx, wg)

	awsConf := &aws.Config{
		Region: aws.String(config.SQS.Region),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	resource := sqsd.NewResource(sqs.New(sess, awsConf), config.SQS.QueueURL())

	msgConsumer := sqsd.NewMessageConsumer(resource, tracker, config.Worker.WorkerURL)
	msgProducer := sqsd.NewMessageProducer(resource, tracker, config.SQS.Concurrency)
	wg.Add(2)
	go msgConsumer.Run(ctx, wg)
	go msgProducer.Run(ctx, wg)

	wg.Wait()
	logger.Info("sqsd ends.")
}
