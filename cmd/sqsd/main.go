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

func RunStatServer(tr *sqsd.JobTracker, port int, ctx context.Context, wg *sync.WaitGroup) {
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

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go waitSignal(cancel, wg)

	tracker := sqsd.NewJobTracker(config.Worker.MaxProcessCount)

	wg.Add(1)
	go RunStatServer(tracker, config.Stat.ServerPort, ctx, wg)

	awsConf := &aws.Config{
		Region: aws.String(config.SQS.Region),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	resource := sqsd.NewResource(sqs.New(sess, awsConf), config.SQS.QueueURL)

	jobHandler := sqsd.NewJobHandler(resource, tracker)
	wg.Add(1)
	go jobHandler.RunEventListener(ctx, wg)

	msgReceiver := sqsd.NewMessageReceiver(
		resource,
		tracker,
		config,
	)
	wg.Add(1)
	go msgReceiver.Run(ctx, wg)

	wg.Wait()
	log.Println("sqsd ends.")
}
