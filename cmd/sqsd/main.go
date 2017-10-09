package main

import (
	"fmt"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
)

var (
	version string
	commit  string
	date    string
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
	var versionFlg bool
	flag.StringVar(&confPath, "config", "config.toml", "config path")
	flag.BoolVar(&versionFlg, "version", false, "version")
	flag.Parse()

	if versionFlg {
		fmt.Printf("version: %s\ncommit: %s\nbuild date: %s\n", version, commit, date)
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

	srv := sqsd.NewStatServer(tracker, config.Stat.ServerPort)
	wg.Add(1)
	go srv.Run(ctx, wg)

	awsConf := &aws.Config{
		Region: aws.String(config.SQS.Region),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	msgHandler := sqsd.NewMessageHandler(
		sqsd.NewResource(sqs.New(sess, awsConf), config.SQS.QueueURL),
		tracker,
		config,
	)
	wg.Add(1)
	go msgHandler.Run(ctx, wg)

	wg.Wait()
	log.Println("sqsd ends.")
}
