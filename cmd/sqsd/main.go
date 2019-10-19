package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
)

var (
	commit string
	date   string
)

func waitSignal(cancel context.CancelFunc, logger sqsd.Logger, wg *sync.WaitGroup) {
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
				logger.Info("SIGTERM caught. shutdown process...")
				return
			case syscall.SIGINT:
				logger.Info("SIGINT caught. shutdown process...")
				return
			case os.Interrupt:
				logger.Info("os.Interrupt caught. shutdown process...")
				return
			}
		}
	}
}

func runStatServer(ctx context.Context, tr *sqsd.QueueTracker, logger sqsd.Logger, port int, wg *sync.WaitGroup) {
	defer wg.Done()

	handler := &sqsd.StatHandler{Tracker: tr}

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: handler.BuildServeMux(),
	}

	logger.Info("stat server start.")

	syncWait := &sync.WaitGroup{}
	syncWait.Add(2)
	go func() {
		defer syncWait.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error(err.Error())
			return
		}
	}()
	go func() {
		defer syncWait.Done()
		<-ctx.Done()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error(err.Error())
			return
		}
	}()
	syncWait.Wait()

	logger.Info("stat server stop.")
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
	if !tracker.HealthCheck(config.Worker.Healthcheck) {
		logger.Error("healthcheck failed.")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(config.SQS.Region),
		EndpointResolver: endpoints.ResolverFunc(func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.SqsServiceID && config.SQS.URL != "" {
				uri, _ := url.ParseRequestURI(config.SQS.URL)
				return endpoints.ResolvedEndpoint{
					URL: fmt.Sprintf("%s://%s", uri.Scheme, uri.Host),
				}, nil
			}
			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}),
	}))
	resource := sqsd.NewResource(sqs.New(sess), config.SQS)

	msgConsumer := sqsd.NewMessageConsumer(resource, tracker, config.Worker.URL)
	msgProducer := sqsd.NewMessageProducer(resource, tracker, config.SQS.Concurrency)

	wg := &sync.WaitGroup{}

	wg.Add(4)
	go waitSignal(cancel, logger, wg)
	go runStatServer(ctx, tracker, logger, config.Main.StatServerPort, wg)
	go msgConsumer.Run(ctx, wg)
	go msgProducer.Run(ctx, wg)

	wg.Wait()
	logger.Info("sqsd ends.")
}
