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
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
	"golang.org/x/sync/errgroup"
)

var (
	commit string
	date   string
)

func waitSignal(cancel context.CancelFunc, logger sqsd.Logger) error {
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGTERM,
		syscall.SIGINT,
		os.Interrupt)
	defer signal.Stop(sigCh)

	switch <-sigCh {
	case syscall.SIGTERM:
		logger.Info("SIGTERM caught. shutdown process...")
	case syscall.SIGINT:
		logger.Info("SIGINT caught. shutdown process...")
	case os.Interrupt:
		logger.Info("os.Interrupt caught. shutdown process...")
	}
	return nil
}

func runStatServer(ctx context.Context, tr *sqsd.QueueTracker, logger sqsd.Logger, port int) error {
	handler := sqsd.NewStatHandler(tr)

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: handler.BuildServeMux(),
	}

	logger.Info("stat server start.")
	defer logger.Info("stat server stop.")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(ctx)
	})

	if err := eg.Wait(); err != nil {
		logger.Infof("stat server err: %v", err)
		return err
	}

	return nil
}

func newSQSAPI(conf sqsd.SQSConf) *sqs.SQS {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(conf.Region),
		EndpointResolver: endpoints.ResolverFunc(func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == endpoints.SqsServiceID && conf.URL != "" {
				uri, _ := url.ParseRequestURI(conf.URL)
				return endpoints.ResolvedEndpoint{
					URL: fmt.Sprintf("%s://%s", uri.Scheme, uri.Host),
				}, nil
			}
			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
		}),
	}))
	return sqs.New(sess)
}

func initializeApp() *sqsd.Conf {
	var confPath string
	var versionFlg bool
	flag.StringVar(&confPath, "config", "config.toml", "config path")
	flag.BoolVar(&versionFlg, "version", false, "version")
	flag.Parse()

	if versionFlg {
		fmt.Printf("version: %s\ncommit: %s\nbuild date: %s\n", sqsd.GetVersion(), commit, date)
		os.Exit(0)
		return nil
	}

	if !filepath.IsAbs(confPath) {
		d, _ := os.Getwd()
		confPath = filepath.Join(d, confPath)
	}
	config, err := sqsd.NewConf(confPath)
	if err != nil {
		log.Fatalf("config file: %s, err: %s\n", confPath, err)
	}

	return config
}

func main() {
	config := initializeApp()

	logger := sqsd.NewLogger(config.Main.LogLevel)

	tracker := sqsd.NewQueueTracker(config.Worker.MaxProcessCount, logger)
	if !tracker.HealthCheck(config.Worker.Healthcheck) {
		logger.Error("healthcheck failed.")
		return
	}

	defer logger.Info("sqsd ends.")

	sqsAPI := newSQSAPI(config.SQS)

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return waitSignal(cancel, logger)
	})
	eg.Go(func() error {
		return runStatServer(ctx, tracker, logger, config.Main.StatServerPort)
	})
	eg.Go(func() error {
		return sqsd.RunProducerAndConsumer(
			ctx,
			sqsAPI,
			tracker,
			sqsd.NewHTTPInvoker(config.Worker.URL),
			config.SQS,
		)
	})

	if err := eg.Wait(); err != nil {
		logger.Infof("process error: %v", err)
	}
}
