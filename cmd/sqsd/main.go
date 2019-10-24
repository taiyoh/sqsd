package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/taiyoh/sqsd"
	"golang.org/x/sync/errgroup"
)

var (
	commit string
	date   string
)

func waitSignal(ctx context.Context, logger sqsd.Logger) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGTERM,
		syscall.SIGINT,
		os.Interrupt)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case sig := <-sigCh:
			switch sig {
			case syscall.SIGTERM:
				logger.Info("SIGTERM caught. shutdown process...")
			case syscall.SIGINT:
				logger.Info("SIGINT caught. shutdown process...")
			case os.Interrupt:
				logger.Info("os.Interrupt caught. shutdown process...")
			}
			return context.Canceled
		}
	}
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

	eg, ctx := errgroup.WithContext(context.Background())

	eg.Go(func() error {
		return waitSignal(ctx, logger)
	})
	eg.Go(func() error {
		return sqsd.RunStatServer(ctx, tracker, config.Main.StatServerPort)
	})
	eg.Go(func() error {
		invoker := sqsd.NewHTTPInvoker(config.Worker.URL)
		return sqsd.RunProducerAndConsumer(ctx, tracker, invoker, config.SQS)
	})

	if err := eg.Wait(); err != nil && err != context.Canceled {
		logger.Infof("process error: %v", err)
	}
}
