package main

import (
	"sync"
	"syscall"
	"os/signal"
	"context"
	"os"
	"path/filepath"
	"log"
	"github.com/taiyoh/sqsd"
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
			switch(sig) {
			case syscall.SIGTERM:
				log.Println("SIGTERM caught. shutdown process...")
				cancel()
				return
			case syscall.SIGINT:
				log.Println("SIGINT caught. shutdown process...")
				cancel()
				return
			case os.Interrupt:
				log.Println("os.Interrupt caught. shutdown process...")
				cancel()
				return
			}
		}
	}
}

func main() {
	d, _ := os.Getwd()
	config, err := sqsd.NewConf(filepath.Join(d, "config.toml"))
	if err != nil {
		log.Fatalf("config file not loaded. %s, err: %s\n", filepath.Join(d, "config.toml"), err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go waitSignal(cancel, wg)

	tracker := sqsd.NewJobTracker(config.ProcessCount)

	stat := sqsd.NewStat(tracker, config)
	wg.Add(1)
	go stat.Run(ctx, wg)

	worker := sqsd.NewWorker(tracker, config)
	wg.Add(1)
	go worker.Run(ctx, wg)

	wg.Wait()
	log.Println("sqsd ends.")
}
