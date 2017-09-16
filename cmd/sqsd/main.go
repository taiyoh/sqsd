package main

import (
	"context"
	"os"
	"path/filepath"
	"log"
	"sqsd"
)

func main() {
	d, _ := os.Getwd()
	config, err := sqsd.NewConf(filepath.Join(d, "config.toml"))
	if err != nil {
		log.Fatalf("config file not loaded. %s, err: %s\n", filepath.Join(d, "config.toml"), err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := sqsd.NewJobTracker(config.ProcessCount)

	stat := sqsd.NewStat(tracker, config)
	go stat.Run(ctx)

	worker := sqsd.NewWorker(tracker, config)
	go worker.Run(ctx)
}
