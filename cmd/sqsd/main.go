package main

import (
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

//	stat := NewStat(config)
//	defer stat.Stop()
//	go stat.Run()

	worker := sqsd.NewWorker(config)
	defer worker.Stop()
	go worker.Run()
}