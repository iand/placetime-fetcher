package main

import (
	"flag"
	"github.com/BurntSushi/toml"
	"github.com/placetime/datastore"
	"log"
	"os"
	"os/user"
	"path"
)

type Config struct {
	Fetcher   FetcherConfig    `toml:"fetcher"`
	Image     ImageConfig      `toml:"image"`
	Datastore datastore.Config `toml:"datastore"`
}

type FetcherConfig struct {
	Workers int                `toml:"workers"`
	Feed    FetcherFeedConfig  `toml:"feed"`
	Image   FetcherImageConfig `toml:"image"`
}

type FetcherFeedConfig struct {
	Interval int `toml:"interval"`
}

type FetcherImageConfig struct {
	Interval int `toml:"interval"`
}

type ImageConfig struct {
	Path string `toml:"path"`
}

var (
	DefaultConfig Config = Config{
		Fetcher: FetcherConfig{
			Workers: 5,
			Feed: FetcherFeedConfig{
				Interval: 30,
			},
			Image: FetcherImageConfig{
				Interval: 30,
			},
		},
		Image: ImageConfig{
			Path: "/var/opt/timescroll/img",
		},
		Datastore: datastore.DefaultConfig,
	}
)

func readConfig() {
	var configFile string

	flag.StringVar(&configFile, "config", "", "configuration file to use")
	flag.BoolVar(&runOnce, "runonce", false, "run the fetcher once and then exit")
	flag.StringVar(&feedurl, "debugfeed", "", "run the fetcher on the given feed url and debug results")
	flag.Parse()

	config = DefaultConfig

	if configFile == "" {
		// Test home directory
		if u, err := user.Current(); err == nil {
			testFile := path.Join(u.HomeDir, ".placetime", "config")
			if _, err := os.Stat(testFile); err == nil {
				configFile = testFile
			}
		}
	}

	if configFile == "" {
		// Test /etc directory
		testFile := "/etc/placetime.conf"
		if _, err := os.Stat(testFile); err == nil {
			configFile = testFile
		}
	}

	if configFile != "" {
		configFile = path.Clean(configFile)
		if _, err := toml.DecodeFile(configFile, &config); err != nil {
			log.Printf("Could not read config file %s: %s", configFile, err.Error())
			os.Exit(1)
		}

		log.Printf("Reading configuration from %s", configFile)
	} else {
		log.Printf("Using default configuration")
	}

}

func checkEnvironment() {
	f, err := os.Open(config.Image.Path)
	if err != nil {
		log.Printf("Could not open image path %s: %s", config.Image.Path, err.Error())
		os.Exit(1)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Printf("Could not stat image path %s: %s", config.Image.Path, err.Error())
		os.Exit(1)
	}

	if !fi.IsDir() {
		log.Printf("Image path is not a directory %s: %s", config.Image.Path, err.Error())
		os.Exit(1)
	}

}
