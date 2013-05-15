package main

import (
	"crypto/md5"
	"fmt"
	"github.com/iand/feedparser"
	"github.com/iand/imgpick"
	"github.com/iand/salience"
	// "github.com/mjarco/bloom"
	"github.com/placetime/datastore"
	"image/png"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"time"
)

var (
	runOnce = false
	feedurl = ""
	config  Config
)

func main() {
	readConfig()

	if feedurl != "" {
		debugFeed(feedurl)
		return
	}

	checkEnvironment()
	datastore.InitRedisStore(config.Datastore)

	log.Printf("Images will be written to: %s", config.Image.Path)

	const bufferLength = 0

	quit := make(chan bool)

	jobs := make(chan Job, bufferLength)

	if runOnce {
		go worker(1, jobs, quit)
		pumpOnce(jobs, quit)
	} else {
		// Start workers
		log.Printf("Using %d processor cores", runtime.NumCPU())
		runtime.GOMAXPROCS(runtime.NumCPU())

		log.Printf("Starting %d workers", config.Fetcher.Workers)
		for w := 0; w < config.Fetcher.Workers; w++ {
			go worker(w, jobs, quit)
		}
		pumpContinuous(jobs, quit)
	}

	close(quit)
	log.Printf("Stopping fetcher")
}

func debugFeed(url string) {
	log.Printf("Debugging feed %s", url)
	resp, err := http.Get(url)
	log.Printf("Response: %s", resp.Status)

	if err != nil {
		log.Printf("Fetch of feed got http error  %s", err.Error())
		return
	}

	defer resp.Body.Close()

	feed, err := feedparser.NewFeed(resp.Body)

	for _, item := range feed.Items {
		fmt.Printf("--Item (%s)\n", item.Id)
		fmt.Printf("  Title: %s\n", item.Title)
		fmt.Printf("  Link:  %s\n", item.Link)
		fmt.Printf("  Image: %s\n", item.Image)

		//		s.AddItem(item.Pid, time.Unix(item.Event, 0), item.Text, item.Link, item.Image, item.Id)
	}

}

type FetchRecord struct {
	Url         string `json:"url"`
	Count       int32  `json:"count"`
	Interval    int64  `json:"interval"`
	LastFetched int64  `json:"fetched"`
	LastChanged int64  `json:"changed"`
}

func pumpContinuous(jobs chan<- Job, quit <-chan bool) {

	feedInterval := time.Duration(config.Fetcher.Feed.Interval) * time.Second
	imageInterval := time.Duration(config.Fetcher.Image.Interval) * time.Second

	log.Printf("Waiting %s seconds before fetching feeds", feedInterval)
	log.Printf("Waiting %s seconds before fetching images", imageInterval)

	feedTicker := time.NewTicker(feedInterval)
	imageTicker := time.NewTicker(imageInterval)

	for {

		select {
		case <-quit:
			return
		case <-feedTicker.C:
			pumpRssJobs(jobs)

		case <-imageTicker.C:
			pumpImageJobs(jobs)

		}

	}
}

// Execute one cycle of fetching feeds and images
func pumpOnce(jobs chan<- Job, quit <-chan bool) {
	pumpRssJobs(jobs)
	pumpImageJobs(jobs)
}

func pumpRssJobs(jobs chan<- Job) {
	s := datastore.NewRedisStore()
	defer s.Close()

	profiles, _ := s.FeedDrivenProfiles()
	if len(profiles) == 0 {
		return
	}
	for _, p := range profiles {
		log.Printf("Pumping feed for profile %s", p.Pid)
		jobs <- RssJob{Url: p.FeedUrl, Pid: p.Pid}
	}

}

func pumpImageJobs(jobs chan<- Job) {
	s := datastore.NewRedisStore()
	defer s.Close()

	for {
		items, _ := s.GrabItemsNeedingImages(10)
		if len(items) == 0 {
			return
		}
		for _, item := range items {
			jobs <- ImageJob{Url: item.Link, ItemId: item.Id}
		}
	}
}

type Job interface {
	Do()
}

func worker(id int, jobs <-chan Job, quit <-chan bool) {
	for {
		select {

		case <-quit:
			return

		case job := <-jobs:
			log.Printf("Worker %d processing job", id)
			job.Do()
		}
	}
}

type RssJob struct {
	Url string
	Pid string
}

func (job RssJob) Do() {
	log.Printf("RSS job fetching feed at %s", job.Url)
	resp, err := http.Get(job.Url)
	defer resp.Body.Close()

	if err != nil {
		log.Printf("RSS job failed to fetch feed: %s", err.Error())
		return
	}

	feed, err := feedparser.NewFeed(resp.Body)
	if err != nil {
		log.Printf("RSS job failed to parse feed: %s", err.Error())
		return
	}

	s := datastore.NewRedisStore()
	defer s.Close()

	log.Printf("RSS job found %d items in feed", len(feed.Items))

	for _, item := range feed.Items {
		hasher := md5.New()
		io.WriteString(hasher, item.Id)
		id := fmt.Sprintf("%x", hasher.Sum(nil))
		_, err := s.AddItem(job.Pid, time.Unix(0, 0), item.Title, item.Link, item.Image, id)
		if err != nil {
			log.Printf("RSS job failed to add item from feed: %s", err.Error())
		}
	}

}

type ImageJob struct {
	Url    string
	ItemId string
}

func (job ImageJob) Do() {
	log.Printf("Looking for a feature image for %s", job.Url)

	img, err := imgpick.PickImage(job.Url)

	if img == nil || err != nil {
		log.Printf("Image job failed to pick an image: %s", err.Error())
		return
	}

	imgOut := salience.Crop(img, 460, 160)

	filename := fmt.Sprintf("%s.png", job.ItemId)

	foutName := path.Join(config.Image.Path, filename)

	fout, err := os.OpenFile(foutName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Printf("Image job failed to open image file for writing: %s", err.Error())
		return
	}

	if err = png.Encode(fout, imgOut); err != nil {
		log.Printf("Image job failed to encode image as PNG: %s", err.Error())
		return
	}

	log.Printf("Image job wrote image to: %s", foutName)

	s := datastore.NewRedisStore()
	defer s.Close()

	item, err := s.Item(job.ItemId)
	if err != nil {
		log.Printf("Image job failed to get item %s from datastore: %s", job.ItemId, err.Error())
		return
	}

	item.Image = filename
	item.Media = "text"

	err = s.UpdateItem(item)
	if err != nil {
		log.Printf("Image job failed to update item %s in datastore: %s", job.ItemId, err.Error())
		return
	}

}
