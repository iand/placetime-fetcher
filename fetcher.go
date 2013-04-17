package main

import (
	"crypto/md5"
	"fmt"
	"github.com/iand/feedparser"
	"github.com/iand/imgpick"
	"github.com/iand/salience"
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
	imgDir = "/var/opt/timescroll/img"
)

func main() {
	ticker := time.Tick(30 * time.Minute)
	for _ = range ticker {
		pollFeeds()
		pollImages()
	}

}

func pollFeeds() {
	log.Print("Refreshing feeds")
	s := datastore.NewRedisStore()
	defer s.Close()

	profiles, _ := s.FeedDrivenProfiles()

	jobs := make(chan *datastore.Profile, len(profiles))
	results := make(chan *ProfileItemData, len(profiles))

	for w := 0; w < 3; w++ {
		go feedWorker(w, jobs, results)
	}

	for _, p := range profiles {
		jobs <- p
	}
	close(jobs)

	for i := 0; i < len(profiles); i++ {
		data := <-results
		if data.Error != nil {
			log.Printf("Error processing feed for %s: %v", data.Profile.Pid, data.Error)
		} else {
			log.Printf("Found %d items in feed for %s", len(data.Items), data.Profile.Pid)
		}

		updateProfileItemData(data)
		runtime.Gosched()
	}

}

func pollImages() {
	log.Print("Fetching images")
	s := datastore.NewRedisStore()
	defer s.Close()

	items, _ := s.GrabItemsNeedingImages(30)
	log.Printf("%d images need to be fetched", len(items))
	if len(items) > 0 {
		jobs := make(chan *datastore.Item, len(items))
		results := make(chan *ItemImageData, len(items))

		for w := 0; w < 3; w++ {
			go imageWorker(w, jobs, results)
		}

		for _, p := range items {
			jobs <- p
		}
		close(jobs)

		for i := 0; i < len(items); i++ {
			data := <-results
			if data.Error != nil {
				log.Printf("Error processing images for %s: %v", data.Item.Id, data.Error)
			} else {
				log.Printf("Found image %s for %s", data.Item.Image, data.Item.Id)
			}

			s.UpdateItem(data.Item)
			runtime.Gosched()
		}
	}
}

type ProfileItemData struct {
	Profile *datastore.Profile
	Items   []*datastore.Item
	Error   error
}

type ItemImageData struct {
	Item  *datastore.Item
	Error error
}

func feedWorker(id int, jobs <-chan *datastore.Profile, results chan<- *ProfileItemData) {
	for p := range jobs {
		log.Printf("Feed worker %d processing feed %s", id, p.FeedUrl)

		resp, err := http.Get(p.FeedUrl)

		if err != nil {
			log.Printf("Feed worker %d got http error  %s", id, err.Error())
			results <- &ProfileItemData{p, nil, err}
			continue
		}
		defer resp.Body.Close()

		feed, err := feedparser.NewFeed(resp.Body)

		results <- &ProfileItemData{p, itemsFromFeed(p.Pid, feed), err}
	}
}

func itemsFromFeed(pid string, feed *feedparser.Feed) []*datastore.Item {

	items := make([]*datastore.Item, 0)
	if feed != nil {
		for _, item := range feed.Items {
			hasher := md5.New()
			io.WriteString(hasher, item.Id)
			id := fmt.Sprintf("%x", hasher.Sum(nil))
			items = append(items, &datastore.Item{Id: id, Pid: pid, Event: item.When.Unix(), Text: item.Title, Link: item.Link})
		}
	}
	return items
}

func imageWorker(id int, jobs <-chan *datastore.Item, results chan<- *ItemImageData) {

	for item := range jobs {
		log.Printf("Image worker %d processing item %s", id, item.Id)
		img, err := imgpick.PickImage(item.Link)

		if img == nil || err != nil {
			results <- &ItemImageData{item, err}
			continue
		}

		imgOut := salience.Crop(img, 460, 160)

		filename := fmt.Sprintf("%s.png", item.Id)

		foutName := path.Join(imgDir, filename)

		fout, err := os.OpenFile(foutName, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			results <- &ItemImageData{item, err}
			continue
		}

		if err = png.Encode(fout, imgOut); err != nil {
			results <- &ItemImageData{item, err}
			continue
		}

		item.Image = filename

		results <- &ItemImageData{item, err}

	}
}

func updateProfileItemData(data *ProfileItemData) error {
	if data.Items != nil {
		s := datastore.NewRedisStore()
		defer s.Close()

		p := data.Profile

		followers, err := s.Followers(p.Pid, p.FollowerCount, 0)
		if err != nil {
			return err
		}

		for _, f := range followers {
			s.Unfollow(f.Pid, p.Pid)
		}

		//s.DeleteMaybeItems(p.Pid)
		for _, item := range data.Items {
			s.AddItem(item.Pid, time.Unix(item.Event, 0), item.Text, item.Link, item.Image, item.Id)
		}

		for _, f := range followers {
			s.Follow(f.Pid, p.Pid)
		}

	}

	return nil
}
