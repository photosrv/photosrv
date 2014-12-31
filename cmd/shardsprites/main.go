// Command shardsprites distributes files to photostorage nodes
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rbastic/photosrv/storage"

	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/storage/replica"
	"github.com/dgryski/go-shardedkv/storage/rest"
	"github.com/dgryski/go-simpleconf"
)

// MaxReplicaFailures is how many errors we should allow during replica operations
const MaxReplicaFailures = 1

// TODO(dgryski): some of these things need to be put into a library for 'quick scripts'

type serverChooser struct {
	servers map[string][]string
	choose  shardedkv.Chooser
}

func (sc serverChooser) Choose(key string) string {
	servers := sc.servers[sc.choose.Choose(key)]
	return servers[rand.Intn(len(servers))]
}

func loadSKVStorageFromFile(configfile string, dc string, migration bool, oneNode string) (shardedkv.Storage, *serverChooser, error) {

	conf, err := simpleconf.NewFromFile(configfile)

	if err != nil {
		return nil, nil, err
	}

	var configBase string
	configBase = "photosrv>"
	if migration {
		configBase = configBase + "migration>"
	}

	chooser, shards, err := storage.LoadShards(conf, configBase, dc)

	var skvshards []shardedkv.Shard

	buckets := make(map[string][]string)

	for _, s := range shards {

		var replicas []shardedkv.Storage
		for _, endpoint := range s.Endpoints {
			if oneNode != "" && endpoint != oneNode {
				log.Println("skipping, ", endpoint, "doesn't match", oneNode)
				continue
			}
			log.Println("adding", s.Name, endpoint)
			replicas = append(replicas, rest.New(endpoint))
			buckets[s.Name] = append(buckets[s.Name], endpoint)
		}

		skvshards = append(skvshards, shardedkv.Shard{Name: s.Name, Backend: replica.New(MaxReplicaFailures, replicas...)})
	}

	skv := shardedkv.New(chooser, skvshards)

	schooser := serverChooser{
		choose:  chooser,
		servers: buckets,
	}

	return skv, &schooser, nil
}

// OutboundProxy is the proxy to connect out via for retrieving data
var OutboundProxy func(*http.Request) (*url.URL, error)

func skvworker(skv shardedkv.Storage, checkExists bool, chooser *serverChooser, urlch <-chan string, donech chan<- struct{}) {

	var client http.Client

	if OutboundProxy != nil {
		client.Transport = &http.Transport{Proxy: OutboundProxy}
	}

	for u := range urlch {

		parsed, err := url.Parse(u)
		if err != nil {
			log.Println("failed to parse", u, ":", err)
			continue
		}

		key := parsed.Path[1:]

		if checkExists {

			svku := chooser.Choose(key)

			r, err := client.Head(svku + "/" + key)
			if err != nil {
				log.Println("Failed to HEAD: ", svku, key, err)
				continue
			}
			// we only care about the status -- ignore the body
			r.Body.Close()

			if r.StatusCode == 200 {
				// file already there
				continue
			}
		}

		resp, err := client.Get(u)
		if err != nil {
			log.Println("failed to get", u, ":", err)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Println("failed to read body for", u, ":", err)
			continue
		}

		if resp.StatusCode == 404 {
			// ignore, no logging
			continue
		}

		if resp.StatusCode != 200 {
			log.Println("failed 200 status for get", u, ":", resp.StatusCode)
			fmt.Println(u)
			continue
		}

		err = skv.Set(key, body)
		if err != nil {
			log.Println("failed during set for", u, ":", err)
			fmt.Println(u)
			continue
		}
	}
	donech <- struct{}{}
}

func skvlocal(skv shardedkv.Storage, chooser *serverChooser, urlch <-chan string, donech chan<- struct{}) {

	var client http.Client

	if OutboundProxy != nil {
		client.Transport = &http.Transport{Proxy: OutboundProxy}
	}

	for u := range urlch {
		key := strings.TrimPrefix(u, "./")
		key = strings.Replace(key, "//", "/", -1)

		body, err := ioutil.ReadFile(u)
		if err != nil {
			log.Println("failed to read body for", u, ":", err)
			continue
		}

		err = skv.Set(key, body)
		if err != nil {
			log.Println("failed during set for", u, ":", err)
			fmt.Println(u)
			continue
		}
	}
	donech <- struct{}{}
}

func skvdelete(skv shardedkv.Storage, chooser *serverChooser, urlch <-chan string, donech chan<- struct{}) {

	for u := range urlch {
		key := u

		//	log.Println("deleting key=", key, "from", chooser.Choose(key))

		_, err := skv.Delete(key)
		if err != nil {
			log.Println("failed during delete for", u, ":", err)
			fmt.Println(u)
			continue
		}
	}
	donech <- struct{}{}
}

func skvdellocal(shard string, chooser *serverChooser, yesReallyIMeanIt bool, urlch <-chan string, donech chan<- struct{}) {

	for u := range urlch {
		key := u

		if chooser.choose.Choose(key) == shard {
			continue
		}

		if yesReallyIMeanIt {
			os.Remove(u)
		} else {
			fmt.Println("rm", u)
		}
	}
	donech <- struct{}{}
}

func main() {
	cfgfname := flag.String("c", "config/photosrv.cfg", "config file to load")
	workers := flag.Int("w", 4, "number of workers")
	inpfname := flag.String("f", "", "input file name")
	httpProxy := flag.String("proxy", "", "http proxy to connect through for retrieving URLs")
	checkExists := flag.Bool("check", false, "check if image exists on photostorage first (HEAD)")
	totalItems := flag.Int("total", 0, "total items (for streaming input)")
	delFiles := flag.Bool("delete", false, "delete files from shards")
	localFiles := flag.Bool("local", false, "send local files to shards")
	delLocal := flag.String("dellocal", "", "delete local files that don't belong on shard")
	yesReallyIMeanIt := flag.Bool("yesreallyimeanit", false, "yes, really delete files")
	migration := flag.Bool("migration", false, "use the migration continuum")
	dc := flag.String("dc", "", "DC to distribute to")
	oneNode := flag.String("onenode", "", "a single node to sync data to")

	// TODO(rbastic): check for 'http://' in oneNode, add it if it's missing?

	flag.Parse()

	skv, choose, err := loadSKVStorageFromFile(*cfgfname, *dc, *migration, *oneNode)
	if err != nil {
		log.Fatal("error loading skv:", err)
	}

	if *httpProxy != "" {
		proxyURL, err := url.Parse(*httpProxy)
		if err != nil {
			log.Fatalf("failed to parse proxy %s: %s\n", *httpProxy, err)
		}
		OutboundProxy = http.ProxyURL(proxyURL)
	}

	urlch := make(chan string)
	donech := make(chan struct{})

	log.Println("using", *workers, "http workers")

	switch {
	case *delFiles:
		for i := 0; i < *workers; i++ {
			go skvdelete(skv, choose, urlch, donech)
		}
	case *localFiles:
		for i := 0; i < *workers; i++ {
			go skvlocal(skv, choose, urlch, donech)
		}
	case *delLocal != "":
		log.Printf("dellocal workers started: yesReallyIMeanIt=%v", *yesReallyIMeanIt)
		for i := 0; i < *workers; i++ {
			go skvdellocal(*delLocal, choose, *yesReallyIMeanIt && os.Getenv("YES_REALLY_I_MEAN_IT") != "", urlch, donech)
		}

	default:
		for i := 0; i < *workers; i++ {
			go skvworker(skv, *checkExists, choose, urlch, donech)
		}
	}

	var f io.ReadCloser
	if *inpfname == "" {
		f = os.Stdin
	} else {
		f, err = os.Open(*inpfname)

		if err != nil {
			log.Fatal("error loading input file", *inpfname, ":", err)
		}
		defer f.Close()
	}

	itemch := make(chan string)

	if *totalItems == 0 {
		scan := bufio.NewScanner(f)
		var items []string
		for scan.Scan() {
			items = append(items, scan.Text())
		}

		*totalItems = len(items)
		log.Println("Slurped total items:", len(items))

		go func() {
			for _, v := range items {
				itemch <- v
			}
			close(itemch)
		}()

	} else {

		log.Println("Streaming total items:", *totalItems)
		go func() {
			scan := bufio.NewScanner(f)
			for scan.Scan() {
				itemch <- scan.Text()
			}
			close(itemch)
		}()

	}

	alpha := 0.25
	var timePerBlockEstimate time.Duration
	blockSize := 10000
	blockStartTime := time.Now()

	processedItems := 0

	// send off the work
	for item := range itemch {
		urlch <- item
		processedItems++
		if processedItems%blockSize == 0 {
			log.Printf("Processed %d/%d (%02d %%)", processedItems, *totalItems, int(100*float64(processedItems)/float64(*totalItems)))

			lastBlockTime := time.Since(blockStartTime)
			timePerBlockEstimate = time.Duration(alpha*float64(lastBlockTime) + (1-alpha)*float64(timePerBlockEstimate))

			remainingBlocks := float64(*totalItems-processedItems) / float64(blockSize)
			remainingTime := time.Duration(int(remainingBlocks)) * timePerBlockEstimate

			log.Println("ETA: ", time.Duration(remainingTime.Seconds())*time.Second, "(", time.Now().Add(remainingTime).Format(time.Stamp), ")")
			blockStartTime = time.Now()
		}

	}

	close(urlch)

	log.Println("waiting for workers to complete")
	// wait for everybody to complete
	for i := 0; i < *workers; i++ {
		<-donech
	}
}
