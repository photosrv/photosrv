// Package photosrv is a sharding http reverse proxy library supporting CHash and JumpHash
// (see github.com/dgryski/go-shardedkv/). See the README for more details and examples.
package photosrv

import (
	"bufio"
	"bytes"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-simpleconf"
	"github.com/facebookgo/grace/gracehttp"
	"github.com/golang/glog"
	"github.com/peterbourgon/g2g"
	"github.com/photosrv/photosrv/storage"
)

// Config is the current photosrv configuration
type Config struct {
	chooser  shardedkv.Chooser
	replicas map[string][]*httputil.ReverseProxy
	rewrites map[string]string

	mchooser  shardedkv.Chooser
	mreplicas map[string][]*httputil.ReverseProxy
}

var config unsafe.Pointer // actual type is *Config
// CurrentConfig atomically returns the current configuration
func CurrentConfig() *Config { return (*Config)(atomic.LoadPointer(&config)) }

// UpdateConfig atomically swaps the current configuration
func UpdateConfig(cfg *Config) { atomic.StorePointer(&config, unsafe.Pointer(cfg)) }

// accumulator wraps all the counters used for stats/debug
var accumulator = struct {
	Requests         *expvar.Int
	Errors           *expvar.Int
	MultiHeadTimeout *expvar.Int
}{
	Requests:         expvar.NewInt("Requests"),
	Errors:           expvar.NewInt("Errors"),
	MultiHeadTimeout: expvar.NewInt("MultiHeadTimeout"),
}

// BuildVersion is the version of this binary
var BuildVersion = "(development build)"

var headTimeoutMs = 0

func loadRosterFile(f io.Reader) map[string]bool {
	scanner := bufio.NewScanner(f)

	hosts := make(map[string]bool)

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) > 1 {
			hosts[string(fields[1])] = true
		}
	}

	return hosts
}

// parses the photosrv>prefix_rewrite subsection of config
func parseRewrites(conf simpleconf.Config) map[string]string {
	rewrites := make(map[string]string)

	simpleconf.UnmarshalConfig(conf, "photosrv>prefix_rewrite", &rewrites)
	badRule := []string{}
	for prefix, replace := range rewrites {
		if len(prefix) == 0 || len(replace) == 0 {
			badRule = append(badRule, prefix)
			glog.Infoln("warning: rewrite ( %v => %v ) ignored. No replacement. (Rule has not replacement or rule is specified twice)", prefix, replace)
			continue
		}
		if !strings.HasPrefix(prefix, "/") {
			badRule = append(badRule, prefix)
			glog.Infoln("warning: rewrite ( %v => %v ) ignored. Rewrite rule must start with '/'.", prefix, replace)
			continue
		}
		if !strings.HasPrefix(replace, "/") {
			badRule = append(badRule, prefix)
			glog.Infoln("warning: rewrite ( %v => %v ) ignored. Rewrite replacement not starting with '/' is considered dangerous and will be ignored.", prefix, replace)
		}
		if prefix[len(prefix)-1] != replace[len(replace)-1] && (strings.HasSuffix(prefix, "/") || strings.HasSuffix(replace, "/")) {
			badRule = append(badRule, prefix)
			glog.Infoln("warning: rewrite ( %v => %v ) ignored. If one ends in '/' then both must.", prefix, replace)
		}
	}
	for _, bad := range badRule {
		delete(rewrites, bad)
	}

	return rewrites
}

// parses the configuration and constructs the list of reverse proxies to be used
func loadConfig(conf simpleconf.Config, dcname string, rosterFile string) (*Config, error) {
	// TODO(dgryski): this should have tests

	var validHosts map[string]bool

	if rosterFile != "" {
		f, err := os.Open(rosterFile)
		if err != nil {
			glog.Infoln("error loading roster file: ", err)
			return nil, err
		}

		validHosts = loadRosterFile(f)

		if len(validHosts) == 0 {
			glog.Infoln("roster file empty -- ignoring")
			validHosts = nil
		}
	}

	chooser, replicas := loadShards(conf, "photosrv>", dcname, rosterFile, validHosts)
	mchooser, mreplicas := loadShards(conf, "photosrv>migration>", dcname, rosterFile, validHosts)

	rewrites := parseRewrites(conf)

	return &Config{chooser: chooser, replicas: replicas, rewrites: rewrites, mchooser: mchooser, mreplicas: mreplicas}, nil
}

func loadShards(conf simpleconf.Config, configBase string, dcname string, rosterFile string, validHosts map[string]bool) (shardedkv.Chooser, map[string][]*httputil.ReverseProxy) {

	chooser, shards, err := storage.LoadShards(conf, configBase, dcname)

	if err != nil {
		glog.Fatalf("error loading shards: %v", err)
	}

	if len(shards) == 0 {
		return nil, nil
	}

	replicas := make(map[string][]*httputil.ReverseProxy)

	for _, shard := range shards {

		var proxies []*httputil.ReverseProxy
		for _, endpoint := range shard.Endpoints {
			u, _ := url.Parse(endpoint)

			if validHosts != nil && !validHosts[u.Host] {
				glog.Infoln("skipping host", u.Host, "not present in", rosterFile)
				continue
			}

			// glog.Infoln("adding shard", name, "with endpoint", endpoint)
			proxies = append(proxies, httputil.NewSingleHostReverseProxy(u))
		}

		if len(proxies) == 0 {
			glog.Infoln("warning: no (valid) endpoints found for shard", shard.Name)
		}

		replicas[shard.Name] = proxies
	}

	return chooser, replicas
}

func rewriteURL(path string, rewrites map[string]string) string {

	for prefix, replace := range rewrites {
		if strings.HasPrefix(path, prefix) {
			path = fmt.Sprintf("%s%s", replace, strings.TrimPrefix(path, prefix))
		}
	}

	return path
}

func normalizeURL(path string) string {

	// unescape if needed
	if idx := strings.Index(path, "%"); idx != -1 {
		var err error
		path, err = url.QueryUnescape(path)
		if err != nil {
			return "/"
		}
	}

	// any path containing "/." is a 404. "/./", "/../", and "/.foo.jpg" are all invalid
	if idx := strings.Index(path, "/."); idx != -1 {
		return "/"
	}

	// trim everything after ?
	if idx := strings.Index(path, "?"); idx != -1 {
		path = path[:idx]
	}

	// change "//" -> "/"
	path = strings.Replace(path, "//", "/", -1)

	// prefix rewrites for symlink-like use
	cfg := CurrentConfig()
	if cfg != nil {
		path = rewriteURL(path, cfg.rewrites)
	}

	return path
}

func randProxy(proxies []*httputil.ReverseProxy) *httputil.ReverseProxy {
	return proxies[rand.Intn(len(proxies))]
}

func singleHead(p *httputil.ReverseProxy, url string, proxych chan<- *httputil.ReverseProxy) {
	r, _ /* ignored */ := http.NewRequest("HEAD", url, nil)
	p.Director(r)
	response, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		proxych <- nil
		return
	}
	if response.StatusCode >= 400 {
		proxych <- nil
		response.Body.Close()
		return
	}
	proxych <- p
	response.Body.Close()
}

// Hash our incoming request and issue parallel multi-HEAD to known available
// nodes with a reasonable timeout; photosrvs are expected to live close to (in
// large clusters) or alongside your photostorage nodes.
func shardedkvDirector(req *http.Request) {

	// TODO(dgryski): this needs metrics tracked(?)

	req.URL.Path = normalizeURL(req.URL.Path)

	p := req.URL.Path[1:]

	cfg := CurrentConfig()
	if cfg == nil {
		panic("cfg was nil!")
	}
	if cfg.chooser == nil {
		panic("chooser was nil")
	}
	// Figure out what replicas we should query based on our migration state
	mstate := migrationState(atomic.LoadUint64(&currentMigrationState))

	var replicas []*httputil.ReverseProxy

	switch mstate {
	case msOldConfig:
		bucket := cfg.chooser.Choose(p)
		replicas = cfg.replicas[bucket]
	case msNewConfig:
		bucket := cfg.mchooser.Choose(p)
		replicas = cfg.mreplicas[bucket]
	case msBothConfigs:
		bucket := cfg.chooser.Choose(p)
		replicas = append(replicas, cfg.replicas[bucket]...)

		bucket = cfg.mchooser.Choose(p)
		replicas = append(replicas, cfg.mreplicas[bucket]...)
	}

	if len(replicas) == 0 {
		// no hosts for this shard :(
		// force  404 from RoundTrip
		req.URL.Path = "/"
		return
	}

	proxych := make(chan *httputil.ReverseProxy, len(replicas))
	var responseProxies []*httputil.ReverseProxy

	if headTimeoutMs > 0 {
		url := "http://localhost/" + p
		for _, r := range replicas {
			go singleHead(r, url, proxych)
		}

		// TODO(rbastic): ask metadata server where the files are kept
		timeout := time.After(time.Duration(headTimeoutMs) * time.Millisecond)
	gather:
		for i := 0; i < len(replicas); i++ {
			select {
			case proxy := <-proxych:
				if proxy != nil {
					responseProxies = append(responseProxies, proxy)
				}
			case <-timeout:
				accumulator.MultiHeadTimeout.Add(1)
				break gather
			}
		}
	}

	var proxy *httputil.ReverseProxy
	if len(responseProxies) > 0 {
		proxy = randProxy(responseProxies)
	} else {
		proxy = randProxy(replicas)
	}

	proxy.Director(req)
}

func httpResponse(code int) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       ioutil.NopCloser(strings.NewReader(http.StatusText(code))),
	}
}

type photoTripper int

// handler for the incoming requests
func (_ photoTripper) RoundTrip(r *http.Request) (*http.Response, error) {

	accumulator.Requests.Add(1)

	// TODO(rbastic): we need more custom logic injection in this piece,
	// maybe we shouldn't make it obvious that the line below is all we have
	// for protection against bad URLs hitting us.
	if r.URL.Path == "/" {
		return httpResponse(http.StatusNotFound), nil
	}

	if r.Method != "GET" && r.Method != "HEAD" {
		// TODO(rbastic): support PUT/POST in photosrv, allowing it to
		// route to storage nodes and handle replication for us
		// so our client code doesn't have to.
		return httpResponse(http.StatusMethodNotAllowed), nil
	}

	response, err := http.DefaultTransport.RoundTrip(r)

	if err != nil {
		glog.Infoln("Error getting item from remote:", err.Error())
		accumulator.Errors.Add(1)
	} else if response.StatusCode == 404 {
		// TODO(rbastic): decide something better to do about this in
		// the OSS version, fold it back into the internal version
		glog.Infoln("Not found: {\"UserAgent\": \"" + r.UserAgent() + "\", \"Referer\": \"" + r.Referer() + "\", \"Status\": \"404\", \"URL\": \"" + r.URL.String() + "\", \"IP\": \"" + r.RemoteAddr + "\"}")
	}

	return response, err
}

func rosterPoller(conf simpleconf.Config, rosterFile string, rosterCheck time.Duration) {
	linkdest, err := os.Readlink(rosterFile)
	if err != nil {
		glog.Infoln("warning: readlink(rosterFile=", rosterFile, "):", err)
	}

	for {
		time.Sleep(rosterCheck)

		newdest, err := os.Readlink(rosterFile)
		if err != nil {
			glog.Infoln("warning: readlink(rosterFile=", rosterFile, "):", err)
			continue
		}

		if newdest == linkdest {
			// no change
			continue
		}

		glog.Infoln("roster symlink changed: old=", linkdest, "new=", newdest, ", reloading config")
		cfg, err := loadConfig(conf, strings.ToLower(*dcName), rosterFile)
		if err != nil {
			glog.Infoln("error loading updated config", err)
			continue
		}
		UpdateConfig(cfg)

		linkdest = newdest
	}
}

type migrationState uint64

const (
	msOldConfig migrationState = iota
	msBothConfigs
	msNewConfig
)

// these stats must match the above constants
var migrationStates = []string{
	"msOldConfig",
	"msBothConfigs",
	"msNewConfig",
}

func (m migrationState) String() string {
	if int(m) < len(migrationStates) {
		return migrationStates[m]
	}

	return "Unknown"
}

var dcName *string
var currentMigrationState uint64
var secretMigrationKey *string

var errMissingMigrationRing = errors.New("missing migration ring")

func canSwitchToState(cfg *Config, newState migrationState) error {

	if cfg.mreplicas == nil && (newState == msBothConfigs || newState == msNewConfig) {
		return errMissingMigrationRing
	}

	return nil
}

func updateMigrationState(w http.ResponseWriter, r *http.Request, newState migrationState, mstateFile string) {
	// TODO(rbastic): check secretMigrationKey

	if r.FormValue("key") != *secretMigrationKey {
		msg := fmt.Sprintf("unable to switch to %s: invalid key", newState)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(msg))
		return
	}

	oldState := atomic.LoadUint64(&currentMigrationState)

	if err := canSwitchToState(CurrentConfig(), newState); err != nil {
		msg := fmt.Sprintf("unable to switch to %s: %v", newState, err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(msg))
		return
	}

	msg := fmt.Sprintf("currentMigrationState = %s (was %s)", newState, migrationState(oldState))
	atomic.StoreUint64(&currentMigrationState, uint64(newState))
	glog.Infoln(msg)
	if mstateFile != "" {
		err := ioutil.WriteFile(mstateFile, []byte(newState.String()), 0666)
		if err != nil {
			glog.Infoln("unable to persist migration state to %s: %v", mstateFile, err)
		}
	}
	w.Write([]byte(msg))
}

func migrationStart(w http.ResponseWriter, r *http.Request, mstateFile string) {
	updateMigrationState(w, r, msBothConfigs, mstateFile)
}

func migrationAbort(w http.ResponseWriter, r *http.Request, mstateFile string) {
	updateMigrationState(w, r, msOldConfig, mstateFile)
}

func migrationFinish(w http.ResponseWriter, r *http.Request, mstateFile string) {
	updateMigrationState(w, r, msNewConfig, mstateFile)
}

// Run is the entry point that your main() function should call.
func Run() {

	cfgfile := flag.String("c", "config/photosrv.cfg", "config file to load")
	httpport := flag.Uint("httpport", 8888, "http port to listen on (default: 8888)")
	debugport := flag.Uint("debugport", 8080, "debug port to listen on (default: 8080)")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU(), "GOMAXPROCS")
	// TODO(rbastic): Document what is expected by the roster format. How this can work in practice.
	rosterFile := flag.String("roster", "config/photostorage.roster", "roster file with valid photostorage hosts")
	rosterCheck := flag.Duration("rosterCheck", 1*time.Second, "time between roster file check")
	flagDcName := flag.String("dcname", "", "DC name for this given daemon.")

	// TODO(rbastic): decide if this is where we want our public-release cache file to be?
	mstateFile := flag.String("migration", "/var/cache/photosrv/migrationState", "file for persistent migration status")
	migrationKey := flag.String("migrationKey", "", "secret migration key")

	readTimeout := flag.Duration("readTimeout", 30*time.Minute, "length of time a read request can take (set this low to prevent Slowloris attacks)")
	writeTimeout := flag.Duration("writeTimeout", 30*time.Minute, "length of time a write request can take")

	// TODO(rbastic): document/advice on what this does
	htMs := flag.Int("headtimeoutms", 0, "timeout (in ms) for storage nodes")
	headTimeoutMs = *htMs

	flag.Parse()

	secretMigrationKey = migrationKey

	glog.Infoln("HEAD timeout (in ms):", headTimeoutMs)

	dcName = flagDcName
	glog.Infoln("DCName chosen", *dcName)

	expvar.NewString("BuildVersion").Set(BuildVersion)
	glog.Infoln("Starting photosrv", BuildVersion)

	conf, err := simpleconf.NewFromFile(*cfgfile)
	if err != nil {
		glog.Fatalf("Unable to load config file %s: %v", *cfgfile, err)
	}

	cfg, err := loadConfig(conf, strings.ToLower(*dcName), *rosterFile)
	if err != nil {
		glog.Fatal("error loading config:", err)
	}
	UpdateConfig(cfg)

	if *rosterFile != "" {
		glog.Infoln("Monitoring roster file", *rosterFile)
		go rosterPoller(conf, *rosterFile, *rosterCheck)
	}

	expvar.Publish("MissingShards", expvar.Func(func() interface{} {
		c := CurrentConfig()

		var primary []string
		for k, v := range c.replicas {
			if len(v) == 0 {
				primary = append(primary, k)
			}
		}

		var migration []string
		for k, v := range c.mreplicas {
			if len(v) == 0 {
				migration = append(migration, k)
			}
		}

		return struct {
			Primary   []string
			Migration []string
		}{
			Primary:   primary,
			Migration: migration,
		}
	}))

	glog.Infoln("Setting GOMAXPROCS=%d", *maxprocs)
	runtime.GOMAXPROCS(*maxprocs)

	// start the backend control server
	debugPortStr := strconv.Itoa(int(*debugport))
	glog.Infoln("Debug info on port", debugPortStr)

	if *mstateFile != "" {
		glog.Infoln("Using mstate file", *mstateFile)
		b, err := ioutil.ReadFile(*mstateFile)
		if err != nil && !os.IsNotExist(err) {
			glog.Infoln("unable to read migration state from %v: %v", *mstateFile, err)
		}
		if b != nil && len(b) > 0 {
			bstr := string(b)
			var found bool
			for i, s := range migrationStates {
				if bstr == s {
					found = true
					if err := canSwitchToState(cfg, migrationState(i)); err != nil {
						glog.Fatalf("unable to load migration state %q: %v", migrationState(i), err)
					}
					glog.Infoln("restoring saved migration state %q from %s", bstr, *mstateFile)
					atomic.StoreUint64(&currentMigrationState, uint64(i))
					break
				}
			}
			if !found {
				glog.Fatalf("bad migration state %q from %v", bstr, *mstateFile)
			}
		}
	}

	expvar.Publish("MigrationState", expvar.Func(func() interface{} { s := atomic.LoadUint64(&currentMigrationState); return migrationState(s).String() }))

	// TODO(rbastic): document rebalancing/migration state
	http.HandleFunc("/migration/start", func(w http.ResponseWriter, r *http.Request) { migrationStart(w, r, *mstateFile) })
	http.HandleFunc("/migration/abort", func(w http.ResponseWriter, r *http.Request) { migrationAbort(w, r, *mstateFile) })
	http.HandleFunc("/migration/finish", func(w http.ResponseWriter, r *http.Request) { migrationFinish(w, r, *mstateFile) })

	// TODO(rbastic): either break this out into main.go or doxx the innate possibility of
	// graphite integration.
	if host, port := os.Getenv("GRAPHITEHOST"), os.Getenv("GRAPHITEPORT"); host != "" {
		// TODO(dgryski): This block is repeated is all of our daemons.  It should be moved to booking/graphite.
		if port == "" {
			port = "2003"
		}
		hostport := host + ":" + port
		glog.Infoln("Using graphite host", hostport)
		graphite, err := g2g.NewGraphite(hostport, time.Duration(60*time.Second), time.Duration(2*time.Second))
		if err != nil {
			glog.Fatal("unable to contact graphite: ", err)
		}

		// TODO: get HostNameGraphite() ....
		//namespace := "http.photosrv." + systemsettings.HostNameGraphite()
		namespace := "http.photosrv."

		graphite.Register(namespace+".requests", accumulator.Requests)
		graphite.Register(namespace+".errors", accumulator.Errors)
		graphite.Register(namespace+".multiheadtimeout", accumulator.MultiHeadTimeout)
	}

	shardproxy := &httputil.ReverseProxy{Director: shardedkvDirector, Transport: photoTripper(0)}

	// start the front end server
	portString := strconv.Itoa(int(*httpport))

	debugServer := &http.Server{
		Addr:    ":" + debugPortStr,
		Handler: nil,
	}

	s := &http.Server{
		Addr:           ":" + portString,
		Handler:        shardproxy,
		ReadTimeout:    *readTimeout,
		WriteTimeout:   *writeTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	gracehttp.Serve(s, debugServer)
}
