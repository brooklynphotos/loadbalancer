package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/**
 * based on https://github.com/kasvith/simplelb/blob/master/main.go
 * article here: https://kasvith.github.io/posts/lets-create-a-simple-lb-go/
 */

var serverPool ServerPool

// used for storing information in the context
const (
	// iota is unique
	Attempts int = iota
	Retry
)

func main() {
	var serverList string
	var port int
	// using flag to get the command arguments
	flag.StringVar(&serverList, "backends", "", "Load balanced backends, use commas to separate")
	flag.IntVar(&port, "port", 3030, "Port to serve")
	flag.Parse()

	// make sure we have something to work with
	if len(serverList) == 0 {
		log.Fatal("Please provide one or more backends to load balance")
	}

	tokens := strings.Split(serverList, ",")
	for _, tok := range tokens {
		serverURL, err := url.Parse(tok)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverURL)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", serverURL.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			// after 3 retries
			serverPool.MarkBackendStatus(serverURL, false)

			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			lb(writer, request.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:          serverURL,
			Alive:        true,
			ReverseProxy: proxy,
		})
		log.Printf("Configured server: %s\n", serverURL)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	// separate go routine to do the health check
	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}

/**
 * checks the health every 20 seconds
 */
func healthCheck() {
	t := time.NewTicker(time.Second * 20)
	for {
		select {
		case <-t.C: // wait until 20 seconds have passed
			log.Println("Starting health check....")
			serverPool.HealthCheck()
			log.Println("Health check completed")
		}
	}
}

/**
 * IsBackendAlive checks if url is responsive
 */
func IsBackendAlive(u *url.URL) bool {
	// checks if tcp is alive
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error", err)
		return false
	}
	_ = conn.Close()
	return true
}

// GetRetryFromContext returns the retries for request
func GetRetryFromContext(r *http.Request) int {
	// r.Context().Value takes a key, in this case, Retry
	if attempt, ok := r.Context().Value(Retry).(int); ok {
		return attempt
	}
	return 0
}

// GetAttemptsFromContext returns the attempts for request
func GetAttemptsFromContext(r *http.Request) int {
	// r.Context().Value takes a key, in this case, Retry
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 1
}

/**
 * load balance
 */
func lb(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}
	if peer := serverPool.GetNextPeer(); peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

type ServerPool struct {
	backends []*Backend // all the backends
	current  uint64     // counter
}

// MarkBackendStatus changes a status of a backend
func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	// find the backend whose url matches
	for _, bg := range s.backends {
		if bg.URL.String() == backendUrl.String() {
			bg.SetAlive(alive)
			break
		}
	}
}

// NextIndex returns the next index for the backend. This will work in multithreaded situations
func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (s *ServerPool) GetNextPeer() *Backend {
	// look for the next server that is alive
	next := s.NextIndex()
	// setting the upper bound of the loop
	l := len(s.backends) + next
	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx)) // set the current value to the idx
			}
			return s.backends[idx]
		}
	}
	// everything is dead
	return nil
}

// HealthCheck pings the backends and update the status
func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "up"
		alive := IsBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", b.URL, status)
	}
}

type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

/**
 * sets the alive value for this backend
 */
func (b *Backend) SetAlive(alive bool) {
	// this is needed since many goroutines could be accessing this function
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

/**
 * thread-safe retrieval of alive info
 */
func (b *Backend) IsAlive() bool {
	// the read is more frequent so let's not block the write
	b.mux.RLock()
	alive := b.Alive
	b.mux.RUnlock()
	return alive
}
