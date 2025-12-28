package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	baseURL     = flag.String("url", "http://localhost:8000", "Centrifugo base URL")
	apiKey      = flag.String("key", "fs61BkgTximBGGpCg7aiq4iKHzU-z_6tbSjkiJ0myEMpYTiFIuBzl_Oai1RJ4tVN-tCrHQj3EaQ", "API key")
	numMessages = flag.Int("messages", 1000, "Number of messages to send")
	concurrency = flag.Int("concurrency", 10, "Number of concurrent workers")
	channel     = flag.String("channel", "self:test", "Channel to publish to")
)

type PublishRequest struct {
	Channel string      `json:"channel"`
	Data    interface{} `json:"data"`
}

type APIRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

type Stats struct {
	mu           sync.Mutex
	latencies    []time.Duration
	errors       int64
	totalSent    int64
	startTime    time.Time
	lastPrintTime time.Time
}

func (s *Stats) RecordLatency(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
	atomic.AddInt64(&s.totalSent, 1)
}

func (s *Stats) RecordError() {
	atomic.AddInt64(&s.errors, 1)
}

func (s *Stats) PrintProgress() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	now := time.Now()
	if now.Sub(s.lastPrintTime) < time.Second {
		return
	}
	s.lastPrintTime = now
	
	elapsed := now.Sub(s.startTime)
	sent := atomic.LoadInt64(&s.totalSent)
	errors := atomic.LoadInt64(&s.errors)
	rate := float64(sent) / elapsed.Seconds()
	
	fmt.Printf("\r[%s] Sent: %d, Errors: %d, Rate: %.1f msg/s", 
		elapsed.Round(time.Second), sent, errors, rate)
}

func (s *Stats) PrintSummary() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	elapsed := time.Since(s.startTime)
	sent := atomic.LoadInt64(&s.totalSent)
	errors := atomic.LoadInt64(&s.errors)
	rate := float64(sent) / elapsed.Seconds()
	
	fmt.Printf("\n\n========== Summary ==========\n")
	fmt.Printf("Total messages: %d\n", sent)
	fmt.Printf("Total errors: %d\n", errors)
	fmt.Printf("Total time: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Rate: %.1f msg/s\n", rate)
	
	if len(s.latencies) > 0 {
		sort.Slice(s.latencies, func(i, j int) bool {
			return s.latencies[i] < s.latencies[j]
		})
		
		var total time.Duration
		for _, l := range s.latencies {
			total += l
		}
		avg := total / time.Duration(len(s.latencies))
		
		p50 := s.latencies[len(s.latencies)*50/100]
		p90 := s.latencies[len(s.latencies)*90/100]
		p95 := s.latencies[len(s.latencies)*95/100]
		p99 := s.latencies[len(s.latencies)*99/100]
		max := s.latencies[len(s.latencies)-1]
		min := s.latencies[0]
		
		fmt.Printf("\nLatency Statistics:\n")
		fmt.Printf("  Min: %s\n", min.Round(time.Microsecond))
		fmt.Printf("  Avg: %s\n", avg.Round(time.Microsecond))
		fmt.Printf("  P50: %s\n", p50.Round(time.Microsecond))
		fmt.Printf("  P90: %s\n", p90.Round(time.Microsecond))
		fmt.Printf("  P95: %s\n", p95.Round(time.Microsecond))
		fmt.Printf("  P99: %s\n", p99.Round(time.Microsecond))
		fmt.Printf("  Max: %s\n", max.Round(time.Microsecond))
	}
	fmt.Printf("==============================\n")
}

func publishMessage(client *http.Client, url, apiKey, channel string, msgNum int) (time.Duration, error) {
	data := map[string]interface{}{
		"text":      fmt.Sprintf("Test message %d", msgNum),
		"timestamp": time.Now().UnixMilli(),
		"msgNum":    msgNum,
	}
	
	reqBody := APIRequest{
		Method: "publish",
		Params: PublishRequest{
			Channel: channel,
			Data:    data,
		},
	}
	
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}
	
	req, err := http.NewRequest("POST", url+"/api", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, err
	}
	
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "apikey "+apiKey)
	
	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)
	
	if err != nil {
		return latency, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return latency, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	
	// Read response body to complete the request
	io.Copy(io.Discard, resp.Body)
	
	return latency, nil
}

func worker(id int, jobs <-chan int, stats *Stats, client *http.Client, url, apiKey, channel string, wg *sync.WaitGroup) {
	defer wg.Done()
	
	for msgNum := range jobs {
		latency, err := publishMessage(client, url, apiKey, channel, msgNum)
		if err != nil {
			stats.RecordError()
			if msgNum%100 == 0 {
				fmt.Printf("\nWorker %d error on message %d: %v\n", id, msgNum, err)
			}
		} else {
			stats.RecordLatency(latency)
		}
		stats.PrintProgress()
	}
}

func runSubscribeUnsubscribeTest(client *http.Client, url, apiKey string, iterations int) {
	fmt.Println("\n========== Subscribe/Unsubscribe Test ==========")
	
	stats := &Stats{
		startTime:     time.Now(),
		lastPrintTime: time.Now(),
	}
	
	for i := 0; i < iterations; i++ {
		channel := fmt.Sprintf("test:channel_%d", i)
		
		// Subscribe
		start := time.Now()
		reqBody := APIRequest{
			Method: "subscribe",
			Params: map[string]interface{}{
				"channel": channel,
				"user":    "test_user",
			},
		}
		jsonBody, _ := json.Marshal(reqBody)
		
		req, _ := http.NewRequest("POST", url+"/api", bytes.NewReader(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "apikey "+apiKey)
		
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			stats.RecordLatency(time.Since(start))
		} else {
			stats.RecordError()
		}
		
		// Unsubscribe
		start = time.Now()
		reqBody = APIRequest{
			Method: "unsubscribe",
			Params: map[string]interface{}{
				"channel": channel,
				"user":    "test_user",
			},
		}
		jsonBody, _ = json.Marshal(reqBody)
		
		req, _ = http.NewRequest("POST", url+"/api", bytes.NewReader(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "apikey "+apiKey)
		
		resp, err = client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			stats.RecordLatency(time.Since(start))
		} else {
			stats.RecordError()
		}
		
		stats.PrintProgress()
	}
	
	stats.PrintSummary()
}

func main() {
	flag.Parse()
	
	fmt.Printf("Stress Test Configuration:\n")
	fmt.Printf("  URL: %s\n", *baseURL)
	fmt.Printf("  Channel: %s\n", *channel)
	fmt.Printf("  Messages: %d\n", *numMessages)
	fmt.Printf("  Concurrency: %d\n", *concurrency)
	fmt.Println()
	
	// Create HTTP client with connection pooling
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
	
	// Test 1: Publish messages
	fmt.Println("========== Publish Test ==========")
	
	stats := &Stats{
		startTime:     time.Now(),
		lastPrintTime: time.Now(),
	}
	
	jobs := make(chan int, *numMessages)
	var wg sync.WaitGroup
	
	// Start workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(i, jobs, stats, client, *baseURL, *apiKey, *channel, &wg)
	}
	
	// Send jobs
	for i := 0; i < *numMessages; i++ {
		jobs <- i
	}
	close(jobs)
	
	// Wait for completion
	wg.Wait()
	stats.PrintSummary()
	
	// Test 2: Subscribe/Unsubscribe cycles (simulates goroutine leak issue)
	fmt.Println("\nPress Enter to run Subscribe/Unsubscribe test (or Ctrl+C to exit)...")
	fmt.Scanln()
	runSubscribeUnsubscribeTest(client, *baseURL, *apiKey, 500)
	
	// Get P2P status
	fmt.Println("\n========== P2P Status ==========")
	resp, err := client.Get(*baseURL + "/api/p2p/status")
	if err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		
		var result map[string]interface{}
		if json.Unmarshal(body, &result) == nil {
			prettyJSON, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(prettyJSON))
		} else {
			fmt.Println(string(body))
		}
	} else {
		fmt.Printf("Error getting P2P status: %v\n", err)
	}
}

