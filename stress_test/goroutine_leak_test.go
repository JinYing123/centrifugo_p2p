package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

var (
	baseURL    = flag.String("url", "http://localhost:8000", "Centrifugo base URL")
	apiKey     = flag.String("key", "fs61BkgTximBGGpCg7aiq4iKHzU-z_6tbSjkiJ0myEMpYTiFIuBzl_Oai1RJ4tVN-tCrHQj3EaQ", "API key")
	iterations = flag.Int("iterations", 100, "Number of subscribe/unsubscribe cycles")
	interval   = flag.Duration("interval", time.Second*5, "Interval between status checks")
)

type APIRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

func callAPI(client *http.Client, url, apiKey, method string, params interface{}) error {
	reqBody := APIRequest{
		Method: method,
		Params: params,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url+"/api", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "apikey "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return nil
}

func getP2PStatus(client *http.Client, url string) (map[string]interface{}, error) {
	resp, err := client.Get(url + "/api/p2p/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	return result, err
}

func publishMessage(client *http.Client, url, apiKey, channel string, msgNum int) error {
	params := map[string]interface{}{
		"channel": channel,
		"data": map[string]interface{}{
			"text":   fmt.Sprintf("Test message %d", msgNum),
			"time":   time.Now().UnixMilli(),
			"msgNum": msgNum,
		},
	}
	return callAPI(client, url, apiKey, "publish", params)
}

func main() {
	flag.Parse()

	fmt.Println("==============================================")
	fmt.Println("  Goroutine Leak Test for P2P Broker")
	fmt.Println("==============================================")
	fmt.Printf("URL: %s\n", *baseURL)
	fmt.Printf("Iterations: %d\n", *iterations)
	fmt.Println()

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Get initial status
	fmt.Println("Initial P2P Status:")
	status, err := getP2PStatus(client, *baseURL)
	if err != nil {
		fmt.Printf("Error getting initial status: %v\n", err)
		return
	}
	printStatus(status)

	fmt.Println("\n--- Starting Subscribe/Unsubscribe Cycles ---")

	// Run subscribe/unsubscribe cycles with publish
	for i := 0; i < *iterations; i++ {
		channel := fmt.Sprintf("test:leak_test_%d", i)

		// Subscribe a fake user
		err := callAPI(client, *baseURL, *apiKey, "subscribe", map[string]interface{}{
			"channel": channel,
			"user":    fmt.Sprintf("user_%d", i),
		})
		if err != nil {
			fmt.Printf("\n[%d] Subscribe error: %v", i, err)
		}

		// Publish some messages
		for j := 0; j < 5; j++ {
			publishMessage(client, *baseURL, *apiKey, channel, i*5+j)
		}

		// Unsubscribe
		err = callAPI(client, *baseURL, *apiKey, "unsubscribe", map[string]interface{}{
			"channel": channel,
			"user":    fmt.Sprintf("user_%d", i),
		})
		if err != nil {
			fmt.Printf("\n[%d] Unsubscribe error: %v", i, err)
		}

		// Print progress
		if (i+1)%10 == 0 {
			fmt.Printf("\rCompleted %d/%d cycles...", i+1, *iterations)
		}

		// Check status periodically
		if (i+1)%50 == 0 {
			fmt.Println()
			status, _ := getP2PStatus(client, *baseURL)
			printStatus(status)
		}
	}

	fmt.Println("\n\n--- Test Completed ---")

	// Wait a bit for goroutines to settle
	fmt.Println("Waiting 3 seconds for goroutines to settle...")
	time.Sleep(3 * time.Second)

	// Get final status
	fmt.Println("\nFinal P2P Status:")
	status, err = getP2PStatus(client, *baseURL)
	if err != nil {
		fmt.Printf("Error getting final status: %v\n", err)
		return
	}
	printStatus(status)

	// Compare
	fmt.Println("\n--- Analysis ---")
	topicsCount, ok := status["topics_count"].(float64)
	if ok && topicsCount > 10 {
		fmt.Printf("⚠️  WARNING: topics_count is %v - possible memory leak!\n", topicsCount)
	} else {
		fmt.Printf("✅ topics_count looks normal: %v\n", status["topics_count"])
	}
}

func printStatus(status map[string]interface{}) {
	fmt.Printf("  topics_count: %v\n", status["topics_count"])
	fmt.Printf("  node_topics_count: %v\n", status["node_topics_count"])
	fmt.Printf("  known_nodes_count: %v\n", status["known_nodes_count"])
	fmt.Printf("  peer_count: %v\n", status["peer_count"])

	if topics, ok := status["topics"].([]interface{}); ok && len(topics) > 0 {
		fmt.Printf("  topics: %v\n", topics)
	}
}

