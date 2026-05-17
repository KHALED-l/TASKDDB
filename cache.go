package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────
//  Data Structures
// ─────────────────────────────────────────────

// CacheEntry holds a single record stored temporarily in the cache
type CacheEntry struct {
	Name     string    `json:"name"`
	Age      string    `json:"age"`
	StoredAt time.Time `json:"stored_at"`
}

// CacheStore is a thread-safe temporary store
type CacheStore struct {
	mu      sync.Mutex
	entries []CacheEntry
}

func (cs *CacheStore) Add(e CacheEntry) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.entries = append(cs.entries, e)
}

func (cs *CacheStore) All() []CacheEntry {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cp := make([]CacheEntry, len(cs.entries))
	copy(cp, cs.entries)
	return cp
}

func (cs *CacheStore) Clear() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.entries = nil
}

// ─────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────

var (
	cache    = &CacheStore{}
	mainAddr = "127.0.0.1:9100" // Main server TCP address
	logger   = log.New(log.Writer(), "[CACHE] ", log.LstdFlags)
)

// ─────────────────────────────────────────────
//  Step tracking for GUI feedback
// ─────────────────────────────────────────────

type WorkflowStatus struct {
	mu           sync.Mutex
	IncomingData string      `json:"incoming_data"`
	CacheStatus  string      `json:"cache_status"`
	MainServer   string      `json:"main_server"`
	CacheCleared string      `json:"cache_cleared"`
	LastEntry    *CacheEntry `json:"last_entry"`
}

var wfStatus = &WorkflowStatus{}

func (ws *WorkflowStatus) Reset() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.IncomingData = ""
	ws.CacheStatus = ""
	ws.MainServer = ""
	ws.CacheCleared = ""
	ws.LastEntry = nil
}

func (ws *WorkflowStatus) Get() map[string]interface{} {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return map[string]interface{}{
		"incoming_data": ws.IncomingData,
		"cache_status":  ws.CacheStatus,
		"main_server":   ws.MainServer,
		"cache_cleared": ws.CacheCleared,
		"last_entry":    ws.LastEntry,
		"cached_count":  len(cache.All()),
	}
}

// ─────────────────────────────────────────────
//  Core Workflow
// ─────────────────────────────────────────────

// processInsert runs the full 4-step cache workflow:
//  1. Client sends INSERT|Name|Age
//  2. Cache stores temporarily
//  3. Forward to Main: FORWARD|Name|Age
//  4. On success, clear from cache
func processInsert(name, age string) (map[string]interface{}, error) {
	entry := CacheEntry{Name: name, Age: age, StoredAt: time.Now()}

	// Step 1 – receive
	wfStatus.mu.Lock()
	wfStatus.IncomingData = fmt.Sprintf("%s, %s", name, age)
	wfStatus.CacheStatus = ""
	wfStatus.MainServer = ""
	wfStatus.CacheCleared = ""
	wfStatus.LastEntry = &entry
	wfStatus.mu.Unlock()
	logger.Printf("Step 1 — Received: INSERT|%s|%s", name, age)

	// Step 2 – store in cache temporarily
	cache.Add(entry)
	wfStatus.mu.Lock()
	wfStatus.CacheStatus = "Stored Temporarily"
	wfStatus.mu.Unlock()
	logger.Printf("Step 2 — Cached: %s, %s", name, age)

	// Step 3 – forward to Main server
	forwardMsg := fmt.Sprintf("FORWARD|%s|%s", name, age)
	err := sendToMain(forwardMsg)
	if err != nil {
		wfStatus.mu.Lock()
		wfStatus.MainServer = "Replication Failed: " + err.Error()
		wfStatus.mu.Unlock()
		logger.Printf("Step 3 — Forward failed: %v", err)
		return nil, fmt.Errorf("forward failed: %w", err)
	}
	wfStatus.mu.Lock()
	wfStatus.MainServer = "Replication Success"
	wfStatus.mu.Unlock()
	logger.Printf("Step 3 — Forwarded to Main: %s", forwardMsg)

	// Step 4 – clear cache
	cache.Clear()
	wfStatus.mu.Lock()
	wfStatus.CacheCleared = "Cache Cleared"
	wfStatus.mu.Unlock()
	logger.Printf("Step 4 — Cache cleared")

	return wfStatus.Get(), nil
}

// sendToMain opens a TCP connection to the Main server and sends the message
func sendToMain(msg string) error {
	conn, err := net.DialTimeout("tcp", mainAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("cannot reach Main at %s: %w", mainAddr, err)
	}
	defer conn.Close()

	// Send the forward command
	fmt.Fprintln(conn, msg)

	// Wait for ACK from Main (with timeout)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		resp := strings.TrimSpace(scanner.Text())
		if resp == "ACK" {
			return nil
		}
		return fmt.Errorf("unexpected response from Main: %s", resp)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading Main response: %w", err)
	}
	return fmt.Errorf("no response from Main server")
}

// ─────────────────────────────────────────────
//  HTTP API
// ─────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// POST /api/cache/insert   body: {"data":"INSERT|Ahmed|22"}
//
//	or JSON: {"name":"Ahmed","age":"22"}
func handleInsert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Data string `json:"data"` // e.g. "INSERT|Ahmed|22"
		Name string `json:"name"`
		Age  string `json:"age"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	name, age := req.Name, req.Age

	// Also accept raw protocol string "INSERT|Name|Age"
	if name == "" && req.Data != "" {
		parts := strings.SplitN(req.Data, "|", 3)
		if len(parts) == 3 && strings.ToUpper(parts[0]) == "INSERT" {
			name, age = parts[1], parts[2]
		}
	}

	if name == "" {
		jsonErr(w, "name is required (send {name,age} or {data:'INSERT|Name|Age'})", 400)
		return
	}

	result, err := processInsert(name, age)
	if err != nil {
		jsonErr(w, err.Error(), 502)
		return
	}
	jsonOK(w, result)
}

// GET /api/cache/status
func handleStatus(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, wfStatus.Get())
}

// GET /api/cache/entries  – current temporary cache contents
func handleEntries(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, map[string]interface{}{"entries": cache.All()})
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

func main() {
	// Allow overriding Main server address via env
	// e.g.  MAIN_ADDR=192.168.1.5:9100 go run cache.go
	if addr := mainAddrFromEnv(); addr != "" {
		mainAddr = addr
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/cache/insert", handleInsert)
	mux.HandleFunc("/api/cache/status", handleStatus)
	mux.HandleFunc("/api/cache/entries", handleEntries)

	logger.Printf("Cache node HTTP API listening on :8081")
	logger.Printf("Will forward to Main server at %s", mainAddr)

	if err := http.ListenAndServe(":8081", mux); err != nil {
		logger.Fatal("HTTP error:", err)
	}
}

func mainAddrFromEnv() string {
	// Read MAIN_ADDR from environment (simple, no import needed in stdlib)
	// We use os.Getenv via the os package – add it to imports if needed.
	return "" // placeholder; extend with os.Getenv("MAIN_ADDR") if desired
}
