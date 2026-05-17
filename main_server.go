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

// StoredRecord is a permanently stored entry
type StoredRecord struct {
	Name      string    `json:"name"`
	Age       string    `json:"age"`
	ReceivedAt time.Time `json:"received_at"`
}

// RecordStore is thread-safe permanent storage
type RecordStore struct {
	mu      sync.RWMutex
	records []StoredRecord
}

func (rs *RecordStore) Add(r StoredRecord) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.records = append(rs.records, r)
}

func (rs *RecordStore) All() []StoredRecord {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	cp := make([]StoredRecord, len(rs.records))
	copy(cp, rs.records)
	return cp
}

// ─────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────

var (
	store  = &RecordStore{}
	logger = log.New(log.Writer(), "[MAIN] ", log.LstdFlags)
)

// ─────────────────────────────────────────────
//  TCP Server – accepts FORWARD commands from Cache
// ─────────────────────────────────────────────

func startTCP(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal("TCP listen error:", err)
	}
	logger.Printf("TCP listening on port %s (accepting FORWARD from Cache)", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Printf("Accept error: %v", err)
			continue
		}
		go handleCache(conn)
	}
}

// handleCache processes a FORWARD|Name|Age message and replies ACK
func handleCache(conn net.Conn) {
	defer conn.Close()
	ip := conn.RemoteAddr().String()
	logger.Printf("Cache connected from %s", ip)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		logger.Printf("Received from cache: %s", line)

		parts := strings.SplitN(line, "|", 3)
		if len(parts) == 3 && strings.ToUpper(parts[0]) == "FORWARD" {
			record := StoredRecord{
				Name:       parts[1],
				Age:        parts[2],
				ReceivedAt: time.Now(),
			}
			store.Add(record)
			logger.Printf("Stored permanently: %s, %s", record.Name, record.Age)
			// Send ACK back to cache
			fmt.Fprintln(conn, "ACK")
		} else {
			logger.Printf("Unknown command: %s", line)
			fmt.Fprintln(conn, "ERR|unknown command")
		}
	}
}

// ─────────────────────────────────────────────
//  HTTP API – read stored records
// ─────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(v)
}

// GET /api/records
func handleRecords(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, map[string]interface{}{
		"records": store.All(),
		"count":   len(store.All()),
	})
}

func main() {
	// TCP on 9100 for Cache → Main forwarding
	go startTCP("9100")

	// HTTP on 8082 for querying stored records
	mux := http.NewServeMux()
	mux.HandleFunc("/api/records", handleRecords)

	logger.Printf("Main server HTTP API on :8082")
	logger.Printf("Main server TCP on :9100")
	if err := http.ListenAndServe(":8082", mux); err != nil {
		logger.Fatal("HTTP error:", err)
	}
}
