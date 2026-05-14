package main

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────
//  Data Structures
// ─────────────────────────────────────────────

type Snap struct {
	IP      string
	Name    string
	Conn    net.Conn
	ImgPath string
}

// MapReduce result from each snap
type MapResult struct {
	SnapName string `json:"snap"`
	Query    string `json:"query"`
	Count    int    `json:"count"`
	Lines    []string `json:"lines"`
	Error    string `json:"error,omitempty"`
}

var (
	snaps     = make(map[string]*Snap)
	snapsLock = sync.Mutex{}
	logFile   *os.File
)

// ─────────────────────────────────────────────
//  Consistent Hashing Ring
// ─────────────────────────────────────────────

type HashRing struct {
	nodes   []string
	ring    map[uint32]string
	sorted  []uint32
	mu      sync.RWMutex
}

func NewHashRing() *HashRing {
	return &HashRing{
		ring: make(map[uint32]string),
	}
}

func (hr *HashRing) hash(key string) uint32 {
	h := md5.Sum([]byte(key))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

func (hr *HashRing) AddNode(node string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	// Add 3 virtual nodes per snap for better distribution
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s#%d", node, i)
		h := hr.hash(key)
		hr.ring[h] = node
		hr.sorted = append(hr.sorted, h)
	}
	hr.nodes = append(hr.nodes, node)
	sortUint32(hr.sorted)
}

func (hr *HashRing) RemoveNode(node string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s#%d", node, i)
		h := hr.hash(key)
		delete(hr.ring, h)
		hr.sorted = removeUint32(hr.sorted, h)
	}
	hr.nodes = removeString(hr.nodes, node)
}

func (hr *HashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	if len(hr.ring) == 0 {
		return ""
	}
	h := hr.hash(key)
	for _, pos := range hr.sorted {
		if h <= pos {
			return hr.ring[pos]
		}
	}
	return hr.ring[hr.sorted[0]]
}

func sortUint32(s []uint32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func removeUint32(s []uint32, v uint32) []uint32 {
	r := []uint32{}
	for _, x := range s {
		if x != v {
			r = append(r, x)
		}
	}
	return r
}

func removeString(s []string, v string) []string {
	r := []string{}
	for _, x := range s {
		if x != v {
			r = append(r, x)
		}
	}
	return r
}

var hashRing = NewHashRing()

// ─────────────────────────────────────────────
//  Logging
// ─────────────────────────────────────────────

func logAction(format string, args ...interface{}) {
	msg := fmt.Sprintf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), fmt.Sprintf(format, args...))
	fmt.Print(msg)
	if logFile != nil {
		logFile.WriteString(msg)
	}
}

// ─────────────────────────────────────────────
//  TCP Server - Snap Connections
// ─────────────────────────────────────────────

func startTCPServer() {
	ln, err := net.Listen("tcp", ":8082")
	if err != nil {
		log.Fatal("TCP listen error:", err)
	}
	logAction("TCP server listening on :8082")
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleSnap(conn)
	}
}

func handleSnap(conn net.Conn) {
	reader := bufio.NewReader(conn)
	// First message: "online|snap-name"
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}
	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, "|", 2)
	if len(parts) < 2 || parts[0] != "online" {
		conn.Close()
		return
	}
	name := parts[1]
	ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()

	snap := &Snap{IP: ip, Name: name, Conn: conn}

	snapsLock.Lock()
	snaps[name] = snap
	snapsLock.Unlock()

	hashRing.AddNode(name)
	logAction("Snap connected: %s (%s)", name, ip)

	// Keep reading incoming messages from snap (imgpath, mapresult, etc.)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		msg = strings.TrimSpace(msg)
		handleSnapMessage(name, msg)
	}

	snapsLock.Lock()
	delete(snaps, name)
	snapsLock.Unlock()
	hashRing.RemoveNode(name)
	logAction("Snap disconnected: %s", name)
	conn.Close()
}

// Channel to collect MapReduce results
var mapResultChan = make(chan MapResult, 100)

func handleSnapMessage(snapName, msg string) {
	if strings.HasPrefix(msg, "imgpath|") {
		path := strings.TrimPrefix(msg, "imgpath|")
		snapsLock.Lock()
		if s, ok := snaps[snapName]; ok {
			s.ImgPath = path
		}
		snapsLock.Unlock()
		logAction("Snap %s sent image path: %s", snapName, path)
	} else if strings.HasPrefix(msg, "mapresult|") {
		// Format: mapresult|<json>
		jsonStr := strings.TrimPrefix(msg, "mapresult|")
		var result MapResult
		if err := json.Unmarshal([]byte(jsonStr), &result); err == nil {
			result.SnapName = snapName
			mapResultChan <- result
		}
	} else {
		logAction("Message from %s: %s", snapName, msg)
	}
}

// ─────────────────────────────────────────────
//  Send Command Helpers
// ─────────────────────────────────────────────

func sendCommand(snapName, cmd string) error {
	snapsLock.Lock()
	s, ok := snaps[snapName]
	snapsLock.Unlock()
	if !ok {
		return fmt.Errorf("snap %s not found", snapName)
	}
	_, err := fmt.Fprintf(s.Conn, cmd+"\n")
	return err
}

// ─────────────────────────────────────────────
//  HTTP Handlers
// ─────────────────────────────────────────────

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("static/dashboard.html")
	if err != nil {
		http.Error(w, "Template error: "+err.Error(), 500)
		return
	}
	snapsLock.Lock()
	snapList := []map[string]string{}
	for name, s := range snaps {
		snapList = append(snapList, map[string]string{
			"name": name,
			"ip":   s.IP,
		})
	}
	snapsLock.Unlock()
	tmpl.Execute(w, snapList)
}

// GET /api/snaps — list connected snaps
func apiSnaps(w http.ResponseWriter, r *http.Request) {
	snapsLock.Lock()
	list := []map[string]string{}
	for name, s := range snaps {
		list = append(list, map[string]string{"name": name, "ip": s.IP})
	}
	snapsLock.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// POST /api/shutdown?snap=snap-01  (snap=ALL for all)
func shutdownHandler(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("snap")
	results := map[string]string{}

	snapsLock.Lock()
	targets := []string{}
	if target == "ALL" {
		for name := range snaps {
			targets = append(targets, name)
		}
	} else {
		targets = append(targets, target)
	}
	snapsLock.Unlock()

	for _, name := range targets {
		if err := sendCommand(name, "shutdown"); err != nil {
			results[name] = "error: " + err.Error()
		} else {
			results[name] = "shutdown sent"
			logAction("Shutdown sent to %s", name)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// POST /api/background?snap=snap-01&path=/path/to/img.jpg
func backgroundHandler(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("snap")
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing path", 400)
		return
	}
	results := map[string]string{}

	snapsLock.Lock()
	targets := []string{}
	if target == "ALL" {
		for name := range snaps {
			targets = append(targets, name)
		}
	} else {
		targets = append(targets, target)
	}
	snapsLock.Unlock()

	for _, name := range targets {
		cmd := "background|" + path
		if err := sendCommand(name, cmd); err != nil {
			results[name] = "error: " + err.Error()
		} else {
			results[name] = "background command sent"
			logAction("Background changed on %s to %s", name, path)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// POST /api/sendfile — multipart upload, then stream to snap
// Form fields: snap=snap-01, file=<binary>
func sendFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	r.ParseMultipartForm(100 << 20) // 100 MB max

	snapName := r.FormValue("snap")
	if snapName == "" {
		http.Error(w, "missing snap", 400)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file: "+err.Error(), 400)
		return
	}
	defer file.Close()

	// Read file bytes
	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "read error: "+err.Error(), 500)
		return
	}

	snapsLock.Lock()
	s, ok := snaps[snapName]
	snapsLock.Unlock()
	if !ok {
		http.Error(w, "snap not found", 404)
		return
	}

	// Protocol:
	// MASTER → SNAP: "sendfile|<filename>|<size>\n"
	// MASTER → SNAP: <binary data>
	filename := filepath.Base(header.Filename)
	size := len(data)
	header2 := fmt.Sprintf("sendfile|%s|%d\n", filename, size)
	s.Conn.Write([]byte(header2))
	s.Conn.Write(data)

	logAction("Sent file '%s' (%d bytes) to %s", filename, size, snapName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "ok",
		"filename": filename,
		"size":     strconv.Itoa(size),
		"snap":     snapName,
	})
}

// POST /api/mapreduce?query=hello  — sends query to ALL snaps, collects results
func mapReduceHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		query = r.FormValue("query")
	}
	if query == "" {
		http.Error(w, "missing query", 400)
		return
	}

	snapsLock.Lock()
	snapNames := []string{}
	for name := range snaps {
		snapNames = append(snapNames, name)
	}
	snapsLock.Unlock()

	if len(snapNames) == 0 {
		http.Error(w, "no snaps connected", 503)
		return
	}

	// Send query to all snaps
	for _, name := range snapNames {
		sendCommand(name, "mapreduce|"+query)
		logAction("MapReduce query '%s' sent to %s", query, name)
	}

	// Collect results with timeout
	results := []MapResult{}
	timeout := time.After(10 * time.Second)
	expected := len(snapNames)
	received := 0

	for received < expected {
		select {
		case result := <-mapResultChan:
			results = append(results, result)
			received++
		case <-timeout:
			logAction("MapReduce timeout: got %d/%d results", received, expected)
			goto done
		}
	}
done:
	// Reduce: merge all results
	totalCount := 0
	allLines := []string{}
	for _, r := range results {
		totalCount += r.Count
		allLines = append(allLines, r.Lines...)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"query":      query,
		"totalCount": totalCount,
		"allLines":   allLines,
		"results":    results,
	})
}

// GET /api/hash?key=somekey — show which snap handles this key
func hashHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", 400)
		return
	}

	node := hashRing.GetNode(key)
	h := md5.Sum([]byte(key))
	hashVal := fmt.Sprintf("%08x", uint32(h[0])<<24|uint32(h[1])<<16|uint32(h[2])<<8|uint32(h[3]))

	// Build ring info
	snapsLock.Lock()
	snapNames := []string{}
	for name := range snaps {
		snapNames = append(snapNames, name)
	}
	snapsLock.Unlock()

	snapHashes := []map[string]string{}
	for _, name := range snapNames {
		sh := md5.Sum([]byte(name + "#0"))
		snapHashes = append(snapHashes, map[string]string{
			"name": name,
			"hash": fmt.Sprintf("%08x", uint32(sh[0])<<24|uint32(sh[1])<<16|uint32(sh[2])<<8|uint32(sh[3])),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":        key,
		"hash":       hashVal,
		"assignedTo": node,
		"snapHashes": snapHashes,
	})
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

func main() {
	var err error
	logFile, err = os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Warning: could not open log file:", err)
	} else {
		defer logFile.Close()
	}

	go startTCPServer()

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/api/snaps", apiSnaps)
	http.HandleFunc("/api/shutdown", shutdownHandler)
	http.HandleFunc("/api/background", backgroundHandler)
	http.HandleFunc("/api/sendfile", sendFileHandler)
	http.HandleFunc("/api/mapreduce", mapReduceHandler)
	http.HandleFunc("/api/hash", hashHandler)

	logAction("HTTP dashboard on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}