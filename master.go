package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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
//  Data structures
// ─────────────────────────────────────────────

// Client represents a connected Snap node
type Client struct {
	IP   string
	Conn net.Conn
	mu   sync.Mutex
}

// Send writes a line to the client safely
func (c *Client) Send(msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := fmt.Fprintln(c.Conn, msg)
	return err
}

// SendRaw writes raw bytes to the client safely
func (c *Client) SendRaw(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.Conn.Write(data)
	return err
}

// ClientStore manages all connected clients thread-safely
type ClientStore struct {
	mu      sync.RWMutex
	clients map[string]*Client // key = IP:port
}

func NewClientStore() *ClientStore {
	return &ClientStore{clients: make(map[string]*Client)}
}

func (s *ClientStore) Add(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[c.IP] = c
}

func (s *ClientStore) Remove(ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, ip)
}

func (s *ClientStore) Get(ip string) (*Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clients[ip]
	return c, ok
}

func (s *ClientStore) All() []*Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*Client, 0, len(s.clients))
	for _, c := range s.clients {
		list = append(list, c)
	}
	return list
}

func (s *ClientStore) IPs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ips := make([]string, 0, len(s.clients))
	for ip := range s.clients {
		ips = append(ips, ip)
	}
	return ips
}

// ─────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────

var (
	store   = NewClientStore()
	logger  *log.Logger
	logFile *os.File
)

// ─────────────────────────────────────────────
//  Logging
// ─────────────────────────────────────────────

func initLogger() {
	var err error
	logFile, err = os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Cannot open log.txt:", err)
	}
	multi := io.MultiWriter(logFile, os.Stdout)
	logger = log.New(multi, "[MASTER] ", log.LstdFlags)
}

func logEvent(format string, args ...interface{}) {
	logger.Printf(format, args...)
}

// ─────────────────────────────────────────────
//  TCP Server – accept Snap connections
// ─────────────────────────────────────────────

func startTCPServer(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal("TCP listen error:", err)
	}
	logEvent("TCP server listening on port %s", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			logEvent("Accept error: %v", err)
			continue
		}
		go handleSnap(conn)
	}
}

// handleSnap manages one connected Snap node
func handleSnap(conn net.Conn) {
	ip := conn.RemoteAddr().String()
	client := &Client{IP: ip, Conn: conn}
	store.Add(client)
	logEvent("Snap connected: %s", ip)

	defer func() {
		conn.Close()
		store.Remove(ip)
		logEvent("Snap disconnected: %s", ip)
	}()

	// Just keep the connection alive; commands are pushed from HTTP handlers
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		logEvent("Message from %s: %s", ip, line)
	}
}

// ─────────────────────────────────────────────
//  HTTP API handlers
// ─────────────────────────────────────────────

// CORS + JSON helpers
func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// GET /api/clients  – list connected Snap nodes
func handleListClients(w http.ResponseWriter, r *http.Request) {
	ips := store.IPs()
	jsonOK(w, map[string]interface{}{"clients": ips})
}

// POST /api/shutdown  body: {"target":"ALL"} or {"target":"192.168.1.5:PORT"}
func handleShutdown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target string `json:"target"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	targets := resolveTargets(req.Target)
	if len(targets) == 0 {
		jsonErr(w, "no matching clients", 400)
		return
	}
	for _, c := range targets {
		c.Send("SHUTDOWN")
		logEvent("SHUTDOWN sent to %s", c.IP)
	}
	jsonOK(w, map[string]string{"status": "sent", "targets": fmt.Sprintf("%d", len(targets))})
}

// POST /api/sendfile  – multipart: target + file
func handleSendFile(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(50 << 20) // 50 MB
	target := r.FormValue("target")
	file, header, err := r.FormFile("file")
	if err != nil {
		jsonErr(w, "file required: "+err.Error(), 400)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		jsonErr(w, "read error", 500)
		return
	}

	targets := resolveTargets(target)
	if len(targets) == 0 {
		jsonErr(w, "no matching clients", 400)
		return
	}
	for _, c := range targets {
		sendFileTo(c, header.Filename, data)
		logEvent("FILE %s sent to %s", header.Filename, c.IP)
	}
	jsonOK(w, map[string]string{"status": "sent"})
}

func sendFileTo(c *Client, filename string, data []byte) {
	cmd := fmt.Sprintf("FILE|%s|%d", filename, len(data))
	c.Send(cmd)
	time.Sleep(50 * time.Millisecond) // let snap prepare
	c.SendRaw(data)
}

// POST /api/wallpaper  – multipart: target + file (image)
func handleWallpaper(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(50 << 20)
	target := r.FormValue("target")
	file, header, err := r.FormFile("file")
	if err != nil {
		jsonErr(w, "file required", 400)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		jsonErr(w, "read error", 500)
		return
	}

	targets := resolveTargets(target)
	if len(targets) == 0 {
		jsonErr(w, "no matching clients", 400)
		return
	}
	for _, c := range targets {
		cmd := fmt.Sprintf("WALLPAPER|%s|%d", header.Filename, len(data))
		c.Send(cmd)
		time.Sleep(50 * time.Millisecond)
		c.SendRaw(data)
		logEvent("WALLPAPER %s sent to %s", header.Filename, c.IP)
	}
	jsonOK(w, map[string]string{"status": "sent"})
}

// POST /api/query  body: {"query":"COUNT"}
func handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Query == "" {
		jsonErr(w, "query required", 400)
		return
	}

	clients := store.All()
	if len(clients) == 0 {
		jsonOK(w, map[string]interface{}{"total": 0, "results": []interface{}{}})
		return
	}

	type result struct {
		IP  string
		Val int
	}

	resultCh := make(chan result, len(clients))
	var wg sync.WaitGroup

	for _, c := range clients {
		wg.Add(1)
		go func(cl *Client) {
			defer wg.Done()
			// Send query command
			cl.Send("QUERY|" + req.Query)
			// Read response with timeout
			cl.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			defer cl.Conn.SetReadDeadline(time.Time{})

			scanner := bufio.NewScanner(cl.Conn)
			if scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "RESULT|") {
					parts := strings.SplitN(line, "|", 2)
					val, _ := strconv.Atoi(parts[1])
					resultCh <- result{IP: cl.IP, Val: val}
					logEvent("RESULT from %s: %s", cl.IP, parts[1])
				}
			}
		}(c)
	}

	wg.Wait()
	close(resultCh)

	total := 0
	var details []map[string]interface{}
	for r := range resultCh {
		total += r.Val
		details = append(details, map[string]interface{}{"ip": r.IP, "value": r.Val})
	}

	logEvent("MapReduce query '%s' total=%d", req.Query, total)
	jsonOK(w, map[string]interface{}{"total": total, "results": details})
}

// POST /api/hash  body: {"text":"hello","difficulty":3}
func handleHash(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text       string `json:"text"`
		Difficulty int    `json:"difficulty"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Text == "" || req.Difficulty < 1 {
		jsonErr(w, "text and difficulty required", 400)
		return
	}

	prefix := strings.Repeat("0", req.Difficulty)
	attempts := 0
	var finalHash string

	for i := 0; ; i++ {
		input := fmt.Sprintf("%d%s", i, req.Text)
		h := sha256.Sum256([]byte(input))
		hash := fmt.Sprintf("%x", h)
		attempts++
		if strings.HasPrefix(hash, prefix) {
			finalHash = hash
			break
		}
		// Safety cap: 10 million attempts
		if attempts > 10_000_000 {
			jsonErr(w, "max attempts reached", 500)
			return
		}
	}

	logEvent("HASH text='%s' difficulty=%d attempts=%d", req.Text, req.Difficulty, attempts)
	jsonOK(w, map[string]interface{}{
		"hash":     finalHash,
		"attempts": attempts,
	})
}

// ─────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────

// resolveTargets returns the clients matching the target string
func resolveTargets(target string) []*Client {
	if strings.ToUpper(target) == "ALL" {
		return store.All()
	}
	if c, ok := store.Get(target); ok {
		return []*Client{c}
	}
	return nil
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

func main() {
	initLogger()
	defer logFile.Close()

	logEvent("Master node starting...")

	// TCP server for Snap nodes on port 9000
	go startTCPServer("9000")

	// HTTP API + static dashboard on port 8080
	mux := http.NewServeMux()

	// Serve dashboard
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("static", "dashboard.html"))
	})

	// API routes
	mux.HandleFunc("/api/clients", handleListClients)
	mux.HandleFunc("/api/shutdown", handleShutdown)
	mux.HandleFunc("/api/sendfile", handleSendFile)
	mux.HandleFunc("/api/wallpaper", handleWallpaper)
	mux.HandleFunc("/api/query", handleQuery)
	mux.HandleFunc("/api/hash", handleHash)

	logEvent("HTTP dashboard on http://localhost:8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		logger.Fatal("HTTP server error:", err)
	}
}
