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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────
//  Data structures
// ─────────────────────────────────────────────

type Client struct {
	IP       string
	Conn     net.Conn
	mu       sync.Mutex
	resultCh chan string
}

func (c *Client) Send(msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := fmt.Fprintln(c.Conn, msg)
	return err
}

func (c *Client) SendRaw(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.Conn.Write(data)
	return err
}

type ClientStore struct {
	mu      sync.RWMutex
	clients map[string]*Client
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

	miningMu     sync.Mutex
	activeMining *miningSession
)

type miningJobPayload struct {
	Text       string `json:"text"`
	Difficulty int    `json:"difficulty"`
	NonceStart int    `json:"nonce_start"`
	NonceEnd   int    `json:"nonce_end"`
	WorkerID   int    `json:"worker_id"`
}

type miningSession struct {
	clients         []*Client
	expectedWorkers int
	found           sync.WaitGroup
	foundOnce       sync.Once
	mu              sync.Mutex
	result          map[string]any
	exhausted       int
	statsMu         sync.Mutex
	stats           map[int]int64
	startTime       time.Time
}

func newMiningSession(clients []*Client) *miningSession {
	s := &miningSession{
		clients:         clients,
		expectedWorkers: len(clients),
		stats:           make(map[int]int64),
	}
	s.found.Add(1)
	s.startTime = time.Now()
	return s
}

func (s *miningSession) broadcastStop() {
	logEvent("Mining: broadcasting STOP to all workers")
	for _, c := range s.clients {
		if err := c.Send("STOP"); err != nil {
			logEvent("Mining: STOP send error to %s: %v", c.IP, err)
		}
	}
}

func (s *miningSession) signalDone() {
	s.foundOnce.Do(func() { s.found.Done() })
}

func (s *miningSession) onWorkerLine(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	parts := strings.Split(line, "|")
	if len(parts) == 0 {
		return
	}
	kind := strings.ToUpper(parts[0])

	switch kind {
	case "PROGRESS":
		if len(parts) >= 4 {
			wid, _ := strconv.Atoi(parts[1])
			nonce, _ := strconv.Atoi(parts[2])
			attempts, _ := strconv.ParseInt(parts[3], 10, 64)
			logEvent("Mining progress: worker=%d nonce=%d attempts_in_range=%d", wid, nonce, attempts)
		}
	case "FOUND":
		if len(parts) >= 5 {
			wid, _ := strconv.Atoi(parts[1])
			nonce, _ := strconv.Atoi(parts[2])
			digest := parts[3]
			attempts, _ := strconv.ParseInt(parts[4], 10, 64)
			logEvent("Mining FOUND: worker=%d nonce=%d hash=%s attempts=%d", wid, nonce, digest, attempts)
			s.mu.Lock()
			if s.result == nil {
				s.result = map[string]any{
					"worker_id":       wid,
					"nonce":           nonce,
					"hash":            digest,
					"finder_attempts": attempts,
				}
				s.mu.Unlock()
				s.broadcastStop()
				s.signalDone()
			} else {
				s.mu.Unlock()
			}
		}
	case "EXHAUSTED":
		if len(parts) >= 3 {
			wid, _ := strconv.Atoi(parts[1])
			attempts, _ := strconv.ParseInt(parts[2], 10, 64)
			logEvent("Mining EXHAUSTED: worker=%d attempts=%d", wid, attempts)
			s.mu.Lock()
			s.exhausted++
			done := s.exhausted >= s.expectedWorkers
			s.mu.Unlock()
			if done {
				s.broadcastStop()
				s.signalDone()
			}
		}
	case "STATS":
		if len(parts) >= 3 {
			wid, _ := strconv.Atoi(parts[1])
			attempts, _ := strconv.ParseInt(parts[2], 10, 64)
			s.statsMu.Lock()
			s.stats[wid] = attempts
			s.statsMu.Unlock()
			logEvent("Mining STATS: worker=%d total_attempts=%d", wid, attempts)
		}
	}
}

func (s *miningSession) waitForStats(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.statsMu.Lock()
		n := len(s.stats)
		s.statsMu.Unlock()
		if n >= s.expectedWorkers {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func isMiningSnapLine(line string) bool {
	u := strings.ToUpper(strings.TrimSpace(line))
	return strings.HasPrefix(u, "PROGRESS|") ||
		strings.HasPrefix(u, "FOUND|") ||
		strings.HasPrefix(u, "EXHAUSTED|") ||
		strings.HasPrefix(u, "STATS|")
}

func miningNonceRanges(n, rangeWidth int) [][2]int {
	ranges := make([][2]int, 0, n)
	start := 0
	for i := 0; i < n; i++ {
		end := start + rangeWidth - 1
		ranges = append(ranges, [2]int{start, end})
		start = end + 1
	}
	return ranges
}

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
//  TCP Server
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

func handleSnap(conn net.Conn) {
	ip := conn.RemoteAddr().String()
	client := &Client{
		IP:       ip,
		Conn:     conn,
		resultCh: make(chan string, 64),
	}
	store.Add(client)
	logEvent("Snap connected: %s", ip)

	defer func() {
		conn.Close()
		store.Remove(ip)
		logEvent("Snap disconnected: %s", ip)
	}()

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		miningMu.Lock()
		sess := activeMining
		miningMu.Unlock()
		if sess != nil && isMiningSnapLine(line) {
			sess.onWorkerLine(line)
			continue
		}
		if strings.HasPrefix(line, "RESULT|") || strings.HasPrefix(line, "HASHRESULT|") {
			select {
			case client.resultCh <- line:
				logEvent("Snap reply queued from %s: %s", ip, line)
			default:
				logEvent("Snap reply dropped (buffer full) from %s: %s", ip, line)
			}
			continue
		}
		logEvent("Message from %s: %s", ip, line)
	}
	if err := scanner.Err(); err != nil {
		logEvent("Snap read error %s: %v", ip, err)
	}
}

// ─────────────────────────────────────────────
//  MapReduce Helper (Split Lines)
// ─────────────────────────────────────────────

func splitLines(lines []string, workers int) [][]string {
	chunks := make([][]string, workers)
	for i := 0; i < workers; i++ {
		start := i * len(lines) / workers
		end := (i + 1) * len(lines) / workers
		chunks[i] = lines[start:end]
	}
	return chunks
}

// ─────────────────────────────────────────────
//  HTTP API handlers
// ─────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func handleListClients(w http.ResponseWriter, r *http.Request) {
	ips := store.IPs()
	jsonOK(w, map[string]interface{}{"clients": ips})
}

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

func handleSendFile(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(50 << 20)
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
	time.Sleep(50 * time.Millisecond)
	c.SendRaw(data)
}

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

func handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	qnorm, ok := normalizeMapReduceQuery(req.Query)
	if !ok {
		jsonErr(w, "supported queries: COUNT, SUM, AVG", 400)
		return
	}

	clients := store.All()
	if len(clients) == 0 {
		jsonOK(w, map[string]interface{}{
			"query":   qnorm,
			"results": []interface{}{},
			"total":   0,
			"message": "no connected snap nodes",
		})
		return
	}

	// 1. Read data.sql as raw lines
	content, err := os.ReadFile("data.sql")
	if err != nil {
		jsonErr(w, "failed to read data.sql: "+err.Error(), 500)
		return
	}
	lines := strings.Split(string(content), "\n")

	// 2. Split lines using the new function
	chunks := splitLines(lines, len(clients))
	sort.Slice(clients, func(i, j int) bool { return clients[i].IP < clients[j].IP })

	logEvent("MapReduce: distributing %d lines to %d Snap node(s)", len(lines), len(clients))

	type nodeOut struct {
		IP       string `json:"ip"`
		Value    int    `json:"value"`
		LocalSum int    `json:"local_sum,omitempty"`
		LocalCnt int    `json:"local_count,omitempty"`
		Raw      string `json:"raw,omitempty"`
		Error    string `json:"error,omitempty"`
	}

	outCh := make(chan nodeOut, len(clients))
	for i, c := range clients {
		go func(idx int, cl *Client) {
			drainSnapReplies(cl)

			// Send DATA chunk (as JSON array of strings) to slave
			dataJSON, _ := json.Marshal(chunks[idx])
			if err := cl.Send("DATA|" + string(dataJSON)); err != nil {
				logEvent("MapReduce: send DATA failed to %s: %v", cl.IP, err)
				outCh <- nodeOut{IP: cl.IP, Error: err.Error()}
				return
			}

			// Send QUERY command
			cmd := "QUERY|" + qnorm
			if err := cl.Send(cmd); err != nil {
				logEvent("MapReduce: send %s failed to %s: %v", cmd, cl.IP, err)
				outCh <- nodeOut{IP: cl.IP, Error: err.Error()}
				return
			}
			logEvent("MapReduce: broadcast %s → %s (chunk size: %d)", cmd, cl.IP, len(chunks[idx]))

			select {
			case line := <-cl.resultCh:
				line = strings.TrimSpace(line)
				logEvent("MapReduce: local result from %s: %s", cl.IP, line)
				rep, err := parseSnapMapReduceLine(line, qnorm)
				if err != nil {
					outCh <- nodeOut{IP: cl.IP, Raw: line, Error: err.Error()}
					return
				}
				switch rep.Kind {
				case "COUNT", "SUM":
					outCh <- nodeOut{IP: cl.IP, Value: rep.N1}
				case "AVG":
					outCh <- nodeOut{IP: cl.IP, Value: rep.N2, LocalSum: rep.N1, LocalCnt: rep.N2}
				}
			case <-time.After(15 * time.Second):
				logEvent("MapReduce: timeout waiting for reply from %s", cl.IP)
				outCh <- nodeOut{IP: cl.IP, Error: "timeout waiting for RESULT"}
			}
		}(i, c)
	}

	rows := make([]nodeOut, 0, len(clients))
	for i := 0; i < len(clients); i++ {
		rows = append(rows, <-outCh)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].IP < rows[j].IP })

	var details []map[string]interface{}
	totalSumAll, totalCntAll := 0, 0
	for _, row := range rows {
		m := map[string]interface{}{"ip": row.IP}
		if row.Error != "" {
			m["error"] = row.Error
			if row.Raw != "" {
				m["raw"] = row.Raw
			}
			details = append(details, m)
			continue
		}
		m["value"] = row.Value
		if qnorm == "AVG" {
			m["local_sum"] = row.LocalSum
			m["local_count"] = row.LocalCnt
			totalSumAll += row.LocalSum
			totalCntAll += row.LocalCnt
		} else {
			totalSumAll += row.Value
		}
		details = append(details, m)
	}

	resp := map[string]interface{}{
		"query":   qnorm,
		"results": details,
	}
	switch qnorm {
	case "COUNT", "SUM":
		resp["total"] = totalSumAll
		logEvent("MapReduce %s: aggregated total=%d", qnorm, totalSumAll)
	case "AVG":
		resp["total_sum"] = totalSumAll
		resp["total_count"] = totalCntAll
		var avg float64
		if totalCntAll > 0 {
			avg = float64(totalSumAll) / float64(totalCntAll)
		}
		resp["average"] = avg
		resp["total"] = totalSumAll
		logEvent("MapReduce AVG: total_sum=%d total_count=%d global_average=%.6f", totalSumAll, totalCntAll, avg)
	}

	jsonOK(w, resp)
}

func drainSnapReplies(cl *Client) {
	for {
		select {
		case <-cl.resultCh:
		default:
			return
		}
	}
}

func normalizeMapReduceQuery(q string) (string, bool) {
	u := strings.ToUpper(strings.TrimSpace(q))
	switch u {
	case "COUNT", "SUM", "AVG":
		return u, true
	default:
		return "", false
	}
}

type snapMRWire struct {
	Kind string
	N1   int
	N2   int
}

func parseSnapMapReduceLine(line, expectedQuery string) (snapMRWire, error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(strings.ToUpper(line), "RESULT|") {
		return snapMRWire{}, fmt.Errorf("expected RESULT|… got %q", line)
	}
	parts := strings.Split(line, "|")
	if len(parts) < 2 {
		return snapMRWire{}, fmt.Errorf("malformed RESULT")
	}
	kind := strings.ToUpper(parts[1])
	switch kind {
	case "ERROR":
		msg := ""
		if len(parts) > 2 {
			msg = strings.Join(parts[2:], "|")
		}
		return snapMRWire{}, fmt.Errorf("snap reported: %s", msg)
	case "COUNT", "SUM":
		if len(parts) < 3 {
			return snapMRWire{}, fmt.Errorf("missing numeric field")
		}
		if kind != expectedQuery {
			return snapMRWire{}, fmt.Errorf("query mismatch: got %s want %s", kind, expectedQuery)
		}
		v, err := strconv.Atoi(parts[2])
		if err != nil {
			return snapMRWire{}, err
		}
		return snapMRWire{Kind: kind, N1: v}, nil
	case "AVG":
		if expectedQuery != "AVG" {
			return snapMRWire{}, fmt.Errorf("query mismatch: got AVG want %s", expectedQuery)
		}
		if len(parts) < 4 {
			return snapMRWire{}, fmt.Errorf("AVG requires sum|count")
		}
		s, err1 := strconv.Atoi(parts[2])
		c, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			return snapMRWire{}, fmt.Errorf("invalid AVG numbers")
		}
		return snapMRWire{Kind: "AVG", N1: s, N2: c}, nil
	default:
		return snapMRWire{}, fmt.Errorf("unknown RESULT kind %q", kind)
	}
}

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

func handleMining(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, "POST only", 405)
		return
	}
	var req struct {
		Text       string `json:"text"`
		Difficulty int    `json:"difficulty"`
		RangeWidth int    `json:"range_width"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, "invalid JSON", 400)
		return
	}
	if req.Text == "" || req.Difficulty < 1 {
		jsonErr(w, "text and difficulty (>=1) required", 400)
		return
	}
	if req.RangeWidth < 1 {
		req.RangeWidth = 100_001
	}

	miningMu.Lock()
	if activeMining != nil {
		miningMu.Unlock()
		jsonErr(w, "mining job already in progress", 409)
		return
	}
	raw := store.All()
	if len(raw) == 0 {
		miningMu.Unlock()
		jsonErr(w, "no connected snap nodes", 400)
		return
	}
	clients := append([]*Client(nil), raw...)
	sort.Slice(clients, func(i, j int) bool { return clients[i].IP < clients[j].IP })

	sess := newMiningSession(clients)
	activeMining = sess
	miningMu.Unlock()

	defer func() {
		miningMu.Lock()
		if activeMining == sess {
			activeMining = nil
		}
		miningMu.Unlock()
	}()

	text := req.Text
	diff := req.Difficulty
	ranges := miningNonceRanges(len(clients), req.RangeWidth)

	for i, c := range clients {
		id := i + 1
		ns, ne := ranges[i][0], ranges[i][1]
		if err := c.Send(fmt.Sprintf("WELCOME|%d", id)); err != nil {
			logEvent("Mining: WELCOME error %s: %v", c.IP, err)
		}
		job := miningJobPayload{
			Text:       text,
			Difficulty: diff,
			NonceStart: ns,
			NonceEnd:   ne,
			WorkerID:   id,
		}
		b, err := json.Marshal(job)
		if err != nil {
			logEvent("Mining: marshal error: %v", err)
			sess.broadcastStop()
			sess.signalDone()
			jsonErr(w, err.Error(), 500)
			return
		}
		if err := c.Send("MINE|" + string(b)); err != nil {
			logEvent("Mining: MINE send error %s: %v", c.IP, err)
		}
		logEvent("Mining: worker %d range [%d,%d] → %s", id, ns, ne, c.IP)
	}

	sess.found.Wait()
	sess.waitForStats(3 * time.Second)
	elapsed := time.Since(sess.startTime).Seconds()

	sess.mu.Lock()
	result := sess.result
	sess.mu.Unlock()

	sess.statsMu.Lock()
	var totalAttempts int64
	for _, v := range sess.stats {
		totalAttempts += v
	}
	sess.statsMu.Unlock()

	if totalAttempts == 0 && result != nil {
		if fa, ok := result["finder_attempts"].(int64); ok {
			totalAttempts = fa
		}
	}

	out := map[string]interface{}{
		"elapsed_sec":    elapsed,
		"workers":        len(clients),
		"total_attempts": totalAttempts,
	}
	if result != nil && result["hash"] != nil {
		out["hash"] = result["hash"]
		out["nonce"] = result["nonce"]
		out["finder_worker_id"] = result["worker_id"]
	} else {
		out["message"] = "no valid hash in assigned ranges (all workers exhausted)"
	}
	logEvent("Mining finished: attempts=%d elapsed=%.4fs", totalAttempts, elapsed)
	jsonOK(w, out)
}

func resolveTargets(target string) []*Client {
	if strings.ToUpper(target) == "ALL" {
		return store.All()
	}
	if c, ok := store.Get(target); ok {
		return []*Client{c}
	}
	return nil
}

func main() {
	initLogger()
	defer logFile.Close()

	logEvent("Master node starting...")

	go startTCPServer("9000")

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("static", "dashboard.html"))
	})

	mux.HandleFunc("/api/clients", handleListClients)
	mux.HandleFunc("/api/shutdown", handleShutdown)
	mux.HandleFunc("/api/sendfile", handleSendFile)
	mux.HandleFunc("/api/wallpaper", handleWallpaper)
	mux.HandleFunc("/api/query", handleQuery)
	mux.HandleFunc("/api/hash", handleHash)
	mux.HandleFunc("/api/mining", handleMining)

	logEvent("HTTP dashboard on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		logger.Fatal("HTTP server error:", err)
	}
}
