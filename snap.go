package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────

var (
	masterAddr = "127.0.0.1:9000" // override: argv[1]
	logger     *log.Logger
	localData  []string // سنخزن هنا خطوط الـ SQL التي يرسلها الـ Master
)

const miningProgressEvery = 250_000

// connState is per-TCP-connection data (e.g. distributed mining worker id).
type connState struct {
	workerID int
}

type mineJobPayload struct {
	Text       string `json:"text"`
	Difficulty int    `json:"difficulty"`
	NonceStart int    `json:"nonce_start"`
	NonceEnd   int    `json:"nonce_end"`
	WorkerID   int    `json:"worker_id"`
}

func init() {
	logger = log.New(os.Stdout, "[SNAP] ", log.LstdFlags)
}

// ─────────────────────────────────────────────
//  Main – connect and loop
// ─────────────────────────────────────────────

func main() {
	if len(os.Args) > 1 {
		masterAddr = os.Args[1]
	}
	logger.Printf("Snap node starting: connecting to %s", masterAddr)

	for {
		if err := connectAndRun(); err != nil {
			logger.Printf("Disconnected: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// connectAndRun establishes a TCP connection and processes commands
func connectAndRun() error {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	logger.Printf("Connected to master at %s", masterAddr)
	reader := bufio.NewReader(conn)
	state := &connState{}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		handleCommand(line, reader, conn, state)
	}
}

// ─────────────────────────────────────────────
//  Command dispatcher
// ─────────────────────────────────────────────

func handleCommand(line string, reader *bufio.Reader, conn net.Conn, st *connState) {
	parts := strings.SplitN(line, "|", -1)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "DATA":
		jsonRaw := ""
		if idx := strings.Index(line, "|"); idx >= 0 && idx+1 < len(line) {
			jsonRaw = strings.TrimSpace(line[idx+1:])
		}
		if jsonRaw != "" {
			var lines []string
			if err := json.Unmarshal([]byte(jsonRaw), &lines); err != nil {
				logger.Printf("Invalid DATA JSON: %v", err)
			} else {
				localData = lines
				logger.Printf("Received data chunk: %d lines", len(localData))
			}
		}
		return

	case "WELCOME":
		if len(parts) >= 2 {
			st.workerID, _ = strconv.Atoi(parts[1])
			logger.Printf("Mining welcome: assigned worker id=%d", st.workerID)
		}
		return

	case "MINE":
		jsonRaw := ""
		if idx := strings.Index(line, "|"); idx >= 0 && idx+1 < len(line) {
			jsonRaw = strings.TrimSpace(line[idx+1:])
		}
		if jsonRaw == "" {
			logger.Println("Invalid MINE command (empty payload)")
			return
		}
		runDistributedMine(jsonRaw, reader, conn, st)
		return

	case "SHUTDOWN":
		handleShutdown()

	case "FILE":
		if len(parts) < 3 {
			logger.Println("Invalid FILE command")
			return
		}
		filename := parts[1]
		size, _ := strconv.Atoi(parts[2])
		handleReceiveFile(filename, size, reader)

	case "WALLPAPER":
		if len(parts) < 3 {
			logger.Println("Invalid WALLPAPER command")
			return
		}
		filename := parts[1]
		size, _ := strconv.Atoi(parts[2])
		handleWallpaper(filename, size, reader, conn)

	case "QUERY":
		query := ""
		if len(parts) > 1 {
			query = parts[1]
		}
		handleMapReduceQuery(query, conn)

	case "HASH":
		if len(parts) < 3 {
			logger.Println("Invalid HASH command")
			return
		}
		text := parts[1]
		difficulty, _ := strconv.Atoi(parts[2])
		handleHash(text, difficulty, conn)

	default:
		// logger.Printf("Unknown command: %s", cmd)
	}
}

func sha256HexString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// runDistributedMine runs SHA256(text+decimal nonce) over [nonce_start, nonce_end] inclusive.
func runDistributedMine(jsonRaw string, reader *bufio.Reader, conn net.Conn, st *connState) {
	var job mineJobPayload
	if err := json.Unmarshal([]byte(jsonRaw), &job); err != nil {
		logger.Printf("Invalid MINE JSON: %v", err)
		return
	}

	wid := job.WorkerID
	st.workerID = wid
	text := job.Text
	difficulty := job.Difficulty
	ns, ne := job.NonceStart, job.NonceEnd

	prefix := strings.Repeat("0", difficulty)
	logger.Printf("Distributed mine: worker=%d difficulty=%d range=[%d,%d]", wid, difficulty, ns, ne)

	var stopped atomic.Bool
	go func() {
		for {
			ln, err := reader.ReadString('\n')
			if err != nil {
				stopped.Store(true)
				return
			}
			s := strings.TrimSpace(ln)
			if s == "" {
				continue
			}
			if strings.HasPrefix(strings.ToUpper(s), "STOP") {
				logger.Println("Stop signal from master; halting mining.")
				stopped.Store(true)
				return
			}
		}
	}()

	attempts := int64(0)
	found := false
	progressOn := miningProgressEvery > 0

	for nonce := ns; nonce <= ne; nonce++ {
		if stopped.Load() {
			break
		}
		payload := fmt.Sprintf("%s%d", text, nonce)
		h := sha256HexString(payload)
		attempts++
		if progressOn && attempts%int64(miningProgressEvery) == 0 {
			fmt.Fprintf(conn, "PROGRESS|%d|%d|%d\n", wid, nonce, attempts)
		}
		if strings.HasPrefix(h, prefix) {
			found = true
			logger.Printf("Valid hash: nonce=%d hash=%s", nonce, h)
			fmt.Fprintf(conn, "FOUND|%d|%d|%s|%d\n", wid, nonce, h, attempts)
			break
		}
	}

	if !found && !stopped.Load() {
		logger.Println("Exhausted assigned nonce range.")
		fmt.Fprintf(conn, "EXHAUSTED|%d|%d\n", wid, attempts)
	}

	deadline := time.Now().Add(30 * time.Second)
	for !stopped.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !stopped.Load() {
		logger.Println("Timed out waiting for STOP; sending STATS anyway.")
	}

	fmt.Fprintf(conn, "STATS|%d|%d\n", wid, attempts)
	logger.Printf("Mining finished; attempts in range=%d", attempts)
}

// ─────────────────────────────────────────────
//  SHUTDOWN
// ─────────────────────────────────────────────

func handleShutdown() {
	logger.Println("Executing SHUTDOWN")
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("shutdown", "/s", "/t", "0")
	case "darwin":
		cmd = exec.Command("sudo", "shutdown", "-h", "now")
	default: // linux
		cmd = exec.Command("sudo", "shutdown", "-h", "now")
	}
	if err := cmd.Run(); err != nil {
		logger.Printf("Shutdown error: %v", err)
	}
}

// ─────────────────────────────────────────────
//  FILE RECEIVE
// ─────────────────────────────────────────────

func handleReceiveFile(filename string, size int, reader *bufio.Reader) {
	logger.Printf("Receiving file: %s (%d bytes)", filename, size)

	os.MkdirAll("received_files", 0755)
	destPath := filepath.Join("received_files", filepath.Base(filename))

	data := make([]byte, size)
	_, err := io.ReadFull(reader, data)
	if err != nil {
		logger.Printf("Error reading file data: %v", err)
		return
	}

	if err := os.WriteFile(destPath, data, 0644); err != nil {
		logger.Printf("Error saving file: %v", err)
		return
	}
	logger.Printf("File saved to %s", destPath)
}

// ─────────────────────────────────────────────
//  WALLPAPER
// ─────────────────────────────────────────────

func handleWallpaper(filename string, size int, reader *bufio.Reader, conn net.Conn) {
	logger.Printf("Receiving wallpaper: %s (%d bytes)", filename, size)

	os.MkdirAll("received_files", 0755)
	destPath := filepath.Join("received_files", filepath.Base(filename))

	data := make([]byte, size)
	_, err := io.ReadFull(reader, data)
	if err != nil {
		logger.Printf("Error reading wallpaper data: %v", err)
		return
	}

	if err := os.WriteFile(destPath, data, 0644); err != nil {
		logger.Printf("Error saving wallpaper: %v", err)
		return
	}

	absPath, _ := filepath.Abs(destPath)
	setWallpaper(absPath)
}

func setWallpaper(path string) {
	logger.Printf("Setting wallpaper: %s", path)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		script := fmt.Sprintf(`Add-Type -TypeDefinition 'using System;using System.Runtime.InteropServices;public class W{[DllImport("user32.dll")]public static extern int SystemParametersInfo(int a,int b,string c,int d);}'; [W]::SystemParametersInfo(20,0,"%s",3)`, path)
		cmd = exec.Command("powershell", "-Command", script)
	case "darwin":
		script := fmt.Sprintf(`tell application "System Events" to tell every desktop to set picture to "%s"`, path)
		cmd = exec.Command("osascript", "-e", script)
	default: // linux (GNOME/KDE etc.)
		cmd = exec.Command("gsettings", "set", "org.gnome.desktop.background", "picture-uri", "file://"+path)
	}
	if err := cmd.Run(); err != nil {
		logger.Printf("Wallpaper set error: %v", err)
	} else {
		logger.Println("Wallpaper changed successfully")
	}
}

// ─────────────────────────────────────────────
//  MapReduce — local dataset, RESULT wire format
// ─────────────────────────────────────────────

func handleMapReduceQuery(query string, conn net.Conn) {
	q := strings.ToUpper(strings.TrimSpace(query))

	// Regex لاستخراج الراتب من سطر الـ SQL
	// مثال: INSERT INTO users (name, age, salary) VALUES ('Ahmed', 22, 5000);
	re := regexp.MustCompile(`VALUES\s*\(\s*'[^']*'\s*,\s*\d+\s*,\s*(\d+)\s*\)`)

	count := 0
	sum := 0

	for _, line := range localData {
		matches := re.FindStringSubmatch(line)
		if len(matches) > 1 {
			count++
			salary, _ := strconv.Atoi(matches[1])
			sum += salary
		}
	}

	switch q {
	case "COUNT":
		line := fmt.Sprintf("RESULT|COUNT|%d\n", count)
		conn.Write([]byte(line))
		logger.Printf("[MapReduce] COUNT → local_count=%d", count)

	case "SUM":
		line := fmt.Sprintf("RESULT|SUM|%d\n", sum)
		conn.Write([]byte(line))
		logger.Printf("[MapReduce] SUM → local_sum=%d", sum)

	case "AVG":
		line := fmt.Sprintf("RESULT|AVG|%d|%d\n", sum, count)
		conn.Write([]byte(line))
		var avg float64
		if count > 0 {
			avg = float64(sum) / float64(count)
		}
		logger.Printf("[MapReduce] AVG → sum=%d count=%d avg=%.6f", sum, count, avg)

	default:
		errLine := fmt.Sprintf("RESULT|ERROR|unsupported query %q\n", query)
		conn.Write([]byte(errLine))
	}
}

// ─────────────────────────────────────────────
//  HASH
// ─────────────────────────────────────────────

func handleHash(text string, difficulty int, conn net.Conn) {
	logger.Printf("Starting hash: text='%s' difficulty=%d", text, difficulty)

	prefix := strings.Repeat("0", difficulty)
	attempts := 0
	var finalHash string

	for i := 0; ; i++ {
		input := fmt.Sprintf("%d%s", i, text)
		h := sha256.Sum256([]byte(input))
		hash := fmt.Sprintf("%x", h)
		attempts++
		if strings.HasPrefix(hash, prefix) {
			finalHash = hash
			break
		}
		if attempts > 10_000_000 {
			break
		}
	}

	response := fmt.Sprintf("HASHRESULT|%s|%d\n", finalHash, attempts)
	conn.Write([]byte(response))
	logger.Printf("Hash found after %d attempts", attempts)
}
