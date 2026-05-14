package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────

var (
	masterAddr = "127.0.0.1:9000" // override via CLI arg
	logger     *log.Logger
)

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

	logger.Printf("Snap node starting. Connecting to master at %s", masterAddr)

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

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		logger.Printf("Command received: %s", line)
		handleCommand(line, reader, conn)
	}
}

// ─────────────────────────────────────────────
//  Command dispatcher
// ─────────────────────────────────────────────

func handleCommand(line string, reader *bufio.Reader, conn net.Conn) {
	parts := strings.SplitN(line, "|", -1)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
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
		handleQuery(query, conn)

	case "HASH":
		if len(parts) < 3 {
			logger.Println("Invalid HASH command")
			return
		}
		text := parts[1]
		difficulty, _ := strconv.Atoi(parts[2])
		handleHash(text, difficulty, conn)

	default:
		logger.Printf("Unknown command: %s", cmd)
	}
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

	// Save to received_files directory
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
		// Use PowerShell to set wallpaper on Windows
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
//  QUERY (MapReduce – local compute)
// ─────────────────────────────────────────────

func handleQuery(query string, conn net.Conn) {
	logger.Printf("Processing query: %s", query)

	var result int
	switch strings.ToUpper(query) {
	case "COUNT":
		// Return a simulated local record count (100–500)
		result = 100 + rand.Intn(400)
	case "SUM":
		result = 1000 + rand.Intn(9000)
	case "AVG":
		result = 10 + rand.Intn(90)
	default:
		result = rand.Intn(200)
	}

	response := fmt.Sprintf("RESULT|%d\n", result)
	conn.Write([]byte(response))
	logger.Printf("Query result sent: %d", result)
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
