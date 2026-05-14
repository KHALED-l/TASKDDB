package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ─────────────────────────────────────────────
//  MapReduce Result
// ─────────────────────────────────────────────

type MapResult struct {
	SnapName string   `json:"snap"`
	Query    string   `json:"query"`
	Count    int      `json:"count"`
	Lines    []string `json:"lines"`
	Error    string   `json:"error,omitempty"`
}

// ─────────────────────────────────────────────
//  Data stored locally on this snap (for MapReduce)
// ─────────────────────────────────────────────

// Each snap has a local data file: data.txt
// MapReduce will search this file for the query string.

var localDataFile = "data.txt"

func searchLocalData(query string) MapResult {
	result := MapResult{Query: query, Lines: []string{}}

	file, err := os.Open(localDataFile)
	if err != nil {
		// Create sample data if not exists
		sampleData := []string{
			"hello world from snap",
			"distributed systems are fun",
			"mapreduce is powerful",
			"hello distributed database",
			"snap node is running",
			"data stored locally here",
		}
		os.WriteFile(localDataFile, []byte(strings.Join(sampleData, "\n")), 0644)
		file, err = os.Open(localDataFile)
		if err != nil {
			result.Error = err.Error()
			return result
		}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(strings.ToLower(line), strings.ToLower(query)) {
			result.Lines = append(result.Lines, line)
			result.Count++
		}
	}
	return result
}

// ─────────────────────────────────────────────
//  Connection
// ─────────────────────────────────────────────

var masterConn net.Conn

func main() {
	masterIP := "192.168.251.22" // ← غيّر الـ IP حسب جهازك
	port := 8082

	var conn net.Conn
	var err error

	for retries := 0; retries < 5; retries++ {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", masterIP, port))
		if err == nil {
			break
		}
		fmt.Println("🔄 Retrying connection...")
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		fmt.Println("❌ Failed to connect.")
		return
	}
	defer conn.Close()
	masterConn = conn

	fmt.Println("✅ Connected to master.")
	conn.Write([]byte("online|snap-01\n"))

	go listenForCommands(conn)

	showMenu(conn)
}

// ─────────────────────────────────────────────
//  Menu
// ─────────────────────────────────────────────

func showMenu(conn net.Conn) {
	for {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("🖼  1. Send image path to master")
		fmt.Println("📋 2. Show local data (MapReduce data)")
		fmt.Println("➕ 3. Add line to local data")
		fmt.Println("❌ 4. Exit")
		fmt.Print("Select: ")
		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			fmt.Print("Enter image path: ")
			var path string
			fmt.Scanln(&path)
			conn.Write([]byte("imgpath|" + path + "\n"))
		case 2:
			data, err := os.ReadFile(localDataFile)
			if err != nil {
				fmt.Println("No local data file yet.")
			} else {
				fmt.Println("\n📄 Local data:\n" + string(data))
			}
		case 3:
			fmt.Print("Enter line to add: ")
			reader := bufio.NewReader(os.Stdin)
			line, _ := reader.ReadString('\n')
			line = strings.TrimSpace(line)
			f, err := os.OpenFile(localDataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				f.WriteString(line + "\n")
				f.Close()
				fmt.Println("✅ Line added to local data.")
			}
		case 4:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option.")
		}
	}
}

// ─────────────────────────────────────────────
//  Command Listener
// ─────────────────────────────────────────────

func listenForCommands(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("🔌 Connection lost.")
			return
		}
		cmd = strings.TrimSpace(cmd)
		handleCommand(conn, reader, cmd)
	}
}

func handleCommand(conn net.Conn, reader *bufio.Reader, cmd string) {
	switch {
	case cmd == "shutdown":
		executeShutdown()

	case strings.HasPrefix(cmd, "background|"):
		path := strings.TrimPrefix(cmd, "background|")
		changeBackground(path)

	case strings.HasPrefix(cmd, "sendfile|"):
		// Format: sendfile|<filename>|<size>
		receiveFile(conn, reader, cmd)

	case strings.HasPrefix(cmd, "mapreduce|"):
		query := strings.TrimPrefix(cmd, "mapreduce|")
		fmt.Printf("📊 Running MapReduce for query: '%s'\n", query)
		result := searchLocalData(query)
		result.SnapName = "snap-01"

		jsonBytes, err := json.Marshal(result)
		if err != nil {
			fmt.Println("❌ JSON marshal error:", err)
			return
		}
		conn.Write([]byte("mapresult|" + string(jsonBytes) + "\n"))
		fmt.Printf("✅ Sent MapReduce result: %d matches found\n", result.Count)

	default:
		fmt.Println("❓ Unknown command:", cmd)
	}
}

// ─────────────────────────────────────────────
//  Receive File from Master
// ─────────────────────────────────────────────

func receiveFile(conn net.Conn, reader *bufio.Reader, header string) {
	// header: sendfile|<filename>|<size>
	parts := strings.SplitN(header, "|", 3)
	if len(parts) < 3 {
		fmt.Println("❌ Invalid sendfile header")
		return
	}
	filename := parts[1]
	var size int
	fmt.Sscanf(parts[2], "%d", &size)

	fmt.Printf("📥 Receiving file: %s (%d bytes)\n", filename, size)

	data := make([]byte, size)
	received := 0
	for received < size {
		n, err := reader.Read(data[received:])
		if err != nil {
			fmt.Println("❌ Error receiving file:", err)
			return
		}
		received += n
	}

	// Save to received_files/ directory
	os.MkdirAll("received_files", 0755)
	savePath := filepath.Join("received_files", filename)
	err := os.WriteFile(savePath, data, 0644)
	if err != nil {
		fmt.Println("❌ Failed to save file:", err)
		return
	}
	fmt.Printf("✅ File saved to: %s\n", savePath)
}

// ─────────────────────────────────────────────
//  System Commands
// ─────────────────────────────────────────────

func executeShutdown() {
	fmt.Println("🔻 Executing shutdown...")
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("shutdown", "/s", "/t", "0")
	} else {
		cmd = exec.Command("shutdown", "-h", "now")
	}
	err := cmd.Run()
	if err != nil {
		fmt.Println("❌ Shutdown failed:", err)
	}
}

func changeBackground(path string) {
	fmt.Println("🎨 Changing background to:", path)
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("powershell", "-Command",
			fmt.Sprintf(`Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public class Wallpaper {
    [DllImport("user32.dll", CharSet = CharSet.Auto)]
    public static extern int SystemParametersInfo(int uAction, int uParam, string lpvParam, int fuWinIni);
}
'@; [Wallpaper]::SystemParametersInfo(20, 0, '%s', 3)`, path))
	} else if runtime.GOOS == "linux" {
		cmd = exec.Command("gsettings", "set", "org.gnome.desktop.background", "picture-uri", "file://"+path)
	} else if runtime.GOOS == "darwin" {
		appleScript := fmt.Sprintf(`tell application "System Events" to set picture of every desktop to "%s"`, path)
		cmd = exec.Command("osascript", "-e", appleScript)
	}
	err := cmd.Run()
	if err != nil {
		fmt.Println("❌ Failed to change background:", err)
	} else {
		fmt.Println("✅ Background changed to:", path)
	}
}