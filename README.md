# TASKDDB-final — Distributed Database & Control System

A complete distributed system built in **Go**, featuring a Master node that controls multiple Snap worker nodes via TCP sockets, with a modern web dashboard for real-time management.

---

## Project Structure

```
TASKDDB-final/
│
├── master/
│   ├── master.go          # Master node: HTTP API + TCP server
│   ├── static/
│   │   └── dashboard.html # Web dashboard UI
│   ├── go.mod
│   └── log.txt            # Auto-generated event log
│
├── snap/
│   ├── snap.go            # Snap worker node
│   └── go.mod
│
└── README.md
```

---

## Features

### Task 1 — Remote Control
| Feature | Description |
|---|---|
| **Shutdown** | Send a shutdown command to one or ALL connected nodes |
| **Send File** | Transfer any file via TCP to selected node(s) |
| **Change Wallpaper** | Push an image and set it as the desktop wallpaper |

### Task 2 — MapReduce
- Broadcast a query (COUNT / SUM / AVG) to all connected Snap nodes
- Each node computes a local result independently
- Master aggregates all results and displays the total

### Task 3 — SHA-256 Hashing
- User provides text + difficulty level
- Master continuously generates SHA-256 hashes with incrementing nonce
- Stops when hash starts with the required number of leading zeros
- Reports final hash and number of attempts

---

## Installation

### Prerequisites
- [Go 1.21+](https://go.dev/dl/)

### Clone / Setup

```bash
# No external dependencies — uses Go standard library only
cd TASKDDB-final
```

---

## How to Run

### 1. Start the Master Node

```bash
cd master
go run master.go
```

- **HTTP Dashboard**: http://localhost:8080
- **TCP Port**: 9000 (Snap nodes connect here)
- **Log file**: `master/log.txt`

### 2. Start a Snap Node

```bash
cd snap
go run snap.go [master-address]
```

**Examples:**

```bash
# Connect to master on same machine
go run snap.go 127.0.0.1:9000

# Connect to master on network
go run snap.go 192.168.1.100:9000
```

You can run **multiple Snap instances** on different machines — each will appear automatically in the dashboard.

---

## Example Usage

### Shutdown a device
1. Open dashboard → Remote Commands
2. Select target from dropdown (or ALL)
3. Click **SEND SHUTDOWN**

### Send a file
1. Remote Commands tab → Send File
2. Click "Choose a file" and select any file
3. Choose target node
4. Click **TRANSFER FILE**

### MapReduce query
1. MapReduce tab
2. Select COUNT / SUM / AVG (or type custom query)
3. Click **EXECUTE QUERY**
4. View per-node results + aggregate total

### SHA-256 Hashing
1. Hashing tab
2. Enter text (e.g. `hello world`)
3. Set difficulty (e.g. `4` = hash must start with `0000`)
4. Click **START HASHING**
5. View final hash and attempts count

---

## Communication Protocol

| Command | Format | Direction |
|---|---|---|
| Shutdown | `SHUTDOWN` | Master → Snap |
| File transfer | `FILE\|filename\|size` + raw bytes | Master → Snap |
| Wallpaper | `WALLPAPER\|filename\|size` + raw bytes | Master → Snap |
| MapReduce query | `QUERY\|COUNT` | Master → Snap |
| Query result | `RESULT\|120` | Snap → Master |
| Hash command | `HASH\|text\|difficulty` | Master → Snap |
| Hash result | `HASHRESULT\|hash\|attempts` | Snap → Master |

---

## Dashboard Sections

| Tab | Features |
|---|---|
| **Devices** | Node IP list, status badges, select all checkbox |
| **Remote** | Shutdown, send file, change wallpaper |
| **MapReduce** | Query input, execute, per-node + total results |
| **Hashing** | Text input, difficulty, hash result, attempts count |
| **Logs** | Real-time activity log from dashboard actions |

---

## Architecture

```
[Browser] ←HTTP→ [master.go :8080]
                        │
                    [TCP :9000]
                   /     |     \
           [snap1]  [snap2]  [snap3]
```

- Master uses **goroutines** to handle each Snap connection concurrently
- Master uses **sync.Mutex** for thread-safe client store access
- Snap nodes **auto-reconnect** every 5 seconds on disconnect
- Dashboard **auto-refreshes** node list every 5 seconds

---

## Notes

- Wallpaper change uses `gsettings` on Linux, `osascript` on macOS, PowerShell on Windows
- SHA-256 hashing is capped at 10,000,000 attempts (difficulty ≤ 7 recommended)
- Files are saved to `received_files/` directory on the Snap node
- All events are logged to `log.txt` on the Master

---

*Built with Go standard library only — no external dependencies.*
