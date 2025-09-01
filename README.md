# Read/Write in HDFS – Simulation (Python + Flask)

A functional, small-scale simulation of the Hadoop Distributed File System (HDFS). It models how a **NameNode** orchestrates storage/retrieval of large files by splitting them into blocks and distributing them across multiple **SlaveNodes**. A simple REST API (Flask) connects the components.

**🌟 NEW:** Now includes a modern **Streamlit web interface** for easy file management and cluster monitoring!

---

## ✨ Features

* Split large files into fixed-size blocks
* Distribute blocks across multiple slave nodes
* Track block locations via a NameNode
* Add (write) and read (reconstruct) files through a client
* **🌐 Web Interface**: Modern Streamlit-based GUI for file management
* **📊 Real-time Monitoring**: Dashboard showing cluster status and node health
* **📤 File Upload**: Drag-and-drop file upload to HDFS through web browser
* **📥 File Download**: One-click download of files from HDFS
* One-click automation script to start, test, and shut down everything

---

## 🧱 Components

* **`namenode.py`** – central directory service that tracks which node stores which block.
* **`slavenode.py`** – storage server that holds blocks and serves them on request.
* **`client.py`** – CLI client to `add` files (write) and `read` files (reconstruct).
* **`run_simulation.py`** – automation script to run end‑to‑end demo.
* **`hdfs_web_app.py`** – 🌐 **NEW**: Streamlit web interface for HDFS management.

> The Flask apps expose REST endpoints so components can communicate locally.

---

## 🚀 Quick Start

### Option 1: 🌐 Web Interface (Recommended)

The easiest way to interact with HDFS is through the modern web interface:

1. **Install Dependencies:**

   ```bash
   # Set up virtual environment (recommended)
   python3 -m venv venv
   source venv/bin/activate  # On Windows: .venv\Scripts\activate

   # Install required packages
   pip install -r requirements.txt
   # OR install individually:
   # pip install streamlit flask requests
   ```
2. **Start the Web Interface:**

   ```bash
   streamlit run hdfs_web_app.py
   ```
3. **Open in Browser:**
   Navigate to `http://localhost:8501` in your web browser
4. **Use the Interface:**

   - Click "🚀 Start Cluster" in the sidebar to launch HDFS nodes
   - Use the "📤 Upload File" tab to add files to HDFS
   - Use the "📥 Download File" tab to retrieve files from HDFS
   - Monitor cluster status in the "📊 Dashboard" tab
   - View all stored files in the "📋 File List" tab

### Option 2: 🖥️ Command Line (Automated)

Use the automation script to start all servers, run a write + read cycle, and shut everything down.

#### Prerequisites

* **Python 3.6+**
* Required libraries (install once):

```bash
pip install Flask requests
# OR using system packages:
sudo apt install python3-flask python3-requests
```

#### Run

1. Place the file you want to add to HDFS (e.g., `my_large_file.bin`, \~100 MB) in your working directory.
2. Open **one** terminal and run:

```bash
python run_simulation.py my_large_file.bin
```

The script will:

* Start the NameNode and four SlaveNodes
* Add the file via the client
* Read the file back to verify integrity
* Shut down all servers cleanly

On success, you’ll see a new file in your directory:

```
retrieved_my_large_file.bin
```

### Option 3: 🧰 Manual Execution (Advanced)

Run components individually to observe how they interact. You will need **multiple terminals**.

### 1) Start the NameNode

**Terminal 1**:

```bash
python namenode.py
```

(Leave this running.)

### 2) Start the SlaveNodes

Start four slave nodes in **separate** terminals.

**Terminal 2**:

```bash
python slavenode.py 5001 slavenode_1
```

**Terminal 3**:

```bash
python slavenode.py 5002 slavenode_2
```

**Terminal 4**:

```bash
python slavenode.py 5003 slavenode_3
```

**Terminal 5**:

```bash
python slavenode.py 5004 slavenode_4
```

(Leave all of these running.)

### 3) Use the Client

Open a **6th** terminal for client operations.

**Add a file**

```bash
python client.py add my_large_file.bin
```

**Read a file**

```bash
python client.py read my_large_file.bin retrieved_file.bin
```

> **Important:** When running manually, stop servers yourself with **Ctrl+C** in each server terminal when finished.

---

## 📦 Data Flow (At a Glance)

1. Client sends **add** request to NameNode.
2. NameNode splits file → computes block placements across SlaveNodes.
3. Client uploads blocks directly to the designated SlaveNodes.
4. Metadata (file → list of blocks → node locations) is stored at the NameNode.
5. For **read**, Client requests block map from NameNode, fetches blocks from SlaveNodes, and reconstructs the file.

---

## 🧪 Tips & Troubleshooting

* If a port is already in use, stop previous runs or change the port arguments for `slavenode.py`.
* Large files: ensure you have enough disk space for both the original and retrieved copies.
* Firewalls/antivirus can block local HTTP calls—allow local connections if prompted.
* If you modify block size or replication logic in code, clear any previous temp/storage directories to avoid stale state.

---

## 📁 Project Structure

```
.
├── client.py                   # CLI client for HDFS operations
├── namenode.py                 # NameNode server (metadata management)
├── slavenode.py               # SlaveNode server (block storage)
├── run_simulation.py          # Command-line automation script
├── hdfs_web_app.py           # 🌐 Streamlit web interface
├── WEB_README.md             # Web interface documentation
├── requirements.txt           # Python dependencies (optional)
└── README.md                  # This file
```

## 🌐 Web Interface Features

The Streamlit web interface provides:

- **📊 Real-time Dashboard**: Monitor NameNode and SlaveNode status
- **🎛️ Cluster Management**: Start/stop HDFS cluster with one click
- **📤 File Upload**: Drag-and-drop file upload with progress tracking
- **📥 File Download**: Browse and download files from HDFS
- **📋 File Management**: View all stored files and cluster statistics
- **💡 User-Friendly**: No command-line knowledge required

## 🔧 API Endpoints

### NameNode (Port 5000)

- `GET /` - Health check
- `POST /get_write_plan` - Get block placement plan for writing
- `POST /commit_write` - Commit file metadata after writing
- `GET /get_read_plan?filename=<name>` - Get block locations for reading
- `GET /list_files` - List all files in HDFS

### SlaveNode (Ports 5001-5004)

- `GET /` - Health check
- `POST /write_block/<block_id>` - Store a block
- `GET /read_block/<block_id>` - Retrieve a block
