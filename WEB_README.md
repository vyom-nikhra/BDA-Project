# HDFS Web Interface

A modern web interface for the Hadoop Distributed File System (HDFS) simulation built with Streamlit.

## Features

- 🎛️ **Cluster Management**: Start and stop the HDFS cluster with one click
- 📊 **Real-time Dashboard**: Monitor the status of NameNode and SlaveNodes
- 📤 **File Upload**: Upload files to HDFS through a web interface
- 📥 **File Download**: Retrieve and download files from HDFS
- 📋 **File Management**: View all files stored in the distributed file system

## Quick Start

1. **Start the Web Interface:**
   ```bash
   ./.venv/bin/streamlit run hdfs_web_app.py
   ```

2. **Open your browser** and navigate to `http://localhost:8501`

3. **Start the HDFS cluster** using the "🚀 Start Cluster" button in the sidebar

4. **Upload files** using the Upload tab and **download** them using the Download tab

## Architecture

The web interface manages a distributed file system with:
- **1 NameNode** (Master) - Manages metadata and coordinates operations
- **4 SlaveNodes** (Workers) - Store actual file blocks

## File Operations

### Upload Process
1. File is split into 32MB blocks
2. NameNode provides write plan (which SlaveNodes to use)
3. Blocks are distributed across SlaveNodes
4. Metadata is stored in NameNode

### Download Process  
1. NameNode provides read plan (where blocks are located)
2. Blocks are retrieved from respective SlaveNodes
3. File is reassembled from blocks
4. Complete file is available for download

## Requirements

- Python 3.8+
- Streamlit
- Flask
- Requests

## Notes

- The web interface automatically manages the cluster lifecycle
- All servers run on localhost with different ports (5000-5004)
- Files are temporarily stored during upload/download operations
- The cluster should be stopped when done to free up system resources
