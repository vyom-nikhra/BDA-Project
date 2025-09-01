import streamlit as st
import subprocess
import time
import os
import sys
import tempfile
import shutil
from client import add_file, read_file
import requests
from datetime import datetime

# --- Configuration ---
SERVER_COMMANDS = [
    [sys.executable, "namenode.py"],
    [sys.executable, "slavenode.py", "5001", "slavenode_1"],
    [sys.executable, "slavenode.py", "5002", "slavenode_2"],
    [sys.executable, "slavenode.py", "5003", "slavenode_3"],
    [sys.executable, "slavenode.py", "5004", "slavenode_4"],
]

NAMENODE_URL = "http://127.0.0.1:5000"
SLAVENODE_URLS = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002", 
    "http://127.0.0.1:5003",
    "http://127.0.0.1:5004"
]

# --- Helper Functions ---

def check_server_status(url):
    """Check if a server is running by making a simple request."""
    try:
        response = requests.get(f"{url}/", timeout=2)
        return True
    except:
        return False

def start_hdfs_cluster():
    """Start all HDFS servers."""
    if 'processes' not in st.session_state:
        st.session_state.processes = []
    
    # Kill any existing processes first
    stop_hdfs_cluster()
    
    processes = []
    with st.spinner("Starting HDFS cluster..."):
        for i, command in enumerate(SERVER_COMMANDS):
            log_filename = f"server_{i}.log"
            with open(log_filename, 'w') as log_file:
                proc = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
                processes.append(proc)
        
        st.session_state.processes = processes
        time.sleep(5)  # Give servers time to start
    
    return True

def stop_hdfs_cluster():
    """Stop all HDFS servers."""
    if 'processes' in st.session_state and st.session_state.processes:
        for proc in st.session_state.processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
        st.session_state.processes = []

def get_cluster_status():
    """Get the status of all cluster nodes."""
    namenode_status = check_server_status(NAMENODE_URL)
    slavenode_status = [check_server_status(url) for url in SLAVENODE_URLS]
    
    return {
        'namenode': namenode_status,
        'slavenodes': slavenode_status
    }

def get_file_list():
    """Get list of files stored in HDFS."""
    try:
        response = requests.get(f"{NAMENODE_URL}/list_files", timeout=5)
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []

# --- Streamlit UI ---

def main():
    st.set_page_config(
        page_title="HDFS Web Interface",
        page_icon="🗄️",
        layout="wide"
    )
    
    st.title("🗄️ HDFS Distributed File System")
    st.markdown("**Web Interface for Hadoop Distributed File System Simulation**")
    
    # Initialize session state
    if 'cluster_started' not in st.session_state:
        st.session_state.cluster_started = False
    
    # Sidebar - Cluster Control
    st.sidebar.header("🎛️ Cluster Control")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🚀 Start Cluster", type="primary"):
            if start_hdfs_cluster():
                st.session_state.cluster_started = True
                st.sidebar.success("Cluster started!")
                st.rerun()
    
    with col2:
        if st.button("🛑 Stop Cluster", type="secondary"):
            stop_hdfs_cluster()
            st.session_state.cluster_started = False
            st.sidebar.success("Cluster stopped!")
            st.rerun()
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📤 Upload File", "📥 Download File", "📋 File List"])
    
    with tab1:
        st.header("Cluster Status")
        
        status = get_cluster_status()
        
        # NameNode status
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            if status['namenode']:
                st.success("✅ NameNode")
            else:
                st.error("❌ NameNode")
        
        # SlaveNode status
        for i, (col, slave_status) in enumerate(zip([col2, col3, col4, col5], status['slavenodes'])):
            with col:
                if slave_status:
                    st.success(f"✅ SlaveNode {i+1}")
                else:
                    st.error(f"❌ SlaveNode {i+1}")
        
        # Cluster overview
        total_nodes = 1 + len(status['slavenodes'])
        active_nodes = int(status['namenode']) + sum(status['slavenodes'])
        
        st.metric("Active Nodes", f"{active_nodes}/{total_nodes}")
        
        if active_nodes == total_nodes:
            st.success("🎉 All nodes are running!")
        elif active_nodes > 0:
            st.warning("⚠️ Some nodes are not responding")
        else:
            st.error("🚨 Cluster is not running. Please start the cluster.")
    
    with tab2:
        st.header("Upload File to HDFS")
        
        if not status['namenode']:
            st.error("❌ NameNode is not running. Please start the cluster first.")
        else:
            uploaded_file = st.file_uploader(
                "Choose a file to upload to HDFS",
                help="Select any file to upload to the distributed file system"
            )
            
            if uploaded_file is not None:
                # Display file info
                st.write("**File Information:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"📄 **Name:** {uploaded_file.name}")
                    st.write(f"📏 **Size:** {uploaded_file.size:,} bytes")
                with col2:
                    st.write(f"📁 **Type:** {uploaded_file.type}")
                
                if st.button("🚀 Upload to HDFS", type="primary"):
                    # Save uploaded file temporarily
                    temp_dir = tempfile.mkdtemp()
                    temp_file_path = os.path.join(temp_dir, uploaded_file.name)
                    
                    try:
                        with open(temp_file_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                        
                        with st.spinner("Uploading file to HDFS..."):
                            # Use the add_file function from client.py
                            add_file(temp_file_path)
                        
                        st.success(f"✅ File '{uploaded_file.name}' uploaded successfully to HDFS!")
                        
                    except Exception as e:
                        st.error(f"❌ Error uploading file: {str(e)}")
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        os.rmdir(temp_dir)
    
    with tab3:
        st.header("Download File from HDFS")
        
        if not status['namenode']:
            st.error("❌ NameNode is not running. Please start the cluster first.")
        else:
            files = get_file_list()
            
            if not files:
                st.info("📭 No files found in HDFS. Upload some files first!")
            else:
                selected_file = st.selectbox(
                    "Select a file to download:",
                    files,
                    help="Choose a file from the HDFS to download"
                )
                
                if selected_file and st.button("📥 Download File", type="primary"):
                    try:
                        output_path = f"retrieved_{selected_file}"
                        
                        with st.spinner("Retrieving file from HDFS..."):
                            read_file(selected_file, output_path)
                        
                        # Read the file and offer it for download
                        if os.path.exists(output_path):
                            with open(output_path, "rb") as f:
                                file_data = f.read()
                            
                            st.success(f"✅ File '{selected_file}' retrieved successfully!")
                            
                            st.download_button(
                                label="💾 Download Retrieved File",
                                data=file_data,
                                file_name=selected_file,
                                mime="application/octet-stream"
                            )
                            
                            # Clean up
                            os.remove(output_path)
                        else:
                            st.error("❌ Failed to retrieve file from HDFS.")
                            
                    except Exception as e:
                        st.error(f"❌ Error retrieving file: {str(e)}")
    
    with tab4:
        st.header("Files in HDFS")
        
        if not status['namenode']:
            st.error("❌ NameNode is not running. Please start the cluster first.")
        else:
            if st.button("🔄 Refresh File List"):
                st.rerun()
            
            files = get_file_list()
            
            if not files:
                st.info("📭 No files found in HDFS.")
            else:
                st.write(f"**Total Files:** {len(files)}")
                
                for i, filename in enumerate(files, 1):
                    st.write(f"{i}. 📄 {filename}")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**HDFS Simulation**")
    st.sidebar.markdown("Built with Streamlit 🎈")

if __name__ == "__main__":
    main()
