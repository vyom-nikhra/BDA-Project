import subprocess
import time
import os
import argparse
import sys
from client import add_file, read_file # Import functions from the client script

# --- Configuration ---
# List of server processes to start. Each is a list of command-line arguments.
SERVER_COMMANDS = [
    [sys.executable, "namenode.py"],
    [sys.executable, "slavenode.py", "5001", "slavenode_1"],
    [sys.executable, "slavenode.py", "5002", "slavenode_2"],
    [sys.executable, "slavenode.py", "5003", "slavenode_3"],
    [sys.executable, "slavenode.py", "5004", "slavenode_4"],
]

# --- Main Simulation Logic ---

def run_simulation(filepath):
    """
    Starts all servers, runs client operations, and then shuts them down.
    """
    if not os.path.exists(filepath):
        print(f"Error: The input file '{filepath}' does not exist.")
        return

    # Create log files for debugging server startup
    processes = []
    
    try:
        # 1. Start all servers as background processes
        print("🚀 Starting HDFS cluster (1 NameNode, 4 SlaveNodes)...")
        for i, command in enumerate(SERVER_COMMANDS):
            # Create log files for each server to help with debugging
            log_filename = f"server_{i}.log"
            with open(log_filename, 'w') as log_file:
                # subprocess.Popen runs a command in a new process without blocking.
                proc = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
                processes.append(proc)
            print(f"Started: {' '.join(command)}")
        
        print("✅ Cluster started successfully.")
        
        # 2. Wait for servers to initialize and check if they're running
        print("\n⏳ Allowing servers to initialize...")
        time.sleep(8) # Give more time for Flask servers to start
        
        # Check if any processes have died
        dead_processes = []
        for i, proc in enumerate(processes):
            if proc.poll() is not None:  # Process has terminated
                dead_processes.append((i, proc.returncode))
        
        if dead_processes:
            print("⚠️  Some servers failed to start:")
            for i, returncode in dead_processes:
                print(f"   Server {i} ({' '.join(SERVER_COMMANDS[i])}) exited with code {returncode}")
                # Print the log file content
                log_filename = f"server_{i}.log"
                if os.path.exists(log_filename):
                    print(f"   Log content for server {i}:")
                    with open(log_filename, 'r') as f:
                        print(f"   {f.read()}")
        else:
            print("✅ All servers appear to be running.")
        
        # 3. Run client operations
        print("\n--- Starting Client Operations ---")
        
        # Add the specified file to HDFS
        print("\n[CLIENT] Running 'add' operation...")
        add_file(filepath)
        
        # Read the file back from HDFS
        filename = os.path.basename(filepath)
        output_path = f"retrieved_{filename}"
        print(f"\n[CLIENT] Running 'read' operation to retrieve file as '{output_path}'...")
        read_file(filename, output_path)
        
        print("\n--- Client Operations Complete ---\n")

    except Exception as e:
        print(f"\nAn error occurred during the simulation: {e}")
    finally:
        # 4. Shut down all server processes
        print("🛑 Shutting down HDFS cluster...")
        for proc in processes:
            proc.terminate() # Gracefully terminate the process
            proc.wait() # Wait for the process to fully close
        print("✅ Cluster shut down successfully.")

# --- Main CLI Parser ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated runner for the HDFS simulation."
    )
    parser.add_argument(
        'filepath', 
        type=str, 
        help='The local path to the file you want to add to HDFS (e.g., your 100MB file).'
    )
    args = parser.parse_args()
    
    run_simulation(args.filepath)
