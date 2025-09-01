import flask
from flask import Flask, request, jsonify
import json
import os
import itertools

# --- Configuration ---
# The port the NameNode will listen on.
NAMENODE_PORT = 5000
# The file to persist metadata to.
METADATA_FILE = "namenode_data/metadata.json"
# The directory to store the metadata file.
METADATA_DIR = "namenode_data"
# List of SlaveNode addresses (host:port).
SLAVENODE_ADDRESSES = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
    "http://127.0.0.1:5003",
    "http://127.0.0.1:5004",
]

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Metadata Management ---
# In-memory storage for file-to-block mapping.
# Format: { "filename": [ {"block_id": "uuid", "slave_node": "http://..."}, ... ] }
metadata = {}
# A cycle iterator for round-robin SlaveNode selection.
slave_node_cycler = itertools.cycle(SLAVENODE_ADDRESSES)

def load_metadata():
    """Loads metadata from the JSON file into memory."""
    global metadata
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
        app.logger.info("Metadata loaded from disk.")
    else:
        app.logger.info("No existing metadata file found. Starting fresh.")
    # Ensure the directory exists for future saves.
    os.makedirs(METADATA_DIR, exist_ok=True)


def save_metadata():
    """Saves the current in-memory metadata to the JSON file."""
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)
    app.logger.info("Metadata saved to disk.")

# --- API Endpoints ---

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "namenode"}), 200

@app.route('/get_write_plan', methods=['POST'])
def get_write_plan():
    """
    Provides a client with a plan for writing a file's blocks.
    The client sends the filename and the number of blocks.
    The NameNode determines which SlaveNodes will store these blocks.
    """
    data = request.get_json()
    filename = data.get('filename')
    num_blocks = data.get('num_blocks')

    if not filename or not num_blocks:
        return jsonify({"error": "Filename and num_blocks are required."}), 400

    if filename in metadata:
        return jsonify({"error": "File already exists."}), 400

    app.logger.info(f"Received write request for '{filename}' with {num_blocks} blocks.")

    # Generate a plan using round-robin assignment.
    write_plan = []
    for i in range(num_blocks):
        block_id = f"{filename}_block_{i}"
        slave_node = next(slave_node_cycler)
        write_plan.append({"block_id": block_id, "slave_node": slave_node})

    app.logger.info(f"Generated write plan for '{filename}': {write_plan}")
    return jsonify({"plan": write_plan})


@app.route('/commit_write', methods=['POST'])
def commit_write():
    """
    Called by the client after it has successfully written all blocks to SlaveNodes.
    This finalizes the file creation by adding its metadata.
    """
    data = request.get_json()
    filename = data.get('filename')
    blocks_info = data.get('blocks') # List of {"block_id": ..., "slave_node": ...}

    if not filename or not blocks_info:
        return jsonify({"error": "Filename and blocks info are required."}), 400

    metadata[filename] = blocks_info
    save_metadata() # Persist the changes.
    app.logger.info(f"Successfully committed write for file '{filename}'.")
    return jsonify({"message": f"File '{filename}' committed successfully."}), 200


@app.route('/get_read_plan', methods=['GET'])
def get_read_plan():
    """
    Provides a client with the locations of all blocks for a given file.
    """
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"error": "Filename is required."}), 400

    if filename not in metadata:
        return jsonify({"error": "File not found."}), 404

    read_plan = metadata[filename]
    app.logger.info(f"Providing read plan for '{filename}': {read_plan}")
    return jsonify({"plan": read_plan})


@app.route('/list_files', methods=['GET'])
def list_files():
    """A simple endpoint to view all files tracked by the NameNode."""
    return jsonify(list(metadata.keys()))

# --- Main Execution ---
if __name__ == '__main__':
    load_metadata()
    app.run(host='127.0.0.1', port=NAMENODE_PORT, debug=False)