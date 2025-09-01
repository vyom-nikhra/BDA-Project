from flask import Flask, request, send_file, jsonify
import os
import sys

# --- Flask App Initialization ---
app = Flask(__name__)

# This will be set from command-line arguments.
SLAVE_STORAGE_DIR = None

# --- API Endpoints ---

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "slavenode"}), 200

@app.route('/write_block/<block_id>', methods=['POST'])
def write_block(block_id):
    """
    Receives a block of data from a client and saves it to local storage.
    """
    # Ensure the storage directory exists.
    os.makedirs(SLAVE_STORAGE_DIR, exist_ok=True)
    
    block_path = os.path.join(SLAVE_STORAGE_DIR, block_id)
    
    try:
        # The raw request data is the file content.
        with open(block_path, 'wb') as f:
            f.write(request.data)
        app.logger.info(f"Successfully wrote block '{block_id}' to {block_path}")
        return jsonify({"message": f"Block '{block_id}' stored successfully."}), 201
    except Exception as e:
        app.logger.error(f"Error writing block '{block_id}': {e}")
        return jsonify({"error": "Failed to store block."}), 500


@app.route('/read_block/<block_id>', methods=['GET'])
def read_block(block_id):
    """
    Reads a block of data from local storage and returns it to the client.
    """
    block_path = os.path.join(SLAVE_STORAGE_DIR, block_id)
    
    if not os.path.exists(block_path):
        return jsonify({"error": "Block not found."}), 404
        
    app.logger.info(f"Serving block '{block_id}' from {block_path}")
    return send_file(block_path)

# --- Main Execution ---
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python slavenode.py <port> <storage_directory>")
        sys.exit(1)
        
    port = int(sys.argv[1])
    SLAVE_STORAGE_DIR = sys.argv[2]
    
    app.logger.info(f"Starting SlaveNode on port {port}, using storage at '{SLAVE_STORAGE_DIR}'")
    app.run(host='127.0.0.1', port=port, debug=False)