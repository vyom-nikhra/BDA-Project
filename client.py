import requests
import os
import argparse
import math

# --- Configuration ---
NAMENODE_URL = "http://127.0.0.1:5000"
BLOCK_SIZE = 32 * 1024 * 1024  # 32 MB

# --- Helper Functions ---

def create_test_file(filepath, size_mb):
    """Creates a dummy binary file of a specified size in MB."""
    size_bytes = size_mb * 1024 * 1024
    print(f"Creating a dummy file '{filepath}' of {size_mb} MB...")
    with open(filepath, 'wb') as f:
        f.write(os.urandom(size_bytes))
    print("Dummy file created successfully.")


def split_file(filepath):
    """Splits a file into chunks of BLOCK_SIZE."""
    file_size = os.path.getsize(filepath)
    num_blocks = math.ceil(file_size / BLOCK_SIZE)
    blocks = []
    with open(filepath, 'rb') as f:
        for i in range(num_blocks):
            blocks.append(f.read(BLOCK_SIZE))
    return blocks

# --- Client Operations ---

def add_file(filepath):
    """
    Implements the 'add' operation to store a file in HDFS.
    """
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        return

    filename = os.path.basename(filepath)
    print(f"Splitting file '{filename}' into blocks...")
    blocks_data = split_file(filepath)
    num_blocks = len(blocks_data)
    print(f"File split into {num_blocks} blocks.")

    # 1. Contact NameNode for a write plan.
    print("Requesting write plan from NameNode...")
    try:
        response = requests.post(
            f"{NAMENODE_URL}/get_write_plan",
            json={"filename": filename, "num_blocks": num_blocks}
        )
        response.raise_for_status()
        plan = response.json().get('plan')
    except requests.exceptions.RequestException as e:
        print(f"Error contacting NameNode: {e}")
        return

    if not plan:
        print(f"Error: {response.json().get('error', 'Could not get a valid write plan.')}")
        return
        
    print("Write plan received. Writing blocks to SlaveNodes...")
    # 2. Write blocks directly to SlaveNodes.
    written_blocks_info = []
    for i, block_info in enumerate(plan):
        block_id = block_info['block_id']
        slave_url = block_info['slave_node']
        block_data = blocks_data[i]
        
        try:
            print(f"  Writing block {block_id} to {slave_url}...")
            write_response = requests.post(
                f"{slave_url}/write_block/{block_id}",
                data=block_data,
                headers={'Content-Type': 'application/octet-stream'}
            )
            write_response.raise_for_status()
            written_blocks_info.append(block_info)
        except requests.exceptions.RequestException as e:
            print(f"Error writing block {block_id} to {slave_url}: {e}")
            print("Aborting file add operation.")
            # In a real system, you would need a rollback mechanism.
            return

    # 3. Tell NameNode to commit the write operation.
    print("All blocks written. Committing write to NameNode...")
    try:
        commit_response = requests.post(
            f"{NAMENODE_URL}/commit_write",
            json={"filename": filename, "blocks": written_blocks_info}
        )
        commit_response.raise_for_status()
        print(commit_response.json().get('message'))
    except requests.exceptions.RequestException as e:
        print(f"Error committing write to NameNode: {e}")

def read_file(filename, output_path):
    """
    Implements the 'read' operation to retrieve a file from HDFS.
    """
    print(f"Requesting read plan for '{filename}' from NameNode...")
    
    # 1. Get the read plan from the NameNode.
    try:
        response = requests.get(
            f"{NAMENODE_URL}/get_read_plan",
            params={"filename": filename}
        )
        response.raise_for_status()
        plan = response.json().get('plan')
    except requests.exceptions.RequestException as e:
        print(f"Error contacting NameNode: {e}")
        return

    if not plan:
        print(f"Error: {response.json().get('error', 'Could not get a valid read plan.')}")
        return
        
    print("Read plan received. Reading blocks from SlaveNodes...")
    # 2. Read blocks directly from SlaveNodes and reassemble.
    try:
        with open(output_path, 'wb') as f_out:
            for block_info in plan:
                block_id = block_info['block_id']
                slave_url = block_info['slave_node']
                
                print(f"  Reading block {block_id} from {slave_url}...")
                read_response = requests.get(f"{slave_url}/read_block/{block_id}")
                read_response.raise_for_status()
                f_out.write(read_response.content)
        print(f"File '{filename}' successfully reassembled at '{output_path}'")
    except requests.exceptions.RequestException as e:
        print(f"Error reading block: {e}")
    except IOError as e:
        print(f"Error writing to output file '{output_path}': {e}")

# --- Main CLI Parser ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for a simulated HDFS.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 'add' command
    parser_add = subparsers.add_parser('add', help='Add a file to HDFS.')
    parser_add.add_argument('filepath', type=str, help='The local path to the file to add.')

    # 'read' command
    parser_read = subparsers.add_parser('read', help='Read a file from HDFS.')
    parser_read.add_argument('filename', type=str, help='The name of the file in HDFS.')
    parser_read.add_argument('output_path', type=str, help='The local path to save the retrieved file.')

    # 'create_testfile' command
    parser_create = subparsers.add_parser('create_testfile', help='Create a dummy file for testing.')
    parser_create.add_argument('filepath', type=str, help='The path where the test file will be created.')
    parser_create.add_argument('--size', type=int, default=100, help='Size of the file in MB (default: 100).')

    args = parser.parse_args()

    if args.command == 'add':
        add_file(args.filepath)
    elif args.command == 'read':
        read_file(args.filename, args.output_path)
    elif args.command == 'create_testfile':
        create_test_file(args.filepath, args.size)