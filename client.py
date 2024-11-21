import grpc
import os
import time
from concurrent.futures import ThreadPoolExecutor
from fileupload_pb2 import UploadRequest, FileRequest
from fileupload_pb2_grpc import FileUploadServiceStub

# gRPC server address (replace with your server address)
SERVER_ADDRESS = "20.241.73.140:8001"

def upload_chunk(client, file_name, chunk_index, chunk_data):
    """Upload a single chunk to the server."""
    try:
        response = client.UploadFile(iter([
            UploadRequest(fileName=file_name, chunkIndex=chunk_index, content=chunk_data)
        ]))
        print(f"Chunk {chunk_index} uploaded: {response.message}")
    except grpc.RpcError as e:
        print(f"Failed to upload chunk {chunk_index}: {e.code()} - {e.details()}")

def upload_file(file_path, chunk_size=1024 * 1024 * 50, max_workers=4):
    """Upload a file to the gRPC server in parallel chunks."""
    # Establish a gRPC channel
    channel = grpc.insecure_channel(
        SERVER_ADDRESS,
        
    )
    client = FileUploadServiceStub(channel)

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    print(f"Uploading file: {file_name} (Size: {file_size} bytes)")

    # Split file into chunks and upload in parallel
    chunk_index = 0
    chunks = []

    with open(file_path, "rb") as file:
        while chunk := file.read(chunk_size):
            chunks.append((chunk_index, chunk))
            chunk_index += 1

    # Upload chunks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, chunk in chunks:
            executor.submit(upload_chunk, client, file_name, index, chunk)

    # Merge chunks on the server
    try:
        response = client.MergeChunks(FileRequest(fileName=file_name))
        print(f"File assembled: {response.message}")
    except grpc.RpcError as e:
        print(f"gRPC error during merge: {e.code()} - {e.details()}")

def get_file_url(file_name):
    """Get the public URL for a file from the gRPC server."""
    # Establish a gRPC channel
    channel = grpc.insecure_channel(
        SERVER_ADDRESS,
        
    )
    client = FileUploadServiceStub(channel)

    try:
        # Request the file URL from the server
        response = client.GetFileURL(FileRequest(fileName=file_name))
        print(f"File URL: {response.fileUrl}")
    except grpc.RpcError as e:
        print(f"gRPC error during URL fetch: {e.code()} - {e.details()}")

if __name__ == "__main__":
    # Example file to upload
    file_path = r"D:\Security_Center_v5.11.3.0_b3130.13_Full.zip"  # Replace with your file path
    file_name = os.path.basename(file_path)

    # Upload the file
    if os.path.exists(file_path):
        upload_file(file_path)
    else:
        print(f"File not found: {file_path}")

    # Fetch the file's public URL
    get_file_url(file_name)
