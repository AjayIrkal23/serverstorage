import grpc
import os
import zlib
from fileupload_pb2 import UploadRequest, FileRequest
from fileupload_pb2_grpc import FileUploadServiceStub

# gRPC server address
SERVER_ADDRESS = "20.241.73.140:8001"

def upload_file(file_path):
    """Upload a file to the gRPC server."""
    # Establish a gRPC channel with extended message limits
    channel = grpc.insecure_channel(
        SERVER_ADDRESS,
    )
    client = FileUploadServiceStub(channel)

    def file_chunks():
        """Generator to stream file chunks."""
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as file:
            chunk_index = 0
            while chunk := file.read(1024 * 1024 * 30):  # 100 MB chunk size
                compressed_chunk = zlib.compress(chunk)  # Compress the chunk
                yield UploadRequest(fileName=file_name, chunkIndex=chunk_index, content=compressed_chunk)
                chunk_index += 1

    try:
        # Send the file chunks to the server
        response = client.UploadFile(file_chunks())
        print(f"Upload response: {response.message}")
    except grpc.RpcError as e:
        print(f"gRPC error during upload: {e.details()}")

def get_received_chunks(file_name):
    """Fetch the list of received chunks from the server."""
    # Establish a gRPC channel
    channel = grpc.insecure_channel(
        SERVER_ADDRESS,
    )
    client = FileUploadServiceStub(channel)

    try:
        # Request the received chunks from the server
        response = client.GetReceivedChunks(FileRequest(fileName=file_name))
        print(f"Received chunks for {file_name}: {response.receivedChunks}")
    except grpc.RpcError as e:
        print(f"gRPC error during chunk tracking: {e.details()}")

if __name__ == "__main__":
    # Example file to upload
    file_path = "D:\Security_Center_v5.11.3.0_b3130.13_Full.zip"  # Replace with your file path
    file_name = os.path.basename(file_path)

    # Upload the file
    if os.path.exists(file_path):
        upload_file(file_path)
    else:
        print(f"File not found: {file_path}")

    # Fetch the file's public URL

    # Get received chunks for error recovery
    get_received_chunks(file_name)
