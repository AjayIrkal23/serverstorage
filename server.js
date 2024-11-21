import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import express from "express";
import fs from "fs";
import path from "path";
import cors from "cors"; // For HTTP server CORS support

// Configuration
const PROTO_PATH = "./fileupload.proto";
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const GRPC_PORT = "0.0.0.0:8001";
const HTTP_PORT = 8000;

// Ensure uploads directory exists
if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

// Load the protobuf file
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const fileUploadProto =
  grpc.loadPackageDefinition(packageDefinition).fileupload;

// gRPC Service Implementation
const uploadFile = (call, callback) => {
  let fileName = "";
  let writeStream;

  call.on("data", (data) => {
    if (data.fileName && !fileName) {
      fileName = data.fileName;
      const filePath = path.join(UPLOAD_DIR, fileName);
      writeStream = fs.createWriteStream(filePath);
    }
    if (data.content && writeStream) {
      writeStream.write(data.content);
    }
  });

  call.on("end", () => {
    if (writeStream) {
      writeStream.end(() => {
        console.log(`File saved: ${path.join(UPLOAD_DIR, fileName)}`);
        callback(null, { message: "File uploaded successfully!" });
      });
    }
  });

  call.on("error", (err) => {
    console.error("Error during file upload:", err);
    if (writeStream) writeStream.destroy();
    callback(err);
  });
};

const getFileURL = (call, callback) => {
  const { fileName } = call.request;
  const filePath = path.join(UPLOAD_DIR, fileName);

  fs.access(filePath, fs.constants.F_OK, (err) => {
    if (err) {
      console.error(`File not found: ${filePath}`);
      return callback({
        code: grpc.status.NOT_FOUND,
        message: "File not found"
      });
    }
    const fileUrl = `http://localhost:${HTTP_PORT}/uploads/${fileName}`;
    callback(null, { fileUrl });
  });
};

// Start gRPC Server with 20GB Support
const grpcServer = new grpc.Server({
  "grpc.max_receive_message_length": 20 * 1024 * 1024 * 1024, // 20 GB
  "grpc.max_send_message_length": 20 * 1024 * 1024 * 1024 // 20 GB
});

grpcServer.addService(fileUploadProto.FileUploadService.service, {
  uploadFile,
  getFileURL
});

grpcServer.bindAsync(
  GRPC_PORT,
  grpc.ServerCredentials.createInsecure(),
  (err, port) => {
    if (err) {
      console.error("Failed to start gRPC server:", err);
      return;
    }
    console.log(`gRPC server is running on ${GRPC_PORT}`);
    grpcServer.start();
  }
);

// Start HTTP Server
const app = express();
app.use(cors()); // Enable CORS for HTTP requests
app.use(express.static(UPLOAD_DIR)); // Serve static files
app.use(express.json({ limit: "20gb" })); // For parsing JSON request bodies, with a 20GB limit

// Serve uploaded files
app.use("/uploads", express.static(UPLOAD_DIR));

// Health Check Endpoint
app.get("/health", (req, res) => {
  res.status(200).send("Server is healthy!");
});

// Error Handling Middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send({ error: "Something went wrong!" });
});

app.listen(HTTP_PORT, "0.0.0.0", () => {
  console.log(`HTTP server is running on http://0.0.0.0:${HTTP_PORT}`);
});
