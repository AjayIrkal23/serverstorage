import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import express from "express";
import fs from "fs/promises"; // Use asynchronous fs module
import path from "path";
import cors from "cors";
import promClient from "prom-client"; // Monitoring library

// Configuration
const PROTO_PATH = "./fileupload.proto";
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const TEMP_DIR = path.join(UPLOAD_DIR, "temp");
const GRPC_PORT = "0.0.0.0:8001";
const HTTP_PORT = 8000;

// Ensure directories exist
await fs.mkdir(UPLOAD_DIR, { recursive: true });
await fs.mkdir(TEMP_DIR, { recursive: true });

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

// Metrics for monitoring
const uploadDuration = new promClient.Histogram({
  name: "upload_duration_seconds",
  help: "Time taken to process file uploads",
  buckets: [0.1, 1, 5, 10, 30, 60, 300, 600] // Buckets in seconds
});

const totalChunksReceived = new promClient.Counter({
  name: "chunks_received_total",
  help: "Total number of chunks received"
});

const mergeDuration = new promClient.Histogram({
  name: "merge_duration_seconds",
  help: "Time taken to merge chunks",
  buckets: [1, 5, 10, 30, 60, 300]
});

// Error Recovery Metadata
const chunkTracker = new Map(); // Tracks received chunks for each file

// gRPC Service Implementation
const uploadFile = async (call, callback) => {
  let fileName = "";
  let chunkIndex = -1;

  call.on("data", async (data) => {
    const start = Date.now();

    if (data.fileName && data.chunkIndex !== undefined) {
      fileName = data.fileName;
      chunkIndex = data.chunkIndex;

      // Track received chunks
      if (!chunkTracker.has(fileName)) {
        chunkTracker.set(fileName, new Set());
      }
      chunkTracker.get(fileName).add(chunkIndex);

      const tempFilePath = path.join(
        TEMP_DIR,
        `${fileName}-chunk-${chunkIndex}`
      );

      try {
        // Directly write the data asynchronously
        await fs.writeFile(tempFilePath, data.content, { flag: "w" });
        totalChunksReceived.inc(); // Increment metric

        const duration = (Date.now() - start) / 1000; // Calculate upload duration
        uploadDuration.observe(duration); // Record metric
      } catch (err) {
        console.error(`Error writing chunk ${chunkIndex}:`, err);
      }
    }
  });

  call.on("end", () => {
    console.log(`All chunks received for ${fileName}`);
    callback(null, { message: "Chunks uploaded successfully!" });
  });

  call.on("error", (err) => {
    console.error("Error during chunk upload:", err);
    callback(err);
  });
};

// Parallel Merging of Chunks
const mergeChunks = async (call, callback) => {
  const { fileName } = call.request;
  const finalFilePath = path.join(UPLOAD_DIR, fileName);

  try {
    const start = Date.now();

    const chunkFiles = (await fs.readdir(TEMP_DIR))
      .filter((file) => file.startsWith(fileName))
      .sort((a, b) => {
        const indexA = parseInt(a.split("-chunk-")[1], 10);
        const indexB = parseInt(b.split("-chunk-")[1], 10);
        return indexA - indexB;
      });

    const writeStream = await fs.open(finalFilePath, "w");

    // Process chunks concurrently
    await Promise.all(
      chunkFiles.map(async (chunkFile) => {
        const chunkPath = path.join(TEMP_DIR, chunkFile);
        const data = await fs.readFile(chunkPath);
        await writeStream.write(data);
        await fs.unlink(chunkPath); // Remove chunk after writing
      })
    );

    await writeStream.close();

    const duration = (Date.now() - start) / 1000; // Calculate merge duration
    mergeDuration.observe(duration); // Record metric
    console.log(`File assembled: ${finalFilePath}`);
    callback(null, { message: "File assembled successfully!" });
  } catch (err) {
    console.error("Error during file assembly:", err);
    callback({
      code: grpc.status.INTERNAL,
      message: "Error during file assembly"
    });
  }
};

// Error Recovery: Check received chunks
const getReceivedChunks = (call, callback) => {
  const { fileName } = call.request;

  if (chunkTracker.has(fileName)) {
    const receivedChunks = Array.from(chunkTracker.get(fileName));
    callback(null, { receivedChunks });
  } else {
    callback({ code: grpc.status.NOT_FOUND, message: "No chunks found" });
  }
};

// Start gRPC Server with Optimized Settings
const grpcServer = new grpc.Server({
  "grpc.max_receive_message_length": 20 * 1024 * 1024 * 1024, // 20 GB
  "grpc.max_send_message_length": 20 * 1024 * 1024 * 1024, // 20 GB
  "grpc.max_concurrent_streams": 2048 // Increase concurrent stream limits
});

grpcServer.addService(fileUploadProto.FileUploadService.service, {
  uploadFile,
  mergeChunks,
  getReceivedChunks
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

// Expose Prometheus Metrics
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", promClient.register.contentType);
  res.send(await promClient.register.metrics());
});

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
