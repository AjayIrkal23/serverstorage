import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import express from "express";
import fs from "fs";
import path from "path";
import cors from "cors"; // For HTTP server CORS support

// Configuration
const PROTO_PATH = "./fileupload.proto";
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const TEMP_DIR = path.join(UPLOAD_DIR, "temp");
const GRPC_PORT = "0.0.0.0:8001";
const HTTP_PORT = 8000;

// Ensure directories exist
if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}
if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
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
  let chunkIndex = -1;
  let writeStream;

  call.on("data", (data) => {
    if (data.fileName && data.chunkIndex !== undefined) {
      fileName = data.fileName;
      chunkIndex = data.chunkIndex;

      const tempFilePath = path.join(
        TEMP_DIR,
        `${fileName}-chunk-${chunkIndex}`
      );
      writeStream = fs.createWriteStream(tempFilePath);

      if (data.content && writeStream) {
        writeStream.write(data.content);
      }
    }
  });

  call.on("end", () => {
    if (writeStream) {
      writeStream.end(() => {
        console.log(`Chunk received: ${fileName} - chunk ${chunkIndex}`);
        callback(null, { message: "Chunk uploaded successfully!" });
      });
    }
  });

  call.on("error", (err) => {
    console.error("Error during chunk upload:", err);
    if (writeStream) writeStream.destroy();
    callback(err);
  });
};

const mergeChunks = (call, callback) => {
  const { fileName } = call.request;

  const tempDir = TEMP_DIR;
  const finalFilePath = path.join(UPLOAD_DIR, fileName);

  try {
    const chunkFiles = fs
      .readdirSync(tempDir)
      .filter((file) => file.startsWith(fileName))
      .sort((a, b) => {
        const indexA = parseInt(a.split("-chunk-")[1], 10);
        const indexB = parseInt(b.split("-chunk-")[1], 10);
        return indexA - indexB;
      });

    const writeStream = fs.createWriteStream(finalFilePath);
    chunkFiles.forEach((chunkFile) => {
      const chunkPath = path.join(tempDir, chunkFile);
      const data = fs.readFileSync(chunkPath);
      writeStream.write(data);
      fs.unlinkSync(chunkPath); // Remove chunk after writing
    });

    writeStream.end(() => {
      console.log(`File assembled: ${finalFilePath}`);
      callback(null, { message: "File assembled successfully!" });
    });
  } catch (err) {
    console.error("Error during file assembly:", err);
    callback({
      code: grpc.status.INTERNAL,
      message: "Error during file assembly"
    });
  }
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
  mergeChunks,
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
