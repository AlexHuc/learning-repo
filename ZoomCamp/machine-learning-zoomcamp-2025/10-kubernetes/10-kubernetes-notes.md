# ML Zoomcamp 10.1 - Overview

## Overview
Session 10 covers how to deploy an image classification system using **TensorFlow Serving** and **Kubernetes**, building on the model trained in previous sessions. The goal is to classify clothing images (e.g., pants) for an online classifieds website.

![](./imgs/ml-10-1/1.png)

## Architecture Components
### 1. **TensorFlow Serving**
- A high-performance C++ system designed exclusively for **inference**.
- Receives a preprocessed input matrix **X** (numpy array representing the image).
- Returns an array of **10 prediction scores** for the clothing categories.
- Communicates using **gRPC**, a fast binary protocol.

### 2. **Gateway Service (Flask)**
Needed because:
- Users cannot perform preprocessing themselves.
- Websites prefer sending JSON, not using gRPC.
- TensorFlow Serving expects a formatted input (e.g., resized image, numpy array).

The gateway:
- Accepts an **image URL**.
- Downloads and preprocesses the image (resize, convert to numpy, prepare input).
- Sends it to TensorFlow Serving via gRPC.
- Postprocesses the 10 prediction scores into a **human-readable JSON response**.

### 3. **User Workflow**
1. User uploads a picture on the website.
2. Website sends the image URL to the gateway.
3. Gateway preprocesses and forwards it to TensorFlow Serving.
4. TensorFlow Serving returns prediction scores.
5. Gateway converts them into readable predictions.
6. Website uses predictions to suggest a clothing category.

## Why Two Services?
- **Different resource requirements:**  
  - TensorFlow Serving → GPU for fast matrix multiplications  
  - Gateway → CPU is enough  
- **Independent scalability:**  
  - Example: 2 GPU TensorFlow Serving instances + 5 CPU gateway instances  
- **Cleaner architecture:** preprocessing and postprocessing stay outside core inference.

## Reuse of Previous Code
- The gateway’s logic (downloading, preprocessing, postprocessing) heavily reuses code from the earlier serverless (AWS Lambda) session.

## Deployment Roadmap (Lessons Summary)

### **Lesson 1**
- Convert the Keras model into **SavedModel** format (required by TensorFlow Serving).
- Run TensorFlow Serving **locally in Docker** and test it.

### **Lesson 2**
- Build the gateway preprocessing service using **Flask**.

### **Lesson 3**
- Use **Docker Compose** to run both services (gateway + TensorFlow Serving) locally and connect them.

### **Lesson 5**
- Introduce core **Kubernetes concepts**.

### **Lesson 6**
- Deploy a simple test application to a local Kubernetes cluster using **kind**.

### **Lesson 7**
- Deploy both the gateway and TensorFlow Serving to Kubernetes.

### **Lesson 8**
- Move the entire system to the cloud using **AWS EKS** (but applicable to any cloud provider).

## Final Notes
- The session focuses on building a scalable, modular inference pipeline using industry-standard tools.
- The next step is converting the trained Keras model into the SavedModel format expected by TensorFlow Serving.

---

# ML Zoomcamp 10.2 - TensorFlow Serving

This section explains how to prepare and deploy a Keras model using **TensorFlow Serving**.

### Converting a Keras Model to SavedModel
- TensorFlow Serving requires models in the **SavedModel** format rather than traditional HDF5 files.
- A pre-trained image classification model (Xception) is downloaded and converted to this format.
- After conversion, the SavedModel directory contains the model architecture, weights, and metadata.

### Inspecting the SavedModel
- TensorFlow provides the `saved_model_cli` tool to inspect exported models.
- The signature definition is especially important:  
  It shows expected **input tensor shape** `(batch, 299, 299, 3)` and **output shape** `(batch, 10)`.
- This information is needed to correctly format data before sending requests to TensorFlow Serving.

### Running the Model with TensorFlow Serving (Docker)
- The SavedModel is loaded into a Docker container using the official `tensorflow/serving` image.
- Important Docker configuration:
  - **Port mapping** exposes gRPC on port `8500`.
  - **Volume mapping** links the local SavedModel directory to the container's model path.
  - **Environment variable** `MODEL_NAME` specifies which model to serve.

### gRPC and Protobuf
- TensorFlow Serving uses **gRPC**, a high-performance binary communication protocol.
- Inputs for inference must be encoded as **Protocol Buffers (protobuf)** instead of JSON.
- This allows efficient, production-grade model serving with low latency.

### Overall Purpose
This lesson demonstrates:
- How to export a model to SavedModel format  
- How to inspect it  
- How to serve it using TensorFlow Serving + Docker  
- Why protobuf/gRPC are required for inference requests  

It sets the foundation for building a complete ML inference pipeline using Kubernetes and TensorFlow Serving.

---

# 10.3