# FROM public.ecr.aws/lambda/python:3.13

# COPY hair_classifier_empty.onnx.data .
# COPY hair_classifier_empty.onnx .

# Use the prebuilt lambda image as base
FROM agrigorev/model-2025-hairstyle:v1

# Install required Python packages
RUN pip install --no-cache-dir pillow numpy onnxruntime

# Copy your lambda/homework code
COPY homework.py /var/task/homework.py

# Set working directory
WORKDIR /var/task

# Optional: set default command to run homework.py
CMD ["python3", "homework.py"]