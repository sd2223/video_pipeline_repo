# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /video-pipeline-app

# Install system dependencies required for OpenCV
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libsm6 \
    libxext6 \
    ffmpeg  

# Copy the requirements file from the respective service directory (this will be passed as an argument)
ARG SERVICE_DIR=video_ingestion

# Copy the source code from the specified service directory into /app/src in the container
COPY ${SERVICE_DIR} /video-pipeline-app

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r /video-pipeline-app/requirements.txt

# Set environment variables (you can customize these as needed)
ENV PYTHONUNBUFFERED=1

# Run the application (this can be customized based on environment variables or arguments)
CMD ["python", "/video-pipeline-app/src/ingestion.py"]