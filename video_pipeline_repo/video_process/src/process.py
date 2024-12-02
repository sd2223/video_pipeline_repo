import os
import io
import json
import sys 
import threading
import requests 
from collections import deque
from flask import Flask, request, jsonify
# from google.oauth2.service_account import Credentials
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload
# from video_pipeline_repo.enhancement.src.enhance import EnhancementService
# from facial_rec.src.facial_rec import FacialRecognitionService
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)

from enhancement.src.enhance import EnhancementService
from facial_rec.src.facial_rec import FacialRecognitionService
import logging 
import cv2
import numpy as np
import concurrent.futures
import time
from queue import Queue
from dataclasses import dataclass
from typing import List, Dict, Any

app = Flask(__name__)

# Configure logging to output to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = 'keys/video-frame-input-credentials.json'  # Placeholder for credentials file
FRAMES_PATH = '../../video_decoder/src/decoded_storage/'
OUTPUT_PATH = 'processed_frames/'

class FrameBuffer:
    def __init__(self, max_size=100):
        self.buffer = deque(maxlen=max_size)
        self.frame_metadata = None

    def add_frame(self, frame):
        self.buffer.append(frame)

    def get_frames(self, count=1):
        return [self.buffer.popleft() for _ in range(min(count, len(self.buffer)))]

    def set_metadata(self, metadata):
        self.frame_metadata = metadata

@dataclass
class VideoJob:
    job_id: str
    metadata: Dict[str, Any]
    quality_levels: List[str]
    priority: str
    pipeline_config: List[str]

class VideoJobQueue:
    def __init__(self):
        self.job_queue = Queue()
        self.active_jobs = {}
        self.job_lock = threading.Lock()
    
    def add_job(self, job: VideoJob):
        self.job_queue.put(job)
    
    def get_next_job(self):
        return self.job_queue.get() if not self.job_queue.empty() else None
    
    def mark_job_active(self, job_id: str):
        with self.job_lock:
            self.active_jobs[job_id] = True
    
    def mark_job_complete(self, job_id: str):
        with self.job_lock:
            self.active_jobs.pop(job_id, None)

class ProcessorService:
    def __init__(self, local_storage_path, output_storage_path, max_concurrent_jobs=3):
        # self.drive_service = None
        self.local_storage_path = local_storage_path 
        self.output_storage_path = output_storage_path
        self.frame_buffers = {}  # Dictionary to store buffers for different quality levels
        self.distribution_manager = DistributionManager()
        self.job_queue = VideoJobQueue()
        self.max_concurrent_jobs = max_concurrent_jobs
        self.job_threads = []

        # Start job processor thread
        threading.Thread(target=self._process_job_queue, daemon=True).start()

    # def authenticate_google_drive(self):
    #     credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    #     self.drive_service = build('drive', 'v3', credentials=credentials)

    def _process_job_queue(self):
        while True:
            if len(self.job_threads) < self.max_concurrent_jobs:
                job = self.job_queue.get_next_job()
                if job:
                    self.job_queue.mark_job_active(job.job_id)
                    thread = threading.Thread(
                        target=self._process_single_video,
                        args=(job,)
                    )
                    self.job_threads.append(thread)
                    thread.start()
            
            # Clean up completed threads
            self.job_threads = [t for t in self.job_threads if t.is_alive()]
            time.sleep(1)  # Prevent busy waiting

    def _process_single_video(self, job: VideoJob):
        try:
            self.process_video(
                job.job_id,
                job.metadata,
                job.quality_levels,
                job.priority,
                job.pipeline_config
            )
        finally:
            self.job_queue.mark_job_complete(job.job_id)

    def fetch_decoded_frames(self, job_id, quality_levels, priority, pipeline_config):
        # try:
        #     folder_name = f"decoded_frames_{job_id}"
        #     query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
        #     results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        #     items = results.get('files', [])

        #     if not items:
        #         raise ValueError(f"Folder {folder_name} not found")

        #     folder_id = items[0]['id']

        #     # Check for all quality folders
        #     for quality in quality_levels:
        #         quality_folder_name = f"quality_{quality}"
        #         query = f"'{folder_id}' in parents and name = '{quality_folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
        #         results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        #         quality_items = results.get('files', [])

        #         if not quality_items:
        #             raise ValueError(f"Quality folder {quality_folder_name} not found for job {job_id}")

        #         quality_folder_id = quality_items[0]['id']
        #         self.frame_buffers[quality] = FrameBuffer()
        #         self.fetch_quality_frames(quality_folder_id, quality)

        #     logger.info(f"Successfully fetched decoded frames for job: {job_id}")
        #     return True
        # except Exception as e:
        #     logger.error(f"Error fetching decoded frames from Google Drive: {e}")
        #     return False
        try:
            job_folder = os.path.join(self.local_storage_path, f"decoded_frames_{job_id}")
            if not os.path.exists(job_folder):
                raise ValueError(f"Folder for job {job_id} not found")

            for quality in quality_levels:
                quality_folder = os.path.join(job_folder, f"quality_{quality}")
                if not os.path.exists(quality_folder):
                    raise ValueError(f"Quality folder {quality} not found for job {job_id}")

                self.frame_buffers[quality] = FrameBuffer()
                self.load_frames(quality_folder, quality)

            logger.info(f"Successfully fetched decoded frames for job: {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error fetching decoded frames: {e}")
            return False
        
    def load_frames(self, folder_path, quality):
        frame_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.raw')])
        for frame_file in frame_files:
            frame_path = os.path.join(folder_path, frame_file)
            with open(frame_path, 'rb') as f:
                frame_data = f.read()
            # Calculate the correct dimensions
            # total_pixels = len(frame_data) // 3  # Assuming 3 channels (RGB)
            width = int(quality.split('x')[0])
            height = int(quality.split('x')[1])
            expected_size = width * height * 3  # 3 channels for RGB
            if len(frame_data) != expected_size:
                logger.warning(f"Frame data size mismatch for {frame_file}. Expected {expected_size}, got {len(frame_data)}")
            frame = np.frombuffer(frame_data, dtype=np.uint8).reshape((height, width, 3))
            # Convert BGR to RGB
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

            # Log frame information for debugging
            # logger.info(f"Frame shape before adding to buffer: {frame.shape}, dtype: {frame.dtype}")
        
            self.frame_buffers[quality].add_frame(frame_rgb)

    def notify_encoder(self, job_id, metadata):
        encoder_service_url = "http://localhost:5002/encode"
        payload = {
            "job_id": job_id,
            "metadata": metadata
        }
        try:
            response = requests.post(encoder_service_url, json=payload)
            if response.status_code == 200:
                logger.info(f"Successfully notified encoder service for job {job_id}")
            else:
                logger.error(f"Failed to notify encoder service for job {job_id}. Status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Error notifying encoder service for job {job_id}: {str(e)}")

    def process_video(self, job_id, video_metadata, quality_levels, priority, pipeline_config):
        try:
            if self.fetch_decoded_frames(job_id, quality_levels, priority, pipeline_config):
                threads = []
                for quality, buffer in self.frame_buffers.items():
                    thread = threading.Thread(
                        target=self.process_frames,
                        args=(buffer, priority, pipeline_config, job_id, quality, video_metadata)
                    )
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()

                if 'enhance' in pipeline_config:
                    logger.info(f"Completed enhancement processing for all frames in job {job_id}")
                    self.notify_encoder(job_id, video_metadata)
                if 'recognize_faces' in pipeline_config:
                    logger.info(f"Completed facial recognition for all frames in job {job_id}")

                logger.info(f"Processed all frames for job {job_id}")
            else:
                logger.error(f"Failed to fetch decoded frames for job {job_id}")
        except Exception as e:
            logger.error(f"Unexpected error during video processing for job {job_id}: {str(e)}")

    def process_frames(self, buffer, priority, pipeline_config, job_id, quality, video_metadata):
        frame_number = 0
        while len(buffer.buffer) > 0:
            frame = buffer.get_frames(1)[0]
            frame_metadata = self.create_frame_metadata(frame, frame_number, job_id, quality, video_metadata)
            self.distribution_manager.distribute_frame(frame, frame_metadata, pipeline_config)

            # for step in pipeline_config:
            #     if step == 'enhance':
            #         frame = self.enhance_frame(frame)
            #     elif step == 'detect_motion':
            #         self.detect_motion(frame)
            #     elif step == 'recognize_faces':
            #         self.recognize_faces(frame)
            
            # Save processed frame with metadata
            self.save_processed_frame(frame, frame_metadata, job_id, quality)
            frame_number += 1

    def create_frame_metadata(self, frame, frame_number, job_id, quality, video_metadata):
        height, width, _ = frame.shape
        avg_color = np.mean(frame, axis=(0, 1)).tolist()
        brightness = np.mean(cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY))
        fps = video_metadata.get('fps', 30)                    # Get fps from video metadata
        timestamp = frame_number / fps if fps > 0 else 0            

        metadata = {
            "job_id": job_id,
            "frame_number": frame_number,
            "quality": quality,
            "timestamp": timestamp,
            "width": width,
            "height": height,
            "avg_color": avg_color,
            "brightness": brightness
        }
        return metadata
    
    def save_processed_frame(self, frame, metadata, job_id, quality):
        output_folder = os.path.join(self.output_storage_path, f"processed_frames_{job_id}", f"quality_{quality}")
        os.makedirs(output_folder, exist_ok=True)
        
        frame_filename = f"frame_{metadata['frame_number']:06d}.png"
        frame_path = os.path.join(output_folder, frame_filename)
        cv2.imwrite(frame_path, frame)
        
        metadata_filename = f"frame_{metadata['frame_number']:06d}_metadata.json"
        metadata_path = os.path.join(output_folder, metadata_filename)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)

class DistributionManager:
    def __init__(self, num_enhancement_workers=5, num_recognition_workers=2):
        # Create multiple service instances
        self.enhancement_services = [EnhancementService() for _ in range(num_enhancement_workers)]
        self.facial_recognition_services = [FacialRecognitionService() for _ in range(num_recognition_workers)]
        
        # Track service availability
        self.enhancement_available = {i: True for i in range(num_enhancement_workers)}
        self.recognition_available = {i: True for i in range(num_recognition_workers)}
        
        # Locks for thread safety
        self.enhancement_lock = threading.Lock()
        self.recognition_lock = threading.Lock()
        
        self.start_services()

    def start_services(self):
        # Start all service instances
        for service in self.enhancement_services + self.facial_recognition_services:
            threading.Thread(target=service.start, daemon=True).start() 

    def get_available_service(self, services_dict, services_list, lock):
        with lock:
            for idx, available in services_dict.items():
                if available:
                    services_dict[idx] = False
                    return idx, services_list[idx]
        return None, None
    
    def release_service(self, service_dict, service_idx, lock):
        with lock:
            service_dict[service_idx] = True
    
    def distribute_frame(self, frame, metadata, pipeline_config):
        results = {}
        futures = []
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            if 'enhance' in pipeline_config:
                while True:
                    idx, service = self.get_available_service(
                        self.enhancement_available,
                        self.enhancement_services,
                        self.enhancement_lock
                    )
                    if service:
                            logger.info(f"Processing frame {metadata['frame_number']} through enhancement service {idx}")
                            future = executor.submit(self._process_enhancement, frame, metadata, service, idx)
                            futures.append(('enhance', future, idx))
                            break
                    else:
                        logger.info(f"Waiting for available enhancement service for frame {metadata['frame_number']}")
                        time.sleep(0.1)  # Wait for 100ms before trying again
            
            if 'recognize_faces' in pipeline_config:
                while True:
                    idx, service = self.get_available_service(
                        self.recognition_available,
                        self.facial_recognition_services,
                        self.recognition_lock
                    )
                    if service:
                        logger.info(f"Processing frame {metadata['frame_number']} through facial recognition service {idx}")
                        future = executor.submit(self._process_recognition, frame, metadata, service, idx)
                        futures.append(('recognize', future, idx))
                        break 
                    else:
                        logger.info(f"Waiting for available recognition service for frame {metadata['frame_number']}")
                        time.sleep(0.1)  # Wait for 100ms before trying again
            
            # Collect results
            for task_type, future, idx in futures:
                try:
                    result = future.result(timeout=30)  # 30-second timeout
                    results[task_type] = result
                    
                    # Release the service
                    if task_type == 'enhance':
                        self.release_service(self.enhancement_available, idx, self.enhancement_lock)
                    else:
                        self.release_service(self.recognition_available, idx, self.recognition_lock)
                except concurrent.futures.TimeoutError:
                    logger.error(f"{task_type} processing timed out")
                except Exception as e:
                    logger.error(f"Error processing {task_type}: {str(e)}")
                
        return results
    # def distribute_frame(self, frame, metadata, pipeline_config):
    #     results = {}
    #     for step in pipeline_config:
    #         if step in self.services:
    #             service = self.services[step]
    #             service.input_queue.put((frame.copy(), metadata))
    #             processed_frame, result = service.output_queue.get()
    #             results[step] = result
    #     return results
    # def distribute_frame(self, frame, metadata, pipeline_config):
        # threads = []
        # if 'enhance' in pipeline_config:
        #     # threads.append(threading.Thread(target=self.enhancement_service.process_frame, args=(frame, metadata)))
        #     pass
        # if 'recognize_faces' in pipeline_config:
        #     # threads.append(threading.Thread(target=self.facial_recognition_service.process_frame, args=(frame, metadata)))
        #     pass

        # for thread in threads:
        #     thread.start()
        
        # for thread in threads:
        #     thread.join()

    def _process_enhancement(self, frame, metadata, service, idx):
        service.input_queue.put((frame.copy(), metadata))
        processed_frame, result = service.output_queue.get()
        return result
    
    def _process_recognition(self, frame, metadata, service, idx):
        service.input_queue.put((frame.copy(), metadata))
        processed_frame, result = service.output_queue.get()
        return result
    
processor_service = ProcessorService(FRAMES_PATH, OUTPUT_PATH)

@app.route('/process', methods=['POST'])
def process_video():
    data = request.json
    # job_id = data.get('job_id')
    # metadata = data.get('metadata')
    # quality_levels = data.get('quality_levels', '1280x720')
    # priority = data.get('priority', 'normal')
    # pipeline_config = data.get('pipeline_config', ['enhance', 'detect_motion', 'recognize_faces'])

    # threading.Thread(target=processor_service.process_video, 
    #                  args=(job_id, metadata, quality_levels, priority, pipeline_config)).start()

    # return jsonify({"message": "Video processing started", "job_id": job_id}), 200
    job = VideoJob(
        job_id=data.get('job_id'),
        metadata=data.get('metadata'),
        quality_levels=data.get('quality_levels', ['1280x720']),
        priority=data.get('priority', 'normal'),
        pipeline_config=data.get('pipeline_config', ['enhance'])
    )
    processor_service.job_queue.add_job(job)
    return jsonify({
        "message": "Video job queued",
        "job_id": job.job_id,
        "position": processor_service.job_queue.job_queue.qsize()
    }), 200

if __name__ == "__main__":
    # processor_service.authenticate_google_drive()
    app.run(host='0.0.0.0', port=5001)