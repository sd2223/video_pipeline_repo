import os
import cv2
import numpy as np
from flask import Flask, request, jsonify
import threading
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# This should be set to the appropriate path
ENHANCED_FRAMES_DIR = '../../enhancement/src/enhanced_frames/'
OUTPUT_DIR = 'encoder_output/'

class EncoderService:
    def __init__(self):
        self.encoding_queue = []
        self.encoding_lock = threading.Lock()

    def start_encoding(self, job_id, metadata):
        threading.Thread(target=self._encode_video, args=(job_id, metadata)).start()

    def _encode_video(self, job_id, metadata):
        try:
            job_dir = os.path.join(ENHANCED_FRAMES_DIR, f"job_{job_id}")
            quality_levels = [d for d in os.listdir(job_dir) if d.startswith("quality_")]

            for quality in quality_levels:
                frames_dir = os.path.join(job_dir, quality)
                self._encode_quality_level(job_id, quality, frames_dir, metadata)

            logger.info(f"Encoding completed for job {job_id}")
        except Exception as e:
            logger.error(f"Error encoding video for job {job_id}: {str(e)}")

    def _encode_quality_level(self, job_id, quality, frames_dir, metadata):
        frame_files = sorted([f for f in os.listdir(frames_dir) if f.endswith('.png')])
        if not frame_files:
            logger.warning(f"No frames found for job {job_id}, quality {quality}")
            return

        # Read the first frame to get dimensions
        first_frame = cv2.imread(os.path.join(frames_dir, frame_files[0]))
        height, width = first_frame.shape[:2]

        # Set up video writer
        output_file = os.path.join(OUTPUT_DIR, f"job_{job_id}_{quality}.mp4")
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        fps = metadata.get('fps', 30)  # Default to 30 if not specified
        out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

        for frame_file in frame_files:
            frame = cv2.imread(os.path.join(frames_dir, frame_file))
            out.write(frame)

        out.release()
        logger.info(f"Encoded video saved: {output_file}")

encoder_service = EncoderService()

@app.route('/encode', methods=['POST'])
def start_encoding():
    data = request.json
    job_id = data.get('job_id')
    metadata = data.get('metadata')

    if not job_id or not metadata:
        return jsonify({"error": "Missing job_id or metadata"}), 400

    encoder_service.start_encoding(job_id, metadata)
    return jsonify({"message": "Encoding started", "job_id": job_id}), 200

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    app.run(host='0.0.0.0', port=5002)