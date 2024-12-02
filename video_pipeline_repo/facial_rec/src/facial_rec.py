import threading
from queue import Queue
import numpy as np
import cv2 
import face_recognition
import logging
import os
import sys 

# Configure logging to output to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FacialRecognitionService:
    def __init__(self):
        self.input_queue = Queue()
        self.output_queue = Queue()
        self.is_running = True
    
    def start(self):
        while self.is_running:
            if not self.input_queue.empty():
                logger.info(f"Reading frame in the facial recognition service now")
                frame, metadata = self.input_queue.get()
                logger.info(f"Frame before facial recognition: shape={frame.shape}, dtype={frame.dtype}, first pixel={frame[0,0]}")
                recognized_faces = self._recognize_faces(frame, metadata)
                self._save_annotated_frame(frame, recognized_faces, metadata)
                self.output_queue.put((None, None))  # Send empty output to process service
    
    def _recognize_faces(self, frame, metadata):
        # Ensure the frame is contiguous and in the correct format
        frame = np.ascontiguousarray(frame)
        if frame.shape[2] == 3:  # If it's a 3-channel image
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        elif frame.shape[2] == 4:  # If it has an alpha channel
            frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2RGB)
        
        logger.info(f"Frame shape before running facial rec: {frame.shape}, dtype: {frame.dtype}")
        face_locations = face_recognition.face_locations(frame)
        
        recognized_faces = []
        for (top, right, bottom, left) in face_locations:
            recognized_faces.append(("Unknown", (left, top, right, bottom)))

        return recognized_faces

    def _save_annotated_frame(self, frame, recognized_faces, metadata):
        for _, (left, top, right, bottom) in recognized_faces:
            cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)
            cv2.putText(frame, "Face Detected", (left, top - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)

        output_folder = os.path.join('processed_frames', metadata['job_id'], metadata['quality'])
        os.makedirs(output_folder, exist_ok=True)
        frame_filename = f"frame_{metadata['frame_number']:06d}.png"
        frame_path = os.path.join(output_folder, frame_filename)
        cv2.imwrite(frame_path, frame)

    def stop(self):
        self.is_running = False