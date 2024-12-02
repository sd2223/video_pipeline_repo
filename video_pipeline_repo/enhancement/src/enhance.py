import threading
from queue import Queue
import numpy as np
import cv2 
import logging
import sys
import os

# Configure logging to output to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_DIR = '../../enhancement/src/enhanced_frames'

class EnhancementService:
    def __init__(self):
        self.input_queue = Queue()
        self.output_queue = Queue()
        self.is_running = True
        self.output_dir = OUTPUT_DIR
    
    def start(self):
        while self.is_running:
            if not self.input_queue.empty():
                frame, metadata = self.input_queue.get()
                enhanced_frame = self._enhance_frame(frame)
                self._save_enhanced_frame(enhanced_frame, metadata)
                self.output_queue.put((enhanced_frame, enhanced_frame))
    
    def _enhance_frame(self, frame):
        # Apply noise reduction (Gaussian Blur)
        frame = cv2.GaussianBlur(frame, (5, 5), 0)
        
        # Apply color correction
        frame = self._color_correct(frame)
        
        # Apply color grading
        frame = self._color_grade(frame)
        
        # Apply deblurring (Unsharp Masking)
        frame = self._deblur(frame)
        
        return frame
    
    def _color_correct(self, frame):
        lab = cv2.cvtColor(frame, cv2.COLOR_RGB2LAB)
        l, a, b = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        l = clahe.apply(l)
        lab = cv2.merge((l, a, b))
        return cv2.cvtColor(lab, cv2.COLOR_LAB2RGB)
    
    def _color_grade(self, frame, temperature=10, tint=0):
        b, g, r = cv2.split(frame)
        b = np.clip(b.astype(np.float32) + temperature, 0, 255).astype(np.uint8)
        r = np.clip(r.astype(np.float32) + tint, 0, 255).astype(np.uint8)
        return cv2.merge([b, g, r])
    
    def _deblur(self, frame, kernel_size=5):
        gaussian = cv2.GaussianBlur(frame, (kernel_size, kernel_size), 0)
        return cv2.addWeighted(frame, 1.5, gaussian, -0.5, 0)

    def _save_enhanced_frame(self, frame, metadata):
        job_id = metadata['job_id']
        frame_number = metadata['frame_number']
        quality = metadata['quality']
        
        job_dir = os.path.join(self.output_dir, f"job_{job_id}", f"quality_{quality}")
        os.makedirs(job_dir, exist_ok=True)
        
        frame_path = os.path.join(job_dir, f"frame_{frame_number:06d}.png")
        try:
            cv2.imwrite(frame_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
            logger.info(f"Enhanced frame saved: {frame_path}")
        except Exception as e:
            logger.error(f"Error saving enhanced frame: {str(e)}")  
    
    def stop(self):
        self.is_running = False