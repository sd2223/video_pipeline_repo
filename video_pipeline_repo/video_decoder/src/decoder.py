import os
import sys
import logging
import requests
import subprocess
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseUpload
import io
import threading  
import time 
import json 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Define Google Drive scope
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_UPLOAD_SCOPES = ['https://www.googleapis.com/auth/drive.file']
JOBS_FOLDER_ID = '1FNWMZya3sMpb1yyCbpm8YFU8tvPUM-84'

# URL of your deployed Colab notebook
colab_url = "https://colab.research.google.com/drive/10mq3XYDyyBlc9s9gMep80u2FKIsw-6T6#scrollTo=pUPhZyP95V_v"

app = Flask(__name__)

class VideoDecoderService:
    def __init__(self, input_credentials_path, job_credentials_path, local_storage_path, use_gpu=False):
        self.input_credentials_path = input_credentials_path
        self.job_credentials_path = job_credentials_path
        self.drive_service_read = None
        self.drive_service_write = None
        self.local_storage_path = local_storage_path
        self.use_gpu = use_gpu

    def authenticate_google_drive(self):
        input_credentials = Credentials.from_service_account_file(self.input_credentials_path, scopes=DRIVE_SCOPES)
        self.drive_service_read = build('drive', 'v3', credentials=input_credentials)
        logger.info("Successfully authenticated with Google Drive for decode input")

        job_credentials = Credentials.from_service_account_file(self.job_credentials_path, scopes=DRIVE_UPLOAD_SCOPES)
        self.drive_service_write = build('drive', 'v3', credentials=job_credentials)
        logger.info("Successfully authenticated with Google Drive for job data upload")

    def download_video(self, file_id, job_id):
        try:
            request = self.drive_service_read.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            file.seek(0)
            
            # Get the file metadata to retrieve the original file name
            file_metadata = self.drive_service_read.files().get(fileId=file_id, fields='name').execute()
            original_file_name = file_metadata.get('name')
            
            # Create job directory
            job_dir = os.path.join(self.local_storage_path, f"job_{job_id}")
            os.makedirs(job_dir, exist_ok=True)

            # Save the file in the job directory
            file_path = os.path.join(job_dir, original_file_name)
            with open(file_path, 'wb') as f:
                f.write(file.getvalue())

            logger.info(f"Successfully downloaded video: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error downloading video from Google Drive: {e}")
            return None

    def process_video(self, file_path, file_id, metadata, job_id, user_settings):
        try:
            # Create output folder
            output_folder = f"decoded_storage/decoded_frames_{job_id}"
            os.makedirs(output_folder, exist_ok=True)

            if self.use_gpu:
                return self.process_video_gpu(file_id, metadata, user_settings, job_id, output_folder)
            else:
                return self.process_video_cpu(file_path, metadata, user_settings, output_folder)
        except Exception as e:
            logger.error(f"Unexpected error during video decoding: {e}")
            return None

    def process_video_cpu(self, file_path, metadata, user_settings, output_folder):
        try:
            # Use metadata provided by ingestion service
            fps = metadata.get('fps')

            # User settings
            quality_levels = user_settings.get('quality_levels', ['1280x720'])  # Default to 720p if not specified

            # Adaptive Bitrate Decoding
            for quality in quality_levels:
                quality_folder = os.path.join(output_folder, f"quality_{quality}")
                os.makedirs(quality_folder, exist_ok=True)
                decode_command = f'ffmpeg -i "{file_path}" -vf "fps={fps},scale={quality}" "{quality_folder}/frame_%06d.raw"'
                self.run_ffmpeg_command(decode_command)
                logger.info(f"Decoded video to quality {quality}")
            logger.info(f"Successfully decoded video on CPU: {file_path}")
            return output_folder
        except Exception as e:
            logger.error(f"Unexpected error during video decoding: {e}")
            return None
        
    def process_video_gpu(self, file_id, metadata, user_settings, job_id, output_folder):
        try:
            job_data = {
                "file_id": file_id,
                "metadata": metadata,
                "user_settings": user_settings,
                "job_id": job_id, 
                # "output_folder": output_folder
                "status": "pending"
            }

            # Create job file
            job_metadata = {
                'name': f'pending_job_{job_id}.json',
                'parents': [JOBS_FOLDER_ID],
                'mimeType': 'application/json'
            }

            # Upload job file
            # media = MediaFileUpload(
            media = MediaIoBaseUpload(
                io.BytesIO(json.dumps(job_data).encode()),
                mimetype='application/json',
                resumable=True
            )
            
            job_file = self.drive_service_write.files().create(
                body=job_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            logger.info(f"Created job file with ID: {job_file.get('id')}")

            # Wait for job completion
            while True:
                # Check for completed job file
                result = self.drive_service_write.files().list(
                    q=f"name = 'completed_job_{job_id}.json'",
                    fields="files(id)"
                ).execute()
                
                if result.get('files'):
                    # Read result
                    completed_file = result['files'][0]
                    request = self.drive_service_write.files().get_media(fileId=completed_file['id'])
                    content = request.execute().decode('utf-8')
                    result_data = json.loads(content)
                    
                    logger.info(f"Job completed with result: {result_data['result']}")
                    return result_data['result']
                
                time.sleep(5)  # Check every 5 seconds
            
        except Exception as e:
            logger.error(f"Error in GPU processing: {e}")
            return None
        
        # logger.info(f"Sending request to Colab with payload: {payload}")
        # response = requests.post(colab_url, json=payload)

        # if response.status_code == 200:
        #     result = response.json()
        #     if result['success']:
        #         logger.info(f"Successfully processed video on GPU: {result['result']}")
        #         return result['result']
        #     else:
        #         logger.error(f"GPU processing failed: {result['error']}")
        #         return None
        # else:
        #     logger.error(f"Error decoding video on GPU: {response.text}")
        #     return None

    def run_ffmpeg_command(self, command):
        try:
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg command failed: {e.stderr}")
            return None        

decoder_service = VideoDecoderService('keys/video-decoder-input-credentials.json', 'keys/video-decoder-job-credentials.json', 'storage/', True)

@app.route('/decode', methods=['POST'])
def decode_video():
    data = request.json
    logger.info("YOLO")
    file_id = data.get('file_id')
    metadata = data.get('metadata')
    job_id = data.get('job_id')
    user_setting = data.get('user_setting')

    logger.info(f"Received decoding request for job {job_id}")

    if not file_id:
        return jsonify({"error": "No file path provided"}), 400

    # Download the video
    # downloaded_file_path = decoder_service.download_video(file_id, job_id)
    # if not downloaded_file_path:
    #     return jsonify({"error": "Failed to download video"}), 500

    # Start the decoding process asynchronously
    threading.Thread(target=process_video_async, args=(file_id, metadata, job_id, user_setting)).start()   

    return jsonify({"message": "Video decoding started", "job_id": job_id}), 200

def process_video_async(file_id, metadata, job_id, user_setting):
    try:
        # Download and process the video
        downloaded_file_path = decoder_service.download_video(file_id, job_id)
        if downloaded_file_path:
            decoder_service.process_video(downloaded_file_path, file_id, metadata, job_id, user_setting)
            logger.info(f"Video processing completed for job {job_id}")
        else:
            logger.error(f"Failed to download video for job {job_id}")
    except Exception as e:
        logger.error(f"Error processing video for job {job_id}: {str(e)}")

def main():
    decoder_service.authenticate_google_drive()
    app.run(host='0.0.0.0', port=5000)

    # List video files in Google Drive folder
    # try:
    #     results = decoder_service.drive_service.files().list(
    #         pageSize=10,
    #         q="mimeType contains 'video/'",
    #         fields="nextPageToken, files(id, name)"
    #     ).execute()
    #     items = results.get('files', [])

    #     if not items:
    #         logger.info('No video files found in Google Drive.')
    #         return

    #     logger.info('Video files found:')
    #     for item in items[:3]:  # Limit to first 3 videos
    #         logger.info(f"{item['name']} ({item['id']})")
            
    #         # Download the video
    #         downloaded_file_path = decoder_service.download_video(item['id'])
            
    #         if downloaded_file_path:
    #             logger.info(f"Successfully downloaded: {downloaded_file_path}")
    #         else:
    #             logger.error(f"Failed to download video with ID: {item['id']}")

    # except Exception as e:
    #     logger.error(f"Error listing or downloading videos: {e}")

if __name__ == "__main__":
    main()