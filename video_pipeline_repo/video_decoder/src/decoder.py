import os
import sys
import logging
import requests
import subprocess
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import threading  

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Define Google Drive scope
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

app = Flask(__name__)

class VideoDecoderService:
    def __init__(self, credentials_path, local_storage_path):
        self.credentials_path = credentials_path
        self.drive_service = None
        self.local_storage_path = local_storage_path

    def authenticate_google_drive(self):
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=DRIVE_SCOPES)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            logger.info("Successfully authenticated with Google Drive")
        except Exception as e:
            logger.error(f"Error authenticating with Google Drive: {e}")

    def download_video(self, file_id, job_id):
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            file.seek(0)
            
            # Get the file metadata to retrieve the original file name
            file_metadata = self.drive_service.files().get(fileId=file_id, fields='name').execute()
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

    def process_video(self, file_path, metadata, job_id, user_settings):
        # Placeholder for video processing logic
        try:
            # Create output folder
            output_folder = f"decoded_storage/decoded_frames_{job_id}"
            os.makedirs(output_folder, exist_ok=True)

            # Use metadata provided by ingestion service
            fps = metadata.get('fps')

            # User settings
            quality_levels = user_settings.get('quality_levels', ['1280x720'])  # Default to 720p if not specified

            # Decode video to raw frames
            # decode_command = f'ffmpeg -i "{file_path}" -vf fps={fps},scale={target_resolution}" "{output_folder}/frame_%06d.raw"'
            # self.run_ffmpeg_command(decode_command)
            # Adaptive Bitrate Decoding
            for quality in quality_levels:
                quality_folder = os.path.join(output_folder, f"quality_{quality}")
                os.makedirs(quality_folder, exist_ok=True)
                decode_command = f'ffmpeg -i "{file_path}" -vf "fps={fps},scale={quality}" "{quality_folder}/frame_%06d.raw"'
                self.run_ffmpeg_command(decode_command)
                logger.info(f"Decoded video to quality {quality}")

            logger.info(f"Successfully decoded video: {file_path}")
        except Exception as e:
            logger.error(f"Unexpected error during video decoding: {e}")
            return None

    def run_ffmpeg_command(self, command):
        try:
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg command failed: {e.stderr}")
            return None        

decoder_service = VideoDecoderService('video-decoder-input-credentials.json', 'storage/')

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
            decoder_service.process_video(downloaded_file_path, metadata, job_id, user_setting)
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