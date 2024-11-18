# from google.oauth2.credentials import Credentials
import gspread
from google.oauth2.service_account import Credentials as SheetsCredentials
from google.oauth2.service_account import Credentials as DriveCredentials
# from google.auth.transport.requests import Request
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
import requests
import io
import cv2
import logging
import os
import sys 
import time 
import uuid 

# Configure logging to output to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define separate scopes for Google Drive and Google Sheets
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_UPLOAD_SCOPES = ['https://www.googleapis.com/auth/drive.file']
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class VideoIngestionService:
    def __init__(self, input_drive_credentials_path, sheets_credentials_path, ingestion_drive_credentials_path, sheet_id, drive_id):
        self.input_drive_credentials_path = input_drive_credentials_path
        self.sheets_credentials_path = sheets_credentials_path
        self.ingestion_drive_credentials_path = ingestion_drive_credentials_path
        self.sheet_id = sheet_id
        self.drive_id = drive_id
        self.input_drive_service = None
        self.ingestion_drive_service = None
        self.sheets_service = None
        self.cap = None
        self.processed_videos = set()  # Track processed videos

    def authenticate_google_services(self):
        # # Check if token.json exists (this stores user's access and refresh tokens)
        # if os.path.exists('token.json'):
        #     self.credentials = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        # # If there are no valid credentials available, let the user log in.
        # if not self.credentials or not self.credentials.valid:
        #     logging.error("Failed to read from token")
        #     if self.credentials and self.credentials.expired and self.credentials.refresh_token:
        #         self.credentials.refresh(Request())
        #     else:
        #         flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, SCOPES)
        #         self.credentials = flow.run_local_server(port=0)
            
        #     # Save the credentials for future use (in token.json)
        #     with open('token.json', 'w') as token_file:
        #         token_file.write(self.credentials.to_json())

        # # Build the Google Drive service
        # self.service = build('drive', 'v3', credentials=self.credentials)   

        # Load credentials from the service account file
        # credentials = Credentials.from_service_account_file(self.service_account_file, scopes=SCOPES)
        
        # # Build the Google Drive service using service account credentials
        # self.service = build('drive', 'v3', credentials=credentials)

        # Authenticate with Google Drive for the video input folder
        drive_credentials = DriveCredentials.from_service_account_file(self.input_drive_credentials_path, scopes=DRIVE_SCOPES)
        self.input_drive_service = build('drive', 'v3', credentials=drive_credentials)

        # Authenticate with Google Sheets for the video metadata
        sheets_credentials = SheetsCredentials.from_service_account_file(self.sheets_credentials_path, scopes=SHEETS_SCOPES)
        self.gc = gspread.authorize(sheets_credentials)

        # Authenticate with Google Drive for the video ingestion output folder
        drive_credentials = DriveCredentials.from_service_account_file(self.ingestion_drive_credentials_path, scopes=DRIVE_UPLOAD_SCOPES)
        self.ingestion_drive_service = build('drive', 'v3', credentials=drive_credentials)

    def get_video_from_drive(self, file_id):
        try:
            # Get the file metadata first to retrieve the original file name
            file_metadata = self.input_drive_service.files().get(fileId=file_id, fields='name').execute()
            original_file_name = file_metadata.get('name')

            request = self.input_drive_service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            file.seek(0)
            output_path = os.path.join('input_storage', original_file_name)

            with open(output_path, 'wb') as f:
                f.write(file.getvalue())
            return output_path
        except Exception as e:
            logging.error(f"Error downloading video from Google Drive: {e}")
            return None

    def start(self, file_id):
        video_file_path = self.get_video_from_drive(file_id)
        if video_file_path is None:
            logging.error("Failed to download video from Google Drive")
            return False
        
        self.cap = cv2.VideoCapture(video_file_path)
        if not self.cap.isOpened():
            logging.error("Error opening video source: {video_file_path}")
            return False
        return video_file_path

    def read_frame(self):
        if self.cap is None:
            logging.error("Video capture not initialized")
            return None
        ret, frame = self.cap.read()
        if not ret:
            logging.info("End of video stream")
            return None
        return frame

    def stop(self):
        if self.cap:
            self.cap.release()
        logging.info("Video ingestion stopped")

    def extract_metadata(self):
        if self.cap is None:
            logging.error("Video capture not initialized")
            return None
        
        # Extract metadata using OpenCV properties
        fps = self.cap.get(cv2.CAP_PROP_FPS)  # Frames per second
        width = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)  # Width of the frame
        height = self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)  # Height of the frame
        frame_count = self.cap.get(cv2.CAP_PROP_FRAME_COUNT)  # Total number of frames
        duration = frame_count / fps if fps > 0 else 0  # Duration in seconds

        metadata = {
            "fps": fps,
            "width": width,
            "height": height,
            "frame_count": frame_count,
            "duration": duration
        }

        logging.info(f"Metadata extracted: {metadata}")
        return metadata

    def publish_metadata_to_sheets(self, metadata):
        try:
            # Open the sheet by ID and select the first worksheet
            sheet = self.gc.open_by_key(self.sheet_id).sheet1
            
            # Append a new row with metadata values
            sheet.append_row([metadata['fps'], metadata['width'], metadata['height'], metadata['frame_count'], metadata['duration']])
            
            logging.info("Metadata successfully published to Google Sheets.")
        
        except Exception as e:
            logging.error(f"Error publishing metadata to Google Sheets: {e}")    

    def upload_video_to_drive(self, file_path, folder_id):
        try:
            file_metadata = {
                'name': os.path.basename(file_path),
                'parents': [folder_id]  # Specify the target folder ID in Google Drive
            }
            media = MediaFileUpload(file_path, mimetype='video/mp4')
            file = self.ingestion_drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logging.info(f"Video uploaded to Google Drive with ID: {file.get('id')}")
            return file.get('id')

        except Exception as e:
            logging.error(f"Error uploading video to Google Drive: {e}")
            return None
    
    def process_video(self, file_id):
        # Start processing and get the original video file path
        video_file_path = self.start(file_id)

        if not video_file_path:
            logging.error("Failed to start video processing")
            return False
        
        # Check if the video is of type MP4 (Placeholder for checks later)
        if not self.is_mp4_video(video_file_path):
            logging.error(f"The video {video_file_path} is not an MP4 file. Skipping processing.")
            self.delete_local_file(video_file_path)
            return False
        
        # Extract metadata before processing the video
        metadata = self.extract_metadata()

        # Publish metadata to Google Sheets 
        if metadata:
            self.publish_metadata_to_sheets(metadata)

        frame_number = 0
        while True:
            frame = self.read_frame()
            if frame is None:
                break
            # Process the frame here (e.g., apply your video processing pipeline)
            frame_number += 1
        self.stop()

        # Mark this video as processed to avoid reprocessing it in future polls.
        self.processed_videos.add(file_id)

        # Upload processed video to Google Drive
        uploaded_file_id = self.upload_video_to_drive(video_file_path, self.drive_id)

        # Generate a placeholder job ID
        job_id = self.generate_job_id()

        # Notify decoder service via REST API
        if uploaded_file_id:
            self.notify_decoder_service(uploaded_file_id, metadata, job_id)

            # Delete the local file after successful upload
            self.delete_local_file(video_file_path)
        else:
            logging.error(f"Failed to upload video {video_file_path}. Local file not deleted.")

    def notify_decoder_service(self, file_id, metadata, job_id):
        user_setting = { "quality_levels" : ["640x360", "1280x720", "1920x1080"], 
                "priority": "high"}
        try:
            # url = "http://decoder-service:5000/decode"  # URL of the decoder service's API
            url = "http://localhost:5000/decode"  # URL to test locally
            data = {
                "file_id": file_id,
                "metadata": metadata,
                "job_id": job_id,
                "user_setting": user_setting
            }
            response = requests.post(url, json=data)
            if response.status_code == 200:
                logging.info("Decoder service notified successfully.")
            else:
                logging.error(f"Failed to notify decoder service. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error notifying decoder service: {e}")

    def generate_job_id(self):
        return str(uuid.uuid4())          

    def is_mp4_video(self, file_path):
        # Check if the file has a .mp4 extension
        if not file_path.lower().endswith('.mp4'):
            return False
        
        # This is a placeholder for more sophisticated checks here in the future
        
        return True
    
    def delete_local_file(self, file_path):
        try:
            os.remove(file_path)
            logging.info(f"Successfully deleted local file: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting local file {file_path}: {e}")
    
    def poll_for_new_videos(self):
        while True:
            try:
                # List video files in Google Drive folder (adjust query as needed)
                results = self.input_drive_service.files().list(
                    pageSize=10,
                    q="mimeType contains 'video/'",
                    fields="nextPageToken, files(id, name)"
                ).execute()

                items = results.get('files', [])

                if not items:
                    logging.info('No new videos found in Google Drive.')
                else:
                    for item in items:
                        # if item not in processed_videos()
                        logging.info(f"Found video: {item['name']} ({item['id']})")
                        # Process each new video by downloading and analyzing it.
                        self.process_video(item['id'])

                # Poll every 5 seconds (adjust interval as needed).
                time.sleep(5)

            except Exception as e:
                logging.error(f"Error during polling: {e}")
                time.sleep(60)  # Wait and retry after an error.
                
def main():

    # input_drive_credentials_path = '/video-pipeline-app/src/video-ingestion-credentials.json'  # Path to your Google Drive service account key file
    # sheets_credentials_path = '/video-pipeline-app/src/video-metadata-credentials.json'  # Path to your Google Sheets service account key file
    # ingestion_drive_credentials_path = '/video-pipeline-app/src/video-output-credentials.json'
    
    input_drive_credentials_path = 'keys/video-ingestion-credentials.json'  # Path to your Google Drive service account key file
    sheets_credentials_path = 'keys/video-metadata-credentials.json' 
    ingestion_drive_credentials_path = 'keys/video-output-credentials.json'

    # The ID of the target Google Sheet (you can find this in the URL of the sheet)
    sheet_id = '1HV7aX8dA1F8Wu2b5NEinpzNHsz1jNrZghusR0pAnKNA'

    # The ID of the target Google Drive Folder where the ingested videos will be stored
    drive_id = '1dVtj6hArWmjEvPS_Hv0VXWotDPDk9z2Y'

    service = VideoIngestionService(input_drive_credentials_path, sheets_credentials_path, ingestion_drive_credentials_path, sheet_id, drive_id)
    
    # Authenticate and build Google Drive service
    service.authenticate_google_services()

    # Start polling for new videos.
    service.poll_for_new_videos()

if __name__ == "__main__":
    main()