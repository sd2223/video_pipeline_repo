from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import cv2
import os

app = Flask(__name__)

# Define scopes for Google Drive access
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Path to your Google Drive service account key file
drive_credentials_path = 'preprocessing-service-credentials.json'

# Authenticate with Google Drive using service account credentials
drive_credentials = Credentials.from_service_account_file(drive_credentials_path, scopes=DRIVE_SCOPES)
drive_service = build('drive', 'v3', credentials=drive_credentials)

@app.route('/preprocess', methods=['POST'])
def preprocess_video():
    data = request.get_json()
    file_id = data.get('file_id')

    if not file_id:
        return jsonify({"error": "No file ID provided"}), 400

    # Download video from Google Drive
    video_file_path = download_video_from_drive(file_id)

    if not video_file_path:
        return jsonify({"error": "Failed to download video"}), 500

    # Perform basic preprocessing (resize and grayscale)
    processed_file_path = preprocess_video_file(video_file_path)

    return jsonify({"message": f"Video processed successfully: {processed_file_path}"}), 200

def download_video_from_drive(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()

        # Save downloaded video locally
        file_io.seek(0)
        output_path = 'downloaded_video.mp4'
        
        with open(output_path, 'wb') as f:
            f.write(file_io.read())
        
        return output_path

    except Exception as e:
        print(f"Error downloading video from Google Drive: {e}")
        return None

def preprocess_video_file(video_file_path):
    try:
        cap = cv2.VideoCapture(video_file_path)
        
        # Define output path for processed video
        output_file_path = 'processed_video.mp4'
        
        # Get original dimensions and FPS of input video
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        
        # Define codec and create VideoWriter object for saving processed video
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_file_path, fourcc, fps, (width // 2, height // 2), isColor=False)

        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Resize and convert to grayscale as part of preprocessing
            resized_frame = cv2.resize(frame, (width // 2, height // 2))
            gray_frame = cv2.cvtColor(resized_frame, cv2.COLOR_BGR2GRAY)

            # Write preprocessed frame to output file
            out.write(gray_frame)

        cap.release()
        out.release()
        
        print(f"Processed video saved at {output_file_path}")
        
        return output_file_path

    except Exception as e:
        print(f"Error during preprocessing: {e}")
        return None

if __name__ == '__main__':
    app.run(debug=True)