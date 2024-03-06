import os
import shutil
import zipfile
import datetime
import boto3
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Authenticate with Google Drive
def authenticate_google_drive(credentials_file):
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_file)
    if gauth.credentials is None:
        print("Google Drive credentials not found or expired.")
        return None
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    gauth.SaveCredentialsFile(credentials_file)
    return GoogleDrive(gauth)

# Download Google Drive folder
def download_google_drive_folder(drive, folder_id, destination_path):
    folder = drive.CreateFile({'id': folder_id})
    folder.GetContentFile(destination_path)

# Upload a file to S3 using AWS profile
def upload_to_s3(file_path, s3_bucket, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')
    with open(file_path, "rb") as f:
        s3.upload_fileobj(f, s3_bucket, os.path.basename(file_path))

# Main function
def main():
    # Google Drive credentials file
    google_creds_file = "C:/Code/google_creds_file.json"

    # Google Drive folder ID
    google_drive_folder_id = "project-management"

    # Destination folder for downloaded Google Drive folder
    destination_folder = "s3://ti-p-data/google_drive/"

    # S3 bucket name
    s3_bucket = "ti-p-data"

    # AWS profile name
    aws_profile = "admin"

    # Connect to Google Drive
    drive = authenticate_google_drive(google_creds_file)
    if not drive:
        print("Failed to authenticate with Google Drive.")
        return

    # Download Google Drive folder
    download_google_drive_folder(drive, google_drive_folder_id, destination_folder)

    # Upload the downloaded folder to S3 bucket
    upload_to_s3(destination_folder, s3_bucket, aws_profile)

    # Clean up: Delete the downloaded folder
    shutil.rmtree(destination_folder)

if __name__ == "__main__":
    main()
