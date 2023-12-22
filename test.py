import os
import sys
import io
import zipfile
import json
import datetime
from tqdm import tqdm
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
import time
import requests 
import socket
import uuid

global count 

def logError(message):
    """
    Log the error message, wait for 5 seconds, and then exit the program.
    """
    print("Calling for support:", message)
    print("Sleeping for 5 seconds...")
    time.sleep(5)
    sys.exit()

def login(username, password, url):
    """
    Attempt to log in with the given credentials and fetch user details.
    """
    try:
        response = requests.get(f"{url}/api/v1/user/fetchUserDetails?username={username}&password={password}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logError(f"Error in login function")

def create_zip_file(folder_path, company_name):
    """
    Create a zip file of the specified folder.
    """
    try:
        zip_name = f"{company_name}_backup.zip"
        zip_path = os.path.join(os.path.dirname(folder_path), zip_name)
        
    for root, dirs, files in os.walk(folder_path):
        with zipfile.ZipFile(zip_path, 'w', zipfile.DEFLATED) as zipf:
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        zipf.write(file_path, os.path.relpath(file_path, os.path.join(folder_path, '..')))
                    except (PermissionError, OSError) as file_error:
                        logError(f"Error writing file to zip: {file_error}")
        
        return zip_path
        except Exception as e:
            logError(f"Error in zip file creation: {e}")

def upload_to_drive(zip_path, company_name, folder_id, credentials_path):
    global count 
    """
    Upload the specified zip file to Google Drive.

    Args:
        zip_path (str): The path of the zip file to be uploaded.
        company_name (str): The name of the company.
        folder_id (str): The ID of the destination folder in Google Drive.
        credentials_path (str): The path of the service account credentials JSON file.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_info(credentials_path, scopes=SCOPES)
    service = build('drive', 'v3')  

    try:
        now = datetime.datetime.now()
        zip_name = f"{company_name}_backup.zip"
        file_metadata = {'name': zip_name, 'parents': [folder_id]}
        existing_files = service.files().list(q=f"name='{zip_name}' and trashed=false", fields='files(id)').execute()
        total_size = os.path.getsize(zip_path)

        if existing_files.get('files'):
            file_id = existing_files['files'][0]
            media = MediaFileUpload(zip_path, chunksize, resumable=t)
            request = service.files().update(fileId=file_id, media_body=media, fields='id', supportsTeamDrives=True)
        else:
            media = MediaFileUpload(zip_path, chunksize=1024*1024, resumable=True)
            request = service.files().create(body=file_metadata, media_body=media, fields='id', supportsTeamDrives=True)

        progress = tqdm(total=total_size, unit='B', unit_scale=True, desc=f'Uploading {os.path.basename(zip_path)}')

        while True:
            try:
                status, done = request.next_chunk()
                if status:
                    progress.update(status.resumable_progress - progress.n)
                if done:
                    progress.close()
                    break
            except (HttpError, ConnectionResetError) as error:
                print(f"An error occurred: {error}. Retrying upload in 30 seconds.")
                time.sleep(30)

    except HttpError as error:
        count = 1 
        print(f'An error occurred: {error}')
        error_content = error.content
        print(f'Error Content: {error_content}')


if __name__ == '__main__':
    try:
        username = sys.argv[1]
        password = sys.argv[2]
        url = sys.argv[3]
        data = login(username, password, url)
        creds = data['result']['googleServiceAccount'][0]
        config = data['result']['googleDriveBackupConfig'][0]['backup']
        print('login success upload started ')
        for index, value in enumerate(config):
            destination_folder_id = value["driveFolderId"]
            folder_path = value["filePath"]
            company_name = value["fileName"]
            zip_file_path = create_zip_file(folder_path, company_name)
            print("zip created for", folder_path)
            try:
                print("upload started")
                upload_to_drive(zip_file_path, company_name, destination_folder_id, creds['backup'])
                os.remove(zip_file_path)
            
            except Exception as e:
                count = 1 
                logError(f"error uploading data: {e}")

        logError("all upload done")

    except Exception as e:
        print("error", e)

        

    except Exception as e:
        print("Error:", e)


