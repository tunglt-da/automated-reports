import os
import logging
import tempfile
from datetime import datetime
from PIL import ImageGrab
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import numpy as np

def setup_logging(log_path):
    """Set up logging to file and console."""
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    
    log_file_path = os.path.join(log_path, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.txt")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info('Logging system initialized')

def capture_area(left, top, right, bottom, export_name):
    """Capture a screen area and save as an image."""
    try:
        image = ImageGrab.grab(bbox=(left, top, right, bottom))
        temp_path = tempfile.gettempdir()
        image_path = os.path.join(temp_path, export_name)
        image.save(image_path, format='PNG')
        return image_path
    except Exception as e:
        logging.error(f"Capture image {export_name} failed: {str(e)}")
        return None

def upload_image(file_path, folder_id, service_account_path):
    """Upload an image to Google Drive and return a shared link."""
    try:
        gauth_settings = {
            'client_config_backend': 'service',
            'service_config': {
                'client_json_file_path': service_account_path
            }
        }
        gauth = GoogleAuth(settings=gauth_settings)
        gauth.ServiceAuth()
        drive = GoogleDrive(gauth)
        
        file_metadata = {
            'title': 'image.jpg',
            'parents': [{'id': folder_id}]
        }
        file_drive = drive.CreateFile(file_metadata)
        file_drive.SetContentFile(file_path)
        file_drive.Upload()
        
        file_drive.InsertPermission({
            'type': 'anyone',
            'value': 'anyone',
            'role': 'reader'
        })
        return f"https://drive.google.com/file/d/{file_drive['id']}/view"
    except Exception as e:
        logging.error(f"Upload image {os.path.basename(file_path)} failed: {str(e)}")
        return None

def convert_numpy(obj):
    """Convert NumPy types to Python native types."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj
