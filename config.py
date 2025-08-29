PROJECT_ID = 'c-loreal-content-coding'
PROJECT_NUMBER = '136331703192'
# REGION = 'europe-west4'
INDEX = 1

REGION = 'europe-west9'
SERVICE_NAME = f'video-resolution'
IMAGE_NAME = f'gcr.io/{PROJECT_ID}/{SERVICE_NAME}:latest'
# gcr.io/YOUR_PROJECT_ID/YOUR_SERVICE_NAME:$VERSION
BASE_URL = f'https://{SERVICE_NAME}-{PROJECT_NUMBER}.{REGION}.run.app'
MEMORY = '4096Mi'
CPU = 8
BUCKET_NAME = 'c-loreal-content-coding-artefacts'
DOWNLOAD_BUCKET_NAME = 'downloaded_videos_traackr_250306'
COPY_BUCKET_NAME = 'c-loreal-content-coding-original-mp4'
VIDEO_PREFIX = '0_yt_urls'
FACE_PREFIX = ''

SERVICE_ACCOUNT_EMAIL = f'{PROJECT_NUMBER}-compute@developer.gserviceaccount.com'

TIKTOK_CREATIVES_REF_TABLE = 'c-loreal-content-coding.face_recognition.tiktok_creatives_ref'
TIKTOK_USERS_REF_TABLE = 'c-loreal-content-coding.face_recognition.tiktok_users_ref'
TIKTOK_USERS_CREA_REF_TABLE = 'c-loreal-content-coding.face_recognition.tiktok_users_crea_ref'

SCOPES = ['https://www.googleapis.com/auth/cloud-platform', 
        ]
