from urllib.parse import quote
# import asyncio
from config import SERVICE_NAME, BASE_URL, SERVICE_ACCOUNT_EMAIL, PROJECT_ID, DOWNLOAD_BUCKET_NAME, PROJECT_NUMBER, REGION
from google.cloud import logging, storage, tasks_v2
# from google.protobuf import timestamp_pb2
import json, os, re
import yt_dlp
from datetime import datetime

# logging client
log_client = logging.Client()
service_name = os.environ.get("K_SERVICE", "unknown-service")  # Get K_SERVICE or use a default
logger = log_client.logger(service_name)
res = logging.Resource(
    type="cloud_run_revision",
    labels={
        "project_id":PROJECT_ID,
        "service_name":service_name
        }
    )

logger.default_resource = res

gs_client = storage.Client()  

    

# wrapper for task creation
def task(uri, method='POST', params=None, body=None, base_url=None, schedule_time=None):
    
    service_name = os.environ.get("K_SERVICE", "unknown-service")  # Get K_SERVICE or use a default
            
    # current_region = service_name[len(SERVICE_NAME_TEMPLATE)+1:]
    
    if base_url is None:
        url = f'https://{SERVICE_NAME}-{PROJECT_NUMBER}.{REGION}.run.app' + uri
    else:
        url = base_url + uri
    
    if params is not None:
        url += '?'+'&'.join(["%s=%s"%(quote(str(key)), quote(str(value))) for key, value in params.items()])  # The full url path that the task will be sent to.
    
        
    task = {
        "http_request": {  # Specify the type of request.
            "http_method": method,
            "url": url,
            "oidc_token": {
                "service_account_email": SERVICE_ACCOUNT_EMAIL,
                "audience": base_url,
                }
            }
        }
    if body is not None:
        task['http_request']['body'] = json.dumps(body).encode()
    if schedule_time is not None:
        task['schedule_time'] = schedule_time
    return task



def get_video_urls(bq_client, nb_videos, offset):
    
    query = f"""
    with base as (
    SELECT distinct platform,
    COALESCE( 
          REGEXP_EXTRACT(video_url, r'facebook.*\/videos\/(\d+).?'),
          REGEXP_EXTRACT(video_url, r'facebook.*\/posts\/(\d+).?'),
          REGEXP_EXTRACT(video_url, r'.?instagram.com/p/(.*)'),
          REGEXP_EXTRACT(video_url, r'.?instagram.com/reel/(.*)\?'),
          REGEXP_EXTRACT(video_url, r'.?instagram.com/reel/(.*)'),
          REGEXP_EXTRACT(video_url, r'instagram.com/stories/[^/]+/([0-9]+)'),
          REGEXP_EXTRACT(video_url, r'snapchat.*story/(.*)'),
          REGEXP_EXTRACT(video_url, r'snapchat.*spotlight/(.*)'),
          REGEXP_EXTRACT(video_url, r'.?v=(.*)'),
          REGEXP_EXTRACT(video_url, r'.?video/(.*)'),
          ''
        ) as video_id,
    FROM `traackr_250306.source_update_part`
    )


    select * from base
    join `c-loreal-content-coding.download_status_eu.large_files` using (video_id)
    order by video_id
    limit {nb_videos}
    offset {offset}

    """
    logger.log_text(f'{query}')
    result = bq_client.query(query).result()
    # logger.log_text(f'query')
    result_df = result.to_dataframe()
    # logger.log_text(f'dataframe')
    return result_df

