# import socket
import ffmpeg
import os, json, re, time, pandas as pd
from numpy.random import randint
import datetime
# from google.protobuf import timestamp_pb2
from flask import Flask
from uuid import uuid4
# import requests
from requests.exceptions import MissingSchema, ConnectionError
import flask
# import pandas_gbq
# import httplib2
from google.cloud import storage, logging, tasks_v2, bigquery
# import google.auth
from utils import task, get_video_urls, logger
from config import PROJECT_ID, SERVICE_NAME, SCOPES, BUCKET_NAME, DOWNLOAD_BUCKET_NAME, COPY_BUCKET_NAME, INDEX, REGION, PROJECT_NUMBER
from google.api_core.exceptions import InvalidArgument, AlreadyExists, ServiceUnavailable
from moviepy.editor import VideoFileClip


# import pandas as pd
# from sklearn.metrics.pairwise import cosine_similarity
app = Flask(__name__)

# cloud tasks client
ct_client = tasks_v2.CloudTasksClient()
ct_parent_download_async = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-download-async')
ct_parent_list_async = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-list-sources-async')
ct_parent_extract = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-extract')
ct_parent_label = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-label')
ct_parent_list_blobs = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'list-blobs')
ct_parent_update_blobs = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'update-blobs')
ct_parent_embed = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-embed')
ct_parent_lower = ct_client.queue_path(PROJECT_ID, 'europe-west1', 'video-lower')

# storage client
gs_client = storage.Client()
bucket = gs_client.get_bucket(BUCKET_NAME)
    
# bigquery client
bq_client = bigquery.Client()

@app.route("/", methods=['GET', 'POST'])
def hello_world():
    logger.log_text('hello world')
    return "200"


@app.route("/list", methods=['GET', 'POST'])
def list():
    params = dict(flask.request.args)

    data = json.loads(flask.request.get_data(as_text=True))
    logger.log_struct(data)

    
    nb_videos = int(data.get('nb_videos'))
    offset = int(params.get('offset', '0'))
    target_task = params.get('target_task', 'log')
    
    df = get_video_urls(bq_client, nb_videos, offset)
    
    row_count = 0
    for row_count, row in enumerate(df.itertuples(), start=1):
        data['video_id'] = row.video_id
        data['platform'] = row.platform
        
        c_task = task(f'/{target_task}', method='POST', params=params, body=data) 
        response = ct_client.create_task(parent=ct_parent_lower, task=c_task) 

    if row_count < nb_videos:
        logger.log_text(f'no more videos to download')
        return "200"
    
    params['offset'] = offset + nb_videos
    c_task = task('/list', method='POST', params=params, body=data) 
    response = ct_client.create_task(parent=ct_parent_list_async, task=c_task) 
    return "200"



@app.route("/log", methods=['GET', 'POST'])
def log():
    params = dict(flask.request.args)

    data = json.loads(flask.request.get_data(as_text=True))
    logger.log_struct(data)

    bucket = gs_client.get_bucket(DOWNLOAD_BUCKET_NAME)
    
    platform = data.get('platform')
    video_id = data.get('video_id')
    
    
    original_blob_name = f'{platform}/{video_id}.mp4'
    logger.log_text(f'original blob name {original_blob_name}')
    blob = bucket.blob(original_blob_name)
    if not blob.exists():
        logger.log_text(f'could not find blob {original_blob_name} in {DOWNLOAD_BUCKET_NAME}')
        return "200"
    
    
    blob = bucket.get_blob(original_blob_name)
    file_size = blob.size
    logger.log_text(f'file size {file_size}')
    if file_size < 10*1024*1024:
        logger.log_text('already processed')
        return "200"
    
    if file_size > 20*1024*1024:
        logger.log_text('focusing on files smaller than 20MB for now')
        return "200"
        
    
    file_size_blob_name = f'0_file_size/{platform}/{video_id}.csv'
    file_size_blob = bucket.blob(file_size_blob_name)
    file_size_blob.upload_from_string(json.dumps({'video_id':video_id, 'platform':platform, 'file_size':str(file_size)}))
    logger.log_text(f"size uploaded to {file_size_blob_name} in {DOWNLOAD_BUCKET_NAME}")

    return "200"


@app.route("/lower", methods=['GET', 'POST'])
def lower():
    params = dict(flask.request.args)

    data = json.loads(flask.request.get_data(as_text=True))
    logger.log_struct(data)

    bucket = gs_client.get_bucket(DOWNLOAD_BUCKET_NAME)
    copy_bucket = gs_client.get_bucket(COPY_BUCKET_NAME)
    
    platform = data.get('platform')
    video_id = data.get('video_id')
    
    
    original_blob_name = f'{platform}/{video_id}.mp4'
    logger.log_text(f'original blob name {original_blob_name}')
    blob = bucket.blob(original_blob_name)
    if not blob.exists():
        logger.log_text(f'could not find blob {original_blob_name} in {DOWNLOAD_BUCKET_NAME}')
        return "200"
    
    rounds = 1
    input_path = f'./{video_id}.mp4'
    output_path = f'./{video_id}_{rounds}.mp4'

    blob = bucket.get_blob(original_blob_name)
    blob.download_to_filename(input_path)
    crf_value = 23
    while True:
        
        file_size = os.path.getsize(input_path)
        logger.log_text(f'input file size {file_size}')
        scale_factor = 9*1024*1024/file_size
        logger.log_text(f'scale factor {scale_factor}')

        probe = ffmpeg.probe(input_path)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

        if video_stream is None:
            logger.log_text(f"Error: No video stream found in {input_path}")
        if audio_stream is None:
            logger.log_text(f"Warning: No audio stream found in {input_path}. Proceeding without audio.")


        original_width = int(video_stream['width'])
        original_height = int(video_stream['height'])
        new_width = int(original_width * scale_factor)
        new_height = int(original_height * scale_factor)

        # Ensure new dimensions are at least 1 pixel and divisible by 2 (often required by codecs)
        new_width = max(10, new_width - (new_width % 2))
        new_height = max(10, new_height - (new_height % 2))


        preset = 'slow'
        
        logger.log_text(f"Original resolution: {video_stream['width']}x{video_stream['height']}")
        logger.log_text(f"new resolution: {new_width}x{new_height}")
        logger.log_text(f"Target CRF: {crf_value}, Preset: {preset}")

        # FFmpeg command construction
        # -i input_path: Specify input file
        # -c:a copy: Copy the audio stream without re-encoding (preserves quality and speed)
        # -c:v libx264: Encode the video using H.264 codec (common for MP4)
        # -crf {crf_value}: Constant Rate Factor. This is the primary control for file size.
        # -preset {preset}: Encoding speed vs. compression efficiency.

        input_stream = ffmpeg.input(input_path)


        scaled_video = ffmpeg.filter(input_stream.video, 'scale', new_width, new_height)

        output_args = {
            'vcodec': 'libx264',
            'crf': crf_value,
            'preset': preset,
            'acodec': 'copy', # Ensure audio is copied
            'y': None
        }

        # Map video and audio streams
        if audio_stream:
            (
                ffmpeg
                .output(scaled_video, input_stream.audio, output_path, **output_args)
                .run(overwrite_output=True)
            )
            logger.log_text(f"Video file size reduced by adjusting compression. Output saved to: {output_path}")
        else:
            # If no audio, just process video
            (
                ffmpeg
                .output(scaled_video, output_path, **output_args)
                .run(overwrite_output=True)
            )
            logger.log_text(f"Video file size reduced by adjusting compression (no audio found). Output saved to: {output_path}")

        output_size = os.path.getsize(output_path)
        logger.log_text(f'output size {output_size}')
        if output_size <= 9*1024*1024 or new_height == 10:
            break
        
        input_path = output_path
        rounds += 1
        crf_value += 2
        output_path = f'./{video_id}_{rounds}.mp4'
        logger.log_text(f'round {rounds}')
        
            
    lower_blob_name = f'{platform}/{video_id}_lower.mp4'
    lower_blob = copy_bucket.blob(lower_blob_name)
    lower_blob.upload_from_filename(output_path)
    logger.log_text(f"lower resolution uploaded to {lower_blob_name} in {COPY_BUCKET_NAME}")

    copy_blob = copy_bucket.blob(original_blob_name)
    copy_blob.upload_from_filename(f'./{video_id}.mp4')
    logger.log_text(f"original blob copied to {original_blob_name} in {COPY_BUCKET_NAME}")

    blob.upload_from_filename(output_path)
    logger.log_text(f"replaced original blob {original_blob_name} with lowered versionin {DOWNLOAD_BUCKET_NAME}")

    return "200"



@app.route("/clip", methods=['GET', 'POST'])
def clip():
    params = dict(flask.request.args)

    data = json.loads(flask.request.get_data(as_text=True))
    logger.log_struct(data)

    bucket = gs_client.get_bucket(DOWNLOAD_BUCKET_NAME)
    copy_bucket = gs_client.get_bucket(COPY_BUCKET_NAME)
    
    platform = data.get('platform')
    video_id = data.get('video_id')
    
    
    original_blob_name = f'{platform}/{video_id}.mp4'
    logger.log_text(f'original blob name {original_blob_name}')
    blob = bucket.blob(original_blob_name)
    if not blob.exists():
        logger.log_text(f'could not find blob {original_blob_name} in {DOWNLOAD_BUCKET_NAME}')
        return "200"
    
    rounds = 1
    video_id_safe = video_id.replace('-', '_').replace('~', '_')
    input_path = f'./{video_id_safe}.mp4'
    output_path = f'./{video_id_safe}_{rounds}.mp4'

    blob = bucket.get_blob(original_blob_name)
    blob.download_to_filename(input_path)
    
    try:
        clip = VideoFileClip(input_path)
    except:
        logger.log_text(f"issue processing video {platform}/{video_id}")
        return "200"
    
    duration = clip.duration
    file_size = os.path.getsize(input_path)
    
    current_time = int(data.get('current_time', '0'))
    chunk_count = int(data.get('chunk_count', '0'))
    
    # set chunk duration by validating chunk size output
    

        
    chunk_duration = duration/file_size * 9*1024*1024    
    logger.log_text(f'video {video_id} first guess chunk duration {chunk_duration}')
    while True: #reduce chunk duration until reaching less than 10MB
        
        end_time = min(current_time + chunk_duration, duration)
    
        subclip = clip.subclip(current_time, end_time)
        temp_output_path = os.path.join('./', f"{video_id_safe}_{chunk_count}.mp4")
        subclip.write_videofile(temp_output_path, codec='libx264', audio_codec='aac', fps=clip.fps)
        chunk_size = os.path.getsize(temp_output_path)
        logger.log_text(f'chunk size {chunk_size}')
        if chunk_size < 9*1024*1024:
            break
        chunk_duration = chunk_duration / chunk_size * 9*1024*1024 * 0.9
        logger.log_text(f'video {video_id} updated chunk duration {chunk_duration}')
        


    chunk_blob_name = f'0_clips/{platform}/{video_id}_{chunk_count}.mp4'
    chunk_blob = bucket.blob(chunk_blob_name)
    chunk_blob.upload_from_filename(temp_output_path)
    logger.log_text(f"chunk uploaded to {chunk_blob_name} in {BUCKET_NAME}")

    if chunk_count == 9 or current_time + chunk_duration >= duration:
        return "200"
    os.remove(input_path)
    os.remove(temp_output_path)
    
    data['current_time'] = end_time
    data['chunk_count'] = chunk_count+1
    c_task = task(f'/clip', method='POST', params=params, body=data) 
    response = ct_client.create_task(parent=ct_parent_lower, task=c_task) 


    return "200"





if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))