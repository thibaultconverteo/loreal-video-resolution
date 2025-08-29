
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.10-slim as build

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
# RUN apt-get update && apt-get install -y cmake g++
RUN apt-get update
RUN apt-get install -y ffmpeg
# RUN apt-get install -y libglib2.0-0 
# RUN apt-get install -y libsm6 libxext6 libxrender-dev


# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.

COPY . ./
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --upgrade yt-dlp
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app