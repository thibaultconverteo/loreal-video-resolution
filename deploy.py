import os
from config import SERVICE_NAME, REGION,  IMAGE_NAME, PROJECT_ID, MEMORY, CPU


stream = os.popen(f'gcloud config set project {PROJECT_ID}')
output = stream.read()
print(output)
# stream = os.popen(f'gcloud config set builds/use_kaniko False')
# stream = os.popen(f'gcloud config set project {PROJECT_ID}')
# output = stream.read()
# print(output)
# stream = os.popen(f'gcloud run deploy {SERVICE_NAME} --source . --region {REGION} --memory {MEMORY} --cpu {CPU}')
# output = stream.read()
# print(output)


stream = os.popen(f'gcloud config set builds/use_kaniko True')
output = stream.read()
print(output)
stream = os.popen(f'gcloud builds submit --tag {IMAGE_NAME} .')
output = stream.read()
print(output)
stream = os.popen(f'gcloud beta run deploy {SERVICE_NAME} --image {IMAGE_NAME} --region {REGION} --memory {MEMORY} --cpu {CPU}')
output = stream.read()
print(output)