# from utils.operators.trinodb import SQLOperators
from core.config import get_settings
from utils.kafka_clients import Prod, Cons
from datetime import datetime
from PIL import Image
import polars as pl
import requests
import base64
import time
import io
import json

settings = get_settings()
# sql_opt = SQLOperators('imcp', settings)


def perform_imc(image_url):
    response = requests.post(
        url="https://e555-34-125-45-105.ngrok-free.app/ocr",  # Ensure this URL matches your Ollama service endpoint
        json=image_url
    )
    print("Response in = ", response.elapsed.total_seconds())
    if response.status_code == 200:
        return response.json().get("response_message")
    else:
        print("Error:", response.status_code, response.text)
        return ""

def image_generator():
    with open('./data/workspace.jpg', 'rb') as file:
        image_bytes = file.read()
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    with Image.open('./data/workspace.jpg') as img:
        width, height = img.size
    
        value = {
            'image_name': f"image_{timestamp}.jpg",
            'image_base64': str(image_base64),
            'image_size': f"{width}x{height}"
        }
        
        yield {'key': timestamp, 'value': value}
        time.sleep(2)

def read_image_from_kafka(message):
    value = message.value().decode('utf-8')
    image_base64 = json.loads(value)['image_base64']
    image_binary = base64.b64decode(image_base64)
    image = Image.open(io.BytesIO(image_binary))
    image.show()

if __name__ == "__main__":
    # result = perform_imc("https://img.cand.com.vn/NewFiles/Images/2024/03/19/a-1710822064944.jpg")
    # print("Image Caption: ", result)
    
    producer = Prod(settings.KAFKA_ADDRESS, 'mobile-images', image_generator)
    producer.run()
    
    # consumer = Cons(settings.KAFKA_ADDRESS, 'test', 'read_image', read_image_from_kafka)
    # consumer.run()