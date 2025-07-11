from utils.operators.trinodb import SQLOperators
# from utils.kafka_clients import Prod, Cons
from utils.operators.image import ImageOperator
from utils.setting import get_settings
from datetime import datetime
from PIL import Image
import requests
import base64
import time

settings = get_settings()
sql_opt = SQLOperators('imcp', settings)


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


if __name__ == "__main__":
    query = """
        SELECT * FROM imcp.layer_catalogs
        WHERE layer_name = 'refined' 
            AND storage_type = 'minio'
            AND s3_bucket = 'lakehouse'
    """
    
    data = sql_opt.execute_query(query)
    print(data)
    
    # image_array = ImageOperator.encode_image("http://images.cocodataset.org/val2017/000000399462.jpg")
    # print(image_array.keys())
    # print(image_array)