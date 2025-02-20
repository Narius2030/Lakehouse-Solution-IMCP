import io
import requests
import base64
import pandas as pd
from minio import Minio
from minio.error import S3Error
from pyvi import ViTokenizer
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import ArrayType, StringType


@pandas_udf(ArrayType(StringType()))
def tokenize_vietnamese(text_series: pd.Series) -> pd.Series:
    return text_series.apply(lambda text: ViTokenizer.tokenize(text).split())

@pandas_udf(StringType())
def generate_caption(image_url):
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
    
@udf(StringType())
def upload_image(image_base64, object_name, bucket_name="mlflow"):
    image_binary = base64.b64decode(image_base64)
    minio_client = Minio(endpoint='160.191.244.13:9000',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)
    try:
        stream_bytes = io.BytesIO(image_binary)
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f"user-data/images/{object_name}",
            data = stream_bytes,
            length = stream_bytes.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"Successfully uploaded {object_name} to {bucket_name}!")
        return "success"
    except S3Error as err:
        raise Exception(f"Error uploading file: {err}")