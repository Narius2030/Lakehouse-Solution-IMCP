import io
import requests
import base64
import time
import pandas as pd
import google.generativeai as genai
from datetime import datetime
from minio import Minio
from PIL import Image
from minio.error import S3Error
from pyvi import ViTokenizer
from pyspark.sql.functions import pandas_udf, udf                       # type: ignore
from pyspark.sql.types import ArrayType, StringType, TimestampType      # type: ignore


@udf(TimestampType())
def get_current_time():
    """Get the current time.

    Returns:
        str: Current time in the format "YYYY-MM-DD HH:MM:SS".
    """
    return datetime.now()


@pandas_udf(ArrayType(StringType()))
def tokenize_vietnamese(text_series: pd.Series) -> pd.Series:
    """Tokenize Vietnamese text.

    Args:
        text_series (pd.Series): Input pandas Series of text.

    Returns:
        pd.Series: Series of tokenized text.
    """
    return (text_series
                .fillna("")
                .apply(lambda text: ViTokenizer.tokenize(text).split())
            )

    
@udf(StringType())
def upload_image(image_base64, object_name, bucket_name="lakehouse"):
    """Upload an image to MinIO.

    Args:
        image_base64 (str): Base64 encoded image data.
        object_name (str): Name of the object to be uploaded.
        bucket_name (str, optional): Name of the bucket in MinIO. Defaults to "mlflow".

    Raises:
        Exception: Errors when uploading file.

    Returns:
        str: "success" if upload is successful.
    """
    image_binary = base64.b64decode(image_base64)
    minio_client = Minio(endpoint='160.191.244.13:9000',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)
    try:
        stream_bytes = io.BytesIO(image_binary)
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f"imcp/user-data/images/{object_name}",
            data = stream_bytes,
            length = stream_bytes.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"Successfully uploaded {object_name} to {bucket_name}!")
        return "success"
    except S3Error as err:
        raise Exception(f"Error uploading file: {err}")
        

@udf(StringType())
def caption_generator(gemini_key, image_url, max_retries=3):
    """Generate a caption for an image using Gemini.

    Args:
        gemini_key (str): API key for Gemini service.
        image_url (str): URL of the image to be captioned.
        max_retries (int, optional): Maximum number of retries for API calls. Defaults to 3.

    Returns:
        str: Caption for the image.
    """
    genai.configure(api_key=gemini_key)
    prompt = """
        Describe the image naturally, as if a person were observing and explaining it. Keep it concise (2-3 sentences) but informative.  

        Some aspects to consider if present:  
        - Traffic: What vehicles are in the scene? Is traffic dense or light?  
        - Roads: Are they clean, well-maintained, or obstructed? Any road signs or markings?  
        - Traffic signals: Is the light red, green, or yellow? Are people stopping or moving?  
        - People: Are there pedestrians? Are they wearing helmets or carrying umbrellas?  
        - Weather: Is it sunny, rainy, foggy, or nighttime?  

        Respond in Vietnamese naturally, as if describing the scene to someone else.
    """
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = requests.get(image_url, timeout=10, verify=False)  # Bỏ qua SSL verify
            response.raise_for_status()
            image = Image.open(io.BytesIO(response.content))

            # Resize image if too large
            max_size = (800, 800)  # Giới hạn kích thước tối đa
            if image.size[0] > max_size[0] or image.size[1] > max_size[1]:
                image.thumbnail(max_size, Image.Resampling.LANCZOS)
                
            if image is None:
                return None

            response = model.generate_content([prompt, image])
            return response.text

        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Error generating content: {e}")
                return "Can't generate caption"
            time.sleep(2 * (attempt + 1)) 