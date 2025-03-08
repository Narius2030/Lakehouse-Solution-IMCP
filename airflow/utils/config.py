import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path("./airflow") / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):
    # MongoDB Atlas
    DATABASE_URL: str = os.getenv('MONGO_ATLAS_PYTHON')
    # Gemini
    GEMINI_API_KEY:str = os.getenv('GEMINI_API_KEY')
    GEMINI_PROMPT:str = """
        Describe the image naturally, as if a person were observing and explaining it. Keep it concise (2-3 sentences) but informative.  

        Some aspects to consider if present:  
        - Traffic: What vehicles are in the scene? Is traffic dense or light?  
        - Roads: Are they clean, well-maintained, or obstructed? Any road signs or markings?  
        - Traffic signals: Is the light red, green, or yellow? Are people stopping or moving?  
        - People: Are there pedestrians? Are they wearing helmets or carrying umbrellas?  
        - Weather: Is it sunny, rainy, foggy, or nighttime?  

        Respond in Vietnamese naturally, as if describing the scene to someone else.
    """
    # Kafka
    KAFKA_ADDRESS:str = os.getenv('KAFKA_ADDRESS')
    KAFKA_PORT:str = os.getenv('KAFKA_PORT')
    # MinIO
    MINIO_HOST:str = os.getenv('MINIO_HOST')
    MINIO_PORT:str = os.getenv('MINIO_PORT')
    MINIO_USER:str = os.getenv('MINIO_USER')
    MINIO_PASSWD:str = os.getenv('MINIO_PASSWD')
    MINIO_URL:str = os.getenv('MINIO_URL')
    AUGMENTED_IMAGE_URL:str = f"{MINIO_URL}/augmented/images"
     # Trino
    TRINO_USER:str = os.getenv('TRINO_USER')
    TRINO_HOST:str = os.getenv('TRINO_HOST')
    TRINO_PORT:str = os.getenv('TRINO_PORT')
    TRINO_CATALOG:str = os.getenv('TRINO_CATALOG')
    # Extracted Feature File Path
    EXTRACT_FEATURE_PATH:str = os.getenv('EXTRACT_FEATURE_PATH')
    # Working Directory
    WORKING_DIRECTORY:str = '/opt/airflow'
    
    
def get_settings() -> Settings:
    return Settings()