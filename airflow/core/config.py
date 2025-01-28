import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path("/opt/airflow") / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):
    # MongoDB Atlas
    DATABASE_URL: str = os.getenv('MONGO_ATLAS_PYTHON')
    # MinIO
    MINIO_HOST:str = os.getenv('MINIO_HOST')
    MINIO_PORT:str = os.getenv('MINIO_PORT')
    MINIO_USER:str = os.getenv('MINIO_USER')
    MINIO_PASSWD:str = os.getenv('MINIO_PASSWD')
    MINIO_URL:str = os.getenv('MINIO_URL')
    # Extracted Feature File Path
    EXTRACT_FEATURE_PATH:str = os.getenv('EXTRACT_FEATURE_PATH')
    RAW_DATA_PATH:str = os.getenv('RAW_DATA_PATH')
    # Working Directory
    WORKING_DIRECTORY:str = '/opt/airflow'
    
    
def get_settings() -> Settings:
    return Settings()