import yaml
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

class Settings(BaseSettings):
    # MongoDB Atlas
    MONGODB_ATLAS_URI:str = os.getenv("MONGO_ATLAS_PYTHON")
    
    # Kafka
    KAFKA_ADDRESS:str = os.getenv("KAFKA_ADDRESS")
    KAFKA_PORT:str = os.getenv("KAFKA_PORT")
    
    # Postgresql
    DB_HOST:str = os.getenv("POSTGRES_HOST")
    DB_PORT:str = os.getenv("POSTGRES_PORT")
    DB_USER:str = os.getenv("POSTGRES_USER")
    DB_PASSWORD:str = os.getenv("POSTGRES_PASSWORD")
    DB_URL:str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
    
    # MinIO
    MINIO_ENDPOINT:str = os.getenv("MINIO_ENDPOINT")
    MINIO_ROOT_USER:str = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD:str = os.getenv("MINIO_ROOT_PASSWORD")


def get_settings() -> Settings:
    return Settings()

def read_yaml():
    filename = Settings().ENDPOINT_PATH
    with open(filename, 'r') as file:
        try:
            return yaml.safe_load(file)
        except Exception as ex:
            print(ex)
            
            
if __name__ == '__main__':
    endpoints = read_yaml()
    print(endpoints)