import yaml
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MongoDB Atlas
    MONGODB_ATLAS_URI:str = "mongodb+srv://nhanbuimongo:nhanbui@mongodb-cluster.eozg9.mongodb.net/?retryWrites=true&w=majority&appName=mongodb-cluster"
    
    # Kafka
    KAFKA_ADDRESS:str = "160.191.244.13"
    KAFKA_PORT:str = "9092"
    
    # Postgresql
    DB_HOST:str = "160.191.244.13"
    DB_PORT:str = "9092"
    DB_USER:str = "admin"
    DB_PASSWORD:str = "admin"
    DB_URL:str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
    
    # MinIO
    MINIO_ENDPOINT:str = "http://160.191.244.13:9000"
    MINIO_ROOT_USER:str = "minio"
    MINIO_ROOT_PASSWORD:str = "minio123"


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