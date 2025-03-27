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
        Bạn là một người khiếm thị đang nghe mô tả về tình huống giao thông xung quanh. Hãy mô tả thành đoạn văn ngắn gọn và khách quan các tiêu chí sau:  
        **Tình trạng giao thông**: 
            - Mô tả tình trạng giao thông hiện tại bằng một câu đơn, tập trung vào phương tiện chính, biển báo giao thông, đèn tín hiệu, con người và bối cảnh trong tấm hình.  
        **Vị trí các đối tượng cố định**: 
            - Chỉ rõ vị trí "trái", "phải", "phía trước", "chính giữa", "bên lề"... của các đối tượng cố định như biển báo, đèn tín hiệu, chốt cảnh sát...   
            - Nếu có các biển báo giao thông hoặc đèn tín hiệu giao thông, nêu rõ nội dung của chúng.
        **Làn đường và hướng di chuyển**: 
            - Chỉ rõ phương tiện di chuyển (cùng chiều hoặc ngược chiều) so với tôi (góc nhìn của ảnh).
            - Nếu phương tiện băng ngang, nêu rõ hướng di chuyển ("từ trái sang phải", "từ phải sang trái").
        **Góc nhìn trong ảnh**: 
            - Xác định vị trí hiện tại so với góc chụp ảnh (ví dụ: đứng trên vỉa hè, giữa đường, hay nhìn từ xa). Xưng hô "bạn".
        **Khả năng di chuyển an toàn**:
            - Xác định chính xác vị trí "trái", "phải", "phía trước" hoặc "chính giữa" cho làn đường có vỉa hè, vạch qua đường cho người đi bộ hoặc làn đường không có vật cản ảnh hưởng đến việc di chuyển an toàn.
        **Kèm theo các ràng buộc điều kiện sau**:
            - Chỉ sử dụng câu đơn có chủ ngữ, động từ, bổ ngữ rõ ràng. 
            - Sử dụng câu tự nhiên, có thể nói thành lời mà không gây khó hiểu
            - Không mô tả thông tin hiển nhiên.
            - Tách ý bằng dấu chấm (.) và không dùng dấu phẩy (,) hoặc chấm phẩy (;) để nối câu.
            - Không thêm cảm xúc hay suy đoán. Không quá 20 từ. Nếu dài hơn, hãy rút gọn.
            - Các câu phải nối tiếp nhau trên cùng một dòng, không xuống dòng mới.
    """
    # MinIO
    MINIO_HOST:str = os.getenv('MINIO_HOST')
    MINIO_PORT:str = os.getenv('MINIO_PORT')
    MINIO_USER:str = os.getenv('MINIO_USER')
    MINIO_PASSWD:str = os.getenv('MINIO_PASSWD')
    MINIO_URL:str = os.getenv('MINIO_URL')
    AUGMENTED_IMAGE_URL:str = f"{MINIO_URL}/augmented-data/images"
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