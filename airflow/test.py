# from utils.operators.trinodb import SQLOperators
from core.config import get_settings
import polars as pl
import requests

# settings = get_settings()
# sql_opt = SQLOperators('imcp', settings)


def perform_imc(image_path):
    response = requests.post(
        url="https://0f0a-34-125-25-209.ngrok-free.app/imc",  # Ensure this URL matches your Ollama service endpoint
        json={
            "image_url": image_path,
        }
    )
    print("Response in = ", response.elapsed.total_seconds())
    if response.status_code == 200:
        return response.json().get("response_message")
    else:
        print("Error:", response.status_code, response.text)
        return None
    
if __name__ == "__main__":
    result = perform_imc("https://img.cand.com.vn/NewFiles/Images/2024/03/19/a-1710822064944.jpg")
    print("Image Caption: ", result)