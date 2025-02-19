import sys
sys.path.append('./airflow')
import polars as pl
import requests
from datetime import datetime
from pyvi import ViTokenizer

def tokenize_vietnamese(text: str):
    tokens = ViTokenizer.tokenize(text).split(" ")
    return tokens

def scaling_data(df:pl.DataFrame, selected_columns:list=None):
    if selected_columns != None:
        temp_df = df.select(selected_columns)
    else:
        temp_df = df.select('*')
    return temp_df

# s3_url=pl.format("{}/augmented/images/{}", pl.lit(storage_path), pl.lit(image_name)),
def clean_caption(df:pl.DataFrame):
    cleaned_df = df.with_columns(created_time=pl.lit(datetime.now()))
    return cleaned_df

def read_stopwords():
    # Đọc stopwords từ file
    with open("./data/stopwords.txt", "r", encoding="utf-8") as file:
        custom_stopwords = list(line.strip() for line in file)
    return custom_stopwords

def remove_stopwords(words):
    stopwords = read_stopwords()
    words = [word for word in words if word not in stopwords]
    return words

def perform_imc(image_path, url):
    response = requests.post(url=url, json={"image_url": image_path,})
    print("Response in = ", response.elapsed.total_seconds())
    if response.status_code == 200:
        return response.json().get("response_message")
    else:
        print("Error:", response.status_code, response.text)
        return None
