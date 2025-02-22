import sys
sys.path.append('./airflow')
import polars as pl
import requests
from datetime import datetime
from pyvi import ViTokenizer

class TextOperator():
    @staticmethod
    def tokenize_vietnamese(text: str):
        tokens = ViTokenizer.tokenize(text).split(" ")
        return tokens

    @staticmethod
    def scaling_data(df:pl.DataFrame, selected_columns:list=None):
        if selected_columns != None:
            temp_df = df.select(selected_columns)
        else:
            temp_df = df.select('*')
        return temp_df

    @staticmethod
    def clean_caption(df:pl.DataFrame):
        cleaned_df = df.with_columns(
            created_time=pl.lit(datetime.now()),
            tokenized_caption=pl.col("caption").map_elements(lambda x: TextOperator.tokenize_vietnamese(x), return_dtype=pl.String())
        )
        return cleaned_df

    @staticmethod
    def perform_imc(image_path, url):
        response = requests.post(url=url, json={"image_url": image_path,})
        print("Response in = ", response.elapsed.total_seconds())
        if response.status_code == 200:
            return response.json().get("response_message")
        else:
            print("Error:", response.status_code, response.text)
            return None
