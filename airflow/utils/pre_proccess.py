import sys
sys.path.append('./airflow')
import polars as pl
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

def read_stopwords():
    # Đọc stopwords từ file
    with open("./data/stopwords.txt", "r", encoding="utf-8") as file:
        custom_stopwords = list(line.strip() for line in file)
    return custom_stopwords

def remove_stopwords(words):
    stopwords = read_stopwords()
    words = [word for word in words if word not in stopwords]
    return words
