import re
import polars as pl


def clean_text(text:str) -> str:
    if text is None:
        return ""
    return re.sub(r'[^a-zA-Z\s]', '', text)

def tokenize(text:str) -> str:
    if text is None:
        return [""]
    return text.split(" ")

def scaling_data(df:pl.DataFrame, selected_columns:list=None):
    if selected_columns != None:
        temp_df = df.select(selected_columns)
    else:
        temp_df = df.select('*')
    return temp_df