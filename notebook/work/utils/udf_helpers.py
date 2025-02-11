from pyvi import ViTokenizer
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, StringType

@pandas_udf(ArrayType(StringType()))
def tokenize_vietnamese(text_series: pd.Series) -> pd.Series:
    return text_series.apply(lambda text: ViTokenizer.tokenize(text).split())

def read_stopwords():
    # Đọc stopwords từ file
    with open("./data/stopwords.txt", "r", encoding="utf-8") as file:
        custom_stopwords = list(line.strip() for line in file)
    return custom_stopwords

def remove_stopwords(words):
    stopwords = read_stopwords()
    words = [word for word in words if word not in stopwords]
    return words