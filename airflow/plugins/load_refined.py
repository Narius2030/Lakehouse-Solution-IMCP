import sys
sys.path.append('./airflow')

import pandas as pd
import polars as pl
from core.config import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.storage import MinioStorageOperator
from utils.pre_proccess import tokenize, scaling_data, clean_text


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    try:
        for batch in sql_opt.data_generator('raw'):
            data = list(batch)
            df = pl.DataFrame(data)
            df = df.filter(pl.col('created_time') >= latest_time)
            lowered_df = df.with_columns(
                *[pl.col(col).str.to_lowercase().alias(col) for col in ['caption']]
            )
            cleaned_df = lowered_df.with_columns(
                *[pl.col(col).map_elements(lambda x: clean_text(x), return_dtype=pl.String).alias(col) for col in ['caption']],
                pl.format("{}/augmented/images/{}", pl.lit(settings.MINIO_URL), pl.col("url").str.extract(r".*/(.*)").str.slice(-16, None)).alias("s3_url")
            )
            tokenized_df = cleaned_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: tokenize(x), return_dtype=pl.List(pl.String)).alias(f'{col}_tokens') for col in ['caption']]
            )
            refined_df = scaling_data(tokenized_df, ['url', 's3_url', 'caption', 'caption_tokens'])
            # insert data batch
            data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', data)
            affected_rows += len(data)
            print('SUCCESS with', len(data))
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
    except Exception as exc:
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))
            

if __name__=='__main__':
    load_refined_data()