import sys
sys.path.append('./airflow')

import hashlib
import pandas as pd
import polars as pl
import functools
import itertools
import logging
import time
import random
import concurrent.futures as cf
from tqdm import tqdm
from datetime import datetime
from utils.setting import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.text import TextOperator
from utils.operators.image import ImageOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)


def process_row(data, catalog, settings):
    new_data = []
    md5_hash = hashlib.md5(data["original_url"].encode()).hexdigest()
    partition = datetime.now().strftime("%Y-%m-%d")
    image_name = f'{md5_hash}.jpg'
    data['s3_url'] =  f"{catalog['s3_path']}/{partition}/{image_name}"

    original_image, is_error = ImageOperator.image_from_url(data['original_url'])
    if is_error == False:
        ## TODO: Upload original image
        ImageOperator.upload_image(original_image, image_name, catalog['s3_bucket'], f'{catalog["s3_object_path"]}/{partition}', settings)
        new_data.append(data)
        augmented_images = ImageOperator.augment_image(original_image)

        for aug_idx, aug_image in enumerate(augmented_images):
            temp = data.copy()
            try:
                new_image_name = f'{md5_hash}_{aug_idx}.jpg'
                ## TODO: Upload augmented image
                ImageOperator.upload_image(aug_image, new_image_name, catalog['s3_bucket'], f'{catalog["s3_object_path"]}/{partition}', settings)
                temp['s3_url'] = f"{catalog['s3_path']}/{partition}/{new_image_name}"
                new_data.append(temp)
                
            except Exception as e:
                print(f"Error when uploading augmented image: {str(e)}")
                continue
    return new_data


def load_refined_data(params):
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    catalog = sql_opt.execute_query(query=f"""
        SELECT * FROM imcp.layer_catalogs
        WHERE layer_name = '{params["layer_name"]}'
            AND storage_type = '{params["storage_type"]}'
            AND s3_bucket = '{params["bucket_name"]}'
    """)[0]
    print(catalog)
    try:
        for batch_idx, batch in enumerate(sql_opt.data_generator('raw', latest_time=latest_time, batch_size=5)):
            datarows = list(batch)
            # args = [(data, params, settings) for data in tqdm(datarows)]
            args = [(data, catalog, settings) for data in tqdm(datarows)]
            with cf.ProcessPoolExecutor(max_workers=2) as executor:
                func = functools.partial(process_row)
                new_data = list(executor.map(func, *zip(*args)))
                
            # insert metadata
            new_data = list(itertools.chain(*new_data))
            df = pl.DataFrame(new_data)
            refined_df = TextOperator.clean_caption(df)
            refined_df = TextOperator.scaling_data(refined_df, ['original_url', 's3_url', 'short_caption', 'tokenized_caption', 'created_time'])
            new_data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', new_data)
            logging.info(f"SUCCESS WITH {len(new_data)} ROWS IN BATCH {batch_idx}")
            affected_rows += len(new_data)
            time.sleep(random.uniform(0.001, 0.005))
            break
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))


if __name__=='__main__':
    params = {
        "bucket_name": "lakehouse",
        "storage_type": "minio",
        "layer_name": "refined"
    }
    
    load_refined_data(params)