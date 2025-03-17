import sys
sys.path.append('/opt/airflow')

import hashlib
import pandas as pd
import polars as pl
import itertools
import logging
import concurrent.futures as cf
import google.generativeai as genai
from tqdm import tqdm
from utils.setting import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.text import TextOperator
from utils.operators.image import ImageOperator


settings = get_settings()
genai.configure(api_key=settings.GEMINI_API_KEY)
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)


def process_row(data, params, settings, genai):
    new_data = []
    md5_hash = hashlib.md5(data["original_url"].encode()).hexdigest()
    image_name = f'{md5_hash}.jpg'
    data['s3_url'] =  f'{settings.AUGMENTED_IMAGE_URL}/{image_name}'

    original_image, is_error = ImageOperator.image_from_url(data['original_url'])
    if is_error == False:
        ## TODO: Upload original image
        ImageOperator.upload_image(original_image, image_name, params['bucket_name'], params['file_image_path'], settings)
        new_data.append(data)
        augmented_images = ImageOperator.augment_image(original_image)

        for aug_idx, aug_image in enumerate(augmented_images):
            temp = data.copy()
            try:
                new_image_name = f'{md5_hash}_{aug_idx}.jpg'
                ## TODO: Upload augmented image
                ImageOperator.upload_image(aug_image, new_image_name, params['bucket_name'], params['file_image_path'], settings)
                temp['s3_url'] = f'{settings.AUGMENTED_IMAGE_URL}/{new_image_name}'
                ## TODO: Generate caption
                short_caption = TextOperator.caption_generator(genai, data['s3_url'], settings.GEMINI_PROMPT)
                temp['short_caption'] = short_caption
                new_data.append(temp)
            except Exception as e:
                print(f"Error when uploading augmented image: {str(e)}")
                continue
    return new_data


def load_refined_data(params):
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    try:
        for batch_idx, batch in enumerate(sql_opt.data_generator('raw', latest_time=latest_time, batch_size=10)):
            datarows = list(batch)
            args = [(data, params, settings, genai) for data in tqdm(datarows)]
            with cf.ThreadPoolExecutor(max_workers=3) as executor:
                new_data = list(executor.map(lambda p: process_row(*p), args))
                
            # insert metadata
            new_data = list(itertools.chain(*new_data))
            df = pl.DataFrame(new_data)
            refined_df = TextOperator.clean_caption(df)
            refined_df = TextOperator.scaling_data(refined_df, ['original_url', 's3_url', 'short_caption', 'tokenized_caption', 'created_time'])
            new_data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', new_data)
            logging.info(f"SUCCESS WITH {len(new_data)} ROWS IN BATCH {batch_idx}")
            affected_rows += len(new_data)
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
        "file_image_path": "imcp/augmented-data/images"
    }
    
    load_refined_data(params)