import sys
sys.path.append('./airflow')

import pandas as pd
import polars as pl
from tqdm import tqdm
from datetime import datetime
from core.config import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.text_functions import scaling_data, clean_caption
from utils.operators.image_augment import image_from_url, upload_image, augment_image


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)


def load_refined_data(params):
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    try:
        for batch in sql_opt.data_generator('raw', latest_time=latest_time, batch_size=1000):
            datarows = list(batch)
            new_data = []
            for data in tqdm(datarows):
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                image_name = f'image_{timestamp}.jpg'
                data['s3_url'] =  f'{settings.AUGMENTED_IMAGE_URL}/image_{timestamp}.jpg'
                # original image
                original_image, is_error = image_from_url(data['original_url'])
                if is_error == False:
                    upload_image(original_image, image_name, params['bucket_name'], params['file_image_path'], settings)
                    ## TODO: augment original image
                    new_data.append(data)
                    augmented_images = augment_image(original_image)
                    # Lưu các ảnh augmented và thêm vào DataFrame
                    for aug_idx, aug_image in enumerate(augmented_images):
                        try:
                            # Thêm vào danh sách dữ liệu mới với caption giống ảnh gốc
                            new_image_name = f'image_{timestamp}_{aug_idx}.jpg'
                            upload_image(aug_image, new_image_name, params['bucket_name'], params['file_image_path'], settings)
                            data['s3_url'] = f'{settings.AUGMENTED_IMAGE_URL}/image_{timestamp}_{aug_idx}.jpg'
                            new_data.append(data)
                        except Exception as e:
                            print(f"Lỗi khi lưu ảnh augmented: {str(e)}")
                            continue
            
            # insert metadata
            df = pl.DataFrame(new_data)
            cleaned_df = clean_caption(df)
            refined_df = scaling_data(cleaned_df, ['original_url', 's3_url', 'caption', 'tokenized_caption', 'created_time'])
            new_data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', new_data)
            print("SUCCESS with", len(new_data))
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        # Write logs
        # sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))
    
def dump_backup():
    pass

if __name__=='__main__':
    params = {
        "bucket_name": "mlflow",
        "file_image_path": "augmented/images"
    }
    
    load_refined_data(params)