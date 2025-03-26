import sys
sys.path.append('/opt/airflow')

from utils.setting import get_settings
from utils.operators.mongodb import MongoDBOperator

settings = get_settings()
mongo_opt = MongoDBOperator('imcp', settings.DATABASE_URL)

def clean_null_data(params):
    try:
        affected_rows = mongo_opt.delete_null_from_latest(params["table_name"])
    except Exception as ex:
        raise Exception(f"Error in deleteing - {str(ex)}")
