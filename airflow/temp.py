from utils.operators.trinodb import SQLOperators
from core.config import get_settings
import polars as pl

settings = get_settings()
sql_opt = SQLOperators('imcp', settings)

latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
print(str(latest_time[0]))

for batch in sql_opt.data_generator('raw'):
    df = pl.DataFrame(batch)
    print(df.head())
    break