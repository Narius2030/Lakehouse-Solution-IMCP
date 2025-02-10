import sys
sys.path.append('./')

from utils.operators.support_query import QueryTemplate
from core.config import Settings
from contextlib import closing
import trino
import time
import polars as pl


class SQLOperators:
    def __init__(self, conn_id: str, settings: Settings):
        try:
            self.settings = settings
            self.__dbconn = trino.dbapi.connect(
                host=self.settings.TRINO_HOST,
                port=self.settings.TRINO_PORT,
                user=self.settings.TRINO_USER,
                catalog=self.settings.TRINO_CATALOG,
                schema=conn_id
            )
        except Exception as ex:
            raise Exception(f"====> Can't connect to '{conn_id}' database with host: {self.settings.TRINO_HOST} - {str(ex)}")
    
    def execute_query(self, query):
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
                return [dict(zip([col[0] for col in cursor.description], row)) for row in data]
        except Exception as ex:
            raise Exception(f"====> Can't execute query: {query} - {str(ex)}")
    
    def get_latest_fetching_time(self, table_name, schema, finished_time, dag_name):
        audit = self.get_dataframe_from_database(query=f'''SELECT * FROM {schema}.{table_name}''')
        latest_time = audit.filter(
            pl.col('status').str.to_uppercase() == 'SUCCESS',
            pl.col('dag') == dag_name
        ).select(
            pl.col(finished_time).max()
        ).to_dicts()[0][finished_time]
        return latest_time
    
    def data_generator(self, table_name, columns, batch_size=10000):
        query = QueryTemplate(table_name).create_query_select(columns)
        try:
            batch = []
            with closing(self.__dbconn.cursor()) as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
                dataset = [dict(zip([col[0] for col in cursor.description], row)) for row in data]
                for doc in dataset:
                    batch.append(doc)
                    if len(batch) == batch_size:
                        yield batch  # Trả về nhóm tài liệu (batch)
                        batch = []  # Reset batch sau khi yield
                # Nếu còn tài liệu dư ra sau khi lặp xong
                if batch:
                    yield batch
                
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
    
    def upsert_dataframe_table(self, table_name: str, schema: str, data: list, columns: list, conflict_column: tuple = None, arrjson: list = [], chunk_size=10000):
        query = QueryTemplate(table_name, schema).create_query_upsert(columns, conflict_column, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    cursor.execute(query, partitioned_data)
                    print(f"Merged or updated {len(partitioned_data)} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def insert_dataframe_table_nonconflict(self, table_name: str, schema: str, data: list, columns: list, conflict_column: tuple = None, arrjson: list = [], chunk_size: int = 10000):
        query = QueryTemplate(table_name, schema).create_query_insert_nonconflict(columns, conflict_column, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    cursor.execute(query, partitioned_data)
                    print(f"Inserted {len(partitioned_data)} records")
                    time.sleep(1)
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def insert_dataframe_table(self, table_name: str, schema: str, data: list, columns: list, arrjson: list = [], chunk_size: int = 10000):
        query = QueryTemplate(table_name, schema).create_query_insert(columns, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    cursor.execute(query, partitioned_data)
                    print(f"Inserted {len(partitioned_data)} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
    
    def close(self):
        self.__dbconn.close()
