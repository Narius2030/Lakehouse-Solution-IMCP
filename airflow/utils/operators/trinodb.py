import sys
sys.path.append('./')

from utils.operators.support_query import QueryTemplate
from core.config import Settings
from contextlib import closing
from datetime import datetime
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
    
    def get_latest_fetching_time(self, layer, table_name):
        query = f"""
            SELECT MAX(end_time) 
            FROM audit
            WHERE status='SUCCESS' AND layer='{layer}' AND table_name='{table_name}'
        """
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                cursor.execute(query)
                latest_time = cursor.fetchone()
                return latest_time[0]
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
    
    def data_generator(self, table_name, columns=None, latest_time="1970-01-01T00:00:00.000+00:00", batch_size=10000):
        query = QueryTemplate(table_name).create_query_select(columns, latest_time)
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
    
    def upsert_dataframe_table(self, table_name: str, data: list, columns: list, conflict_column: tuple = None, arrjson: list = [], chunk_size=10000):
        query = QueryTemplate(table_name).create_query_upsert(columns, conflict_column, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    cursor.execute(query, partitioned_data)
                    print(f"Merged or updated {len(partitioned_data)} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def insert_dataframe_table_nonconflict(self, table_name: str, data: list, columns: list, conflict_column: tuple = None, arrjson: list = [], chunk_size: int = 10000):
        query = QueryTemplate(table_name).create_query_insert_nonconflict(columns, conflict_column, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    cursor.execute(query, partitioned_data)
                    print(f"Inserted {len(partitioned_data)} records")
                    time.sleep(1)
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def insert_dataframe_table(self, table_name: str, data: list, columns: list, arrjson: list = [], chunk_size: int = 10000):
        query = QueryTemplate(table_name).create_query_insert(columns, arrjson)
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    formatted_data = [tuple(col for col in row.values()) for row in partitioned_data]
                    cursor.executemany(query, formatted_data)
                    print(f"Inserted {len(formatted_data)} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def write_log(self, collection, status, layer, start_time=datetime.now(), end_time=datetime.now(), error_message="", affected_rows=0, action=""):
        """write log data about each action interacting with database

        Args:
            collection (_type_): name of collection
            status (_type_): status of action
            start_time (datetime, optional): the time when start. Defaults to datetime.now().
            end_time (datetime, optional): the time when end. Defaults to datetime.now().
            error_message (str, optional): error message got caught in action if have. Defaults to "".
            affected_rows (int, optional): the affected rows when execute the action if have. Defaults to 0.
            action (str, optional): the name of action. Defaults to "".
        """
        try:
            with closing(self.__dbconn.cursor()) as cursor:
                log = [{
                    "layer": layer,
                    "table_name": collection,
                    "start_time": start_time,
                    "end_time": end_time,
                    "status": status,
                    "error_message": error_message,
                    "affected_rows": affected_rows,
                    "action": action
                }]
                self.insert_dataframe_table('audit', log, list(log[0].keys()))
                print("Writed log!")
        except Exception as ex:
            raise Exception(f"====> Can't write log - {str(ex)}")
    
    def close(self):
        self.__dbconn.close()
