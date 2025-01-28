import pymongo
from tqdm import tqdm
from datetime import datetime



class MongoDBOperator():
    def __init__(self, dbname:str, connection_string:str) -> None:
        self.__connstr = connection_string
        self.dbname = dbname
    
    def is_has_data(self, collection) -> bool:
        """check the data remaining in collection

        Args:
            collection (str): name of collection

        Returns:
            bool: confirm the existing of data
        """ 
        check = False
        with pymongo.MongoClient(self.__connstr) as client:
            dbconn = client[self.dbname]
            docs = dbconn[collection].find()
            if any(docs):
                check = True
        return check
    
    def find_data_with_aggregate(self, collection:str, aggregate=None) -> list:
        """Query data by aggregation terms

        Args:
            collection (str): name of collection
            aggregate (list, optional): the aggregation terms. Defaults to empty list.

        Returns:
            list: list of row query from colllection
        """
        data = None
        with pymongo.MongoClient(self.__connstr) as client:
            db = client[self.dbname]
            documents = db[collection].aggregate(aggregate)
            data = list(documents)
            return data
    
    def find_latest_time(self, comp_layer:str, source_table:str) -> datetime:
        """Query the latest integration time of specific layer

        Args:
            comp_layer (str): name of specific layer

        Returns:
            datetime: the latest time of integration in specific layer
        """
        aggregate = [{
            '$match': {
                'status': 'SUCCESS',
                'layer': comp_layer,
                'table_name': source_table,
            }}, {
            '$sort': {
                'end_time': -1
            }}, {
                '$limit': 1
        }]
        data = self.find_data_with_aggregate('audit', aggregate)
        latest_time = data[0]['end_time']
        return latest_time
    
    def build_query(self, params:dict):
        query = {
            "url": params['url'],
            "$and": [
                {"caption": params['caption']},
                {"short_caption": params['short_caption']}
            ]
        }
        return query
    
    def data_generator(self, collection:str, batch_size:int=10000, limit:int=100000):
        """Generate any data by batch

        Args:
            collection (str): name of collection
            batch_size (int, optional): the batchsize to chunk data. Defaults to 10000.
            limit (int, optional): limitation of queried rows. Defaults to 100000.

        Yields:
            list: batch of data
        """
        with pymongo.MongoClient(self.__connstr) as client:
            db = client[self.dbname]
            documents = db[collection].find({}, {}).batch_size(batch_size).limit(limit)
            batch = []
            for doc in documents:
                batch.append(doc)
                if len(batch) == batch_size:
                    yield batch  # Trả về nhóm tài liệu (batch)
                    batch = []  # Reset batch sau khi yield
            # Nếu còn tài liệu dư ra sau khi lặp xong
            if batch:
                yield batch
    
    def checking_data_generator(self, db, datasets, batchsize):
        """Generate unique data by batch

        Args:
            collection (str): name of collection
            batch_size (int, optional): the batchsize to chunk data. Defaults to 10000.
            limit (int, optional): limitation of queried rows. Defaults to 100000.

        Yields:
            list: batch of data
        """
        accepted_datas = []
        count = 0
        for data in tqdm(datasets):
            params = {
                'url': data['url'],
                'caption': data['caption'],
                'short_caption': data['short_caption']
            }
            query = self.build_query(params)
            docs = db.find(query, {"url":1,"caption":1,"short_caption":1})
            if any(docs) == False:
                accepted_datas.append(data)
            count += 1
            if count == batchsize:
                count = 0
                yield accepted_datas
        
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
        with pymongo.MongoClient(self.__connstr) as client:
            dbconn = client[self.dbname]
            log = {
                "layer": layer,
                "table_name": collection,
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "error_message": error_message,
                "affected_rows": affected_rows,
                "action": action
            }
            dbconn['audit'].insert_one(log)
            print("Writed log!")
    
    def insert_batches(self, collection, datasets, batch_size=10000) -> int:
        """Insert all data by each batch

        Args:
            collection (str): the name of collection
            datasets (_type_): list of data rows
            batch_size (int, optional): the bacth size to chunk data. Defaults to 10000.

        Raises:
            Exception: Errors when insert data

        Returns:
            int: inserted rows
        """
        affected_rows = 0
        try:
            with pymongo.MongoClient(self.__connstr) as client:
                dbconn = client[self.dbname]
                for i in range(0, len(datasets), batch_size):
                    batch = datasets[i:i + batch_size]
                    resp = dbconn[collection].insert_many(batch)
                    affected_rows += len(resp.inserted_ids)
        except Exception as ex:
            raise Exception(str(ex))
        return affected_rows
    
    def insert_batches_not_duplication(self, collection, datasets, batchsize=10000):
        """Insert non-duplicated data by each batch

        Args:
            collection (_type_): _description_
            datasets (_type_): _description_
            batchsize (int, optional): _description_. Defaults to 10000.
        """
        with pymongo.MongoClient(self.__connstr) as client:
            dbconn = client[self.dbname]
            for batch in self.checking_data_generator(dbconn[collection], datasets, batchsize):
                if batch != []:
                    print("Loading...", len(batch))
                    dbconn[collection].insert_many(batch)