import sys
sys.path.append("./notebook/work")

import csv
import logging
from operators.database_operators import SQLOperators
from utils.configuration import get_settings

settings = get_settings()
dbopt = SQLOperators("sakila", settings)

def read_csv(filename:str):
    try:
        with open(f"../data/{filename}", encoding="UTF-8") as file:
            reader = csv.DictReader(file)
            data = list(reader)
            return data, reader.fieldnames
    except FileNotFoundError:
        logging.error(f"Error: File '{filename}' not found.")
        return []
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        return []
    
def read_customer():
    dataset, columns = read_csv("customer.csv")
    dbopt = SQLOperators("sakila", settings)
    try:
        dbopt.insert_dataframe_table_nonconflict("customer", "public", dataset, columns, chunk_size=100)
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        
def read_rental():
    dataset, columns = read_csv("rental.csv")
    dbopt = SQLOperators("sakila", settings)
    try:
        dbopt.insert_dataframe_table_nonconflict("rental", "public", dataset, columns, chunk_size=100)
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        
def read_payment():
    dataset, columns = read_csv("payment.csv")
    try:
        dbopt.insert_dataframe_table_nonconflict("customer", "public", dataset, columns, chunk_size=100)
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        
def read_address():
    query = """
        SELECT * FROM address ad 
        JOIN city c ON c.city_id=ad.city_id
        JOIN country co ON co.country_id = c.country_id
    """
    dataset, columns = dbopt.execute_query(query)
    try:
        dbopt.insert_dataframe_table_nonconflict("customer", "public", dataset, columns, chunk_size=100)
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")