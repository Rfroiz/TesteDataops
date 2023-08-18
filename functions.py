import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

class DataframeToMongo():
    def __init__(self) -> None:
        load_dotenv()
        self.clientDB =  MongoClient(os.getenv("DB_URL"))
        self.db = self.clientDB[os.getenv("DBNAME")]

    
    def insert_dataframes(self,collection,df):
        self.db[collection].insert_many(df.to_dict(orient='records'))
        
    def agreg_pipe(self,pipeline,collection):
        result = list(self.db[collection].aggregate(pipeline))
        return result
        





