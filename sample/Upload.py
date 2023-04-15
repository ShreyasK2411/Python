import os
import sys
import json
from mysql.connector import connect
import pandas as pd

class ConfigurationException(Exception):
    pass

class UploadToSql:
    """
    This class implements utility to insert data 
    present in the directory to mysql database
    """

    def __init__(self,file_path:str,
                 table_name:str,
                 config_path:str):
        self.file_path = file_path
        self.table_name = table_name
        self.config_path = config_path
        self.config = None
        self.df = None
        self.conn = None
    
    def configure(self):
        """
        This function prepares the necessary 
        configurations required for database connection
        """
        # opening the json file and loading the configuration
        with open(self.config_path,encoding='utf-8') as file:
            self.config = json.load(file)

    def read(self):
        """
        This function read the data from the directory and 
        creates a dataframe
        """
        dataframes = [pd.read_csv(self.file_path+"/"+file) for file in os.listdir(self.file_path)]
        self.df = pd.concat(dataframes,ignore_index=True)
        self.df.fillna("",inplace=True)

    def connect_to_mysql(self):
        """
        This function connects to mysql database
        """
        try:
            if self.config is not None:
                self.conn = connect(**self.config)
            else:
                raise ConfigurationException
        except ConfigurationException:
            print("Please configure before connecting!")
        else:
            print("Connection successful!")

    def insert(self):
        """
        This function insert the data in the mysql table
        """
        with self.conn.cursor() as cur:
            # iterate through each record of the dataframe
            for idx,data in self.df.iterrows():
                data = list(data)
                data[0] = idx
                query = f"insert into {self.table_name} values (%s" + ",%s"*(len(data)-1) + ")"
                cur.execute(query,data)
        self.conn.commit()

    def close(self):
        """
        This function closes all the resources
        """
        self.conn.close()
