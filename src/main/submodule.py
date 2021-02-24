import logging
import sys, os
import time
import urllib.request, json 
import sqlite3
from datetime import date


class extractAndInsert():
    def __init__(self,url,db_loc,table):
        self.url = url
        self.db_loc = db_loc
        self.table = table
        
    def get_data(self,county):
        with urllib.request.urlopen(self.url) as url:
            data = json.loads(url.read().decode())
        self.table1=f"{self.table}_{county}"
        logging.info(self.table1)
        self.establish_sqlite_conn()
        self.check_table_exists(data)
        self.preparedata_and_insert(data["data"],county)

    def establish_sqlite_conn(self):
        self.db = sqlite3.connect(':memory:')
        self.db = sqlite3.connect(self.db_loc)
        dbname=self.db_loc.split("/")[-1]
        logging.info(f"connect to  {dbname} database")
        self.cursor = self.db.cursor()

    def check_table_exists(self,data):
        
        query=f"""select count(*) from sqlite_master where type='table' and name='{self.table1}'"""
        logging.info(query)
        row=self.cursor.execute(query)
        row=self.cursor.fetchone() 
        logging.info(row[0])
        if row[0]!=1 :
            logging.info(f"{self.table1} table not exists")
            self.create_table(data)

    def create_table(self,data):
        columns=[]
        dataType=[]
        for y in data["meta"]["view"]["columns"]:
            if y["fieldName"] in ["test_date","new_positives","cumulative_number_of_positives","total_number_of_tests","cumulative_number_of_tests"]:
                columns.append(y["fieldName"].replace(":",""))
                dataType.append(y["dataTypeName"])
        table_state=f"CREATE TABLE {self.table1}("
        for x in list(zip(columns,dataType)):
            if x[1].lower() == "number":
                table_state+=x[0] + " " + " INTEGER,"
            else:
                table_state+=x[0] + " " + " TEXT,"
        table_state += "load_dt TEXT)"
        #table_state=table_state[:-1] + ")"
        self.cursor.execute(table_state)
        self.db.commit()
        logging.info(f"{self.table1} table created")


    def preparedata_and_insert(self, data, county):
        cnt=0
        for record in data:
            if record[9].lower() == county:
                test_date=record[8]
                new_positives=int(record[10])
                cumulative_number_of_positives=int(record[11])
                total_number_of_tests=int(record[12])
                cumulative_number_of_tests=int(record[13])
                load_dt = str(date.today()) 
                self.cursor.execute(f"""INSERT INTO {self.table1}(test_date,new_positives,cumulative_number_of_positives,total_number_of_tests ,cumulative_number_of_tests,load_dt) VALUES(?,?,?,?,?,?)""", (test_date,new_positives,cumulative_number_of_positives,total_number_of_tests ,cumulative_number_of_tests,load_dt))
                cnt+=1
        logging.info(f"number of rows fetched and inserted for {county} county is {cnt}")
        
