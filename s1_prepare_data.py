#!/usr/bin/python3
#-*-coding:utf-8-*-
import config
import pandas as pd
import getpass
from clickhouse_driver import Client
import logging


class Prepare_Data():
    
    def __init__(self, host = 'localhost', port = '9000', user = 'default'):
        self.host = host
        self.port = port
        self.user = user
        
        
    def login_and_choose_database(self, database):
        '''
        login clickhouse in notebooks. Using 'database' to choose target database you want.
        '''
        self.client = Client(host = self.host, port = self.port, database = database, user = self.user)
        logging.info('login database: %s', database)


    def raw_query(self, query): 
        '''
        for adhoc query if needed
        '''
        logging.info('run query: %s', query)
        ans = self.client.execute(query)
        return ans


    def create_table(self, table_name, columns, engine, ordered_column):
        '''
        create table:
        table_name: your new table name;
        columns: columns for you new table;
        engine: e.g. MergeTree;
        ordered_column: needed for query 'sort by'
        '''
        logging.info('create table: %s', table_name)
        sql = [
        "CREATE TABLE IF NOT EXISTS " + table_name + '\n'
        " (" + columns + ")" + '\n'
        " ENGINE = " + engine +'\n'
        " ORDER BY " + ordered_column + '\n' 
        ]
        query = ''.join(sql)
        print ('query overview: \n{}'.format(query))
        self.client.execute(query)
        print ('show tables: \n{}'.format(self.client.execute('show tables;')))
        
    
    def get_random_sample(self, current_table, new_table, num_cases, ordered_column, selected_columns, engine = 'MergeTree'):
        '''
        random sample from current dataset    
        '''
        logging.info('create sample table: %s', new_table)
        sql = [
        "CREATE TABLE IF NOT EXISTS " + new_table + '\n'
        " ENGINE = " + engine +'\n'
        " ORDER BY " + ordered_column + '\n' 
        " AS SELECT " + selected_columns + '\n'
        " FROM " + current_table + '\n'
        " ORDER BY rand() \n"
        " LIMIT " + num_cases + '\n'
        ]
        query = ''.join(sql)
        print ('query overview: \n{}'.format(query))
        self.client.execute(query)
        print ('show tables: \n{}'.format(self.client.execute('show tables;')))
        
        
    def get_dataframe_head(self, table_name):
        sql = [
        "SELECT * FROM " + table_name + '\n'
        " LIMIT 5"]
        query = ''.join(sql)
        res = self.client.execute(query)
        col_name = []
        col_desc = self.client.execute('desc {}'.format(table_name))
        for i in range(len(col_desc)):
            col_name.append(col_desc[i][0])
        res_dataframe = pd.DataFrame(res, columns = col_name)
        display (res_dataframe)
            
            
            
    def merge_data(self, new_table, ordered_column, joined_method, left_table_query,\
                       right_table_query, on_query, engine = 'MergeTree'):
        '''
        merge data: left table as a, right table as b;
            left_table_query: define columns and renaming left table;
            right_table_query: define columns and renaming right table;
            on_query: define key_columns to join;
        '''
        logging.info('merge data: %s', new_table)
        sql = [
        "CREATE TABLE IF NOT EXISTS " + new_table + '\n'
        "ENGINE = " + engine +'\n'
        "ORDER BY " + ordered_column + '\n' 
        "AS SELECT * FROM ( \n" + left_table_query + ")a \n" + joined_method + "( \n" + right_table_query + ")b \n"
        "ON " + on_query + '\n'
        ]
        query = ''.join(sql)
        print ('query overview: \n{}'.format(query))
        self.client.execute(query) 
        print ('show tables: \n{}'.format(self.client.execute('show tables;')))

        
        
        
        
        
        