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
            columns: columns for you new table including column's type;
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
            current_table: origin whole table
            new_table: sampled table from origin
            num_cases: number of cases you want to sample
            ordered_column: new table's ordered column
            selected_columns: columns you want to keep when sampling
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
        '''
        a quick overview of table in dataframe
        '''
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
            
            
            
    def merge_data(self, new_table, ordered_column, left_table_query,\
                       right_table_query, on_query, joined_method, engine = 'MergeTree'):
        '''
        merge data: left table as a, right table as b;
            new_table: give name of the new table
            ordered_column: new table's ordered column
            left_table_query: define and rename columns of left table
            right_table_query: define and rename columns of right table
            on_query: define key_columns to join on
            joined_method: how to join the two selected tables: e.g. inner, left, right
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
        
        
    def check_res(self, table_kx, table_jh, columns_kx, columns_jh, columns_comp, header):
        '''
        check res of final tables and compare the res with the answer given by jiaohang;
            table_kx: res table finished by craiditx
            table_jh: res table given by jiaohang
            columns_kx: columns selected from table: e.g. key columns + answers
            columns_jh: columns selected from table: e.g. key columns + answers
            columns_comp: the column you want to check
            header: header of the merged dataframe
        '''
        sql_kx = ["SELECT " + columns_kx + " FROM " + table_kx]
        sql_jh = ["SELECT " + columns_jh + " FROM " + table_jh]
        print ('query overview: \n{0},\n{1}'.format(sql_kx, sql_jh))
        query_kx = ''.join(sql_kx)
        query_jh = ''.join(sql_jh)
        res_kx = self.client.execute(query_kx)
        res_jh = self.client.execute(query_jh)
        res_kx_df = pd.DataFrame(res_kx, columns = header)
        res_jh_df = pd.DataFrame(res_jh, columns = header)
        res_comp = pd.merge(res_kx_df, res_jh_df, on = header[:-1], suffixes = ('_kx', '_jh'), how = 'inner')
        res_comp_unequal = res_comp[res_comp[columns_comp + '_kx'] != res_comp[columns_comp + '_jh']]
        print ('\ncheck merged shape: res_kx_df: {0}, res_jh_df: {1}, res_comp: {2}\n'\
               .format(res_kx_df.shape[0], res_jh_df.shape[0], res_comp.shape[0]))
        print ('There are {} unequal cases. (check detail if >0)\n'.format(res_comp_unequal.shape[0]))
        print ('A glance of merged_data:')
        display (res_comp.head(5))
        return res_kx_df, res_jh_df

        
        
        
        
        
        