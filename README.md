# clickhouse in python
- Prepare codes for DataMarket using clickhouse in python,including:
    - create table
    - insert data
    - sample data
    - merge table
    - check res
    - export data


## py list

### config.py
- loggings config: record logs at specific functions


### s1\_feature\_test.py
- clickhouse login: login_and_choose_database
- create table: create_table
- insert data(cmd)
- sample data: get_random_sample
- merge table: merge_data
- check res: check_res

- other functions:
    - run raw query: raw_query
    - table head overview: get_dataframe_head
    
    
## folder structure and test process
- insert_data: insert 83 raw tables into clickhouse, track by databases
    - PDM
    - PDMDMT
    - BCG
    - CDA
    - CIM
    - DEC
    - ISU
    - OPA
    - OLA
    - IMG
    - BRT
- feature_test: feature testing track folders by date
    - 20191104
    - 20191105
    - 20191106
    - ...
- log: logs of code
- scrip: py code
    
## testing process
- 测试字段名：e.g. 是否员工
- 字段衍生逻辑文字说明
- python+sql代码
- 结果比对


具体参见对应[notebook](https://git.creditx.com/wangxm/clickhouse_python/blob/master/test_sample/02_feature_test.ipynb)

