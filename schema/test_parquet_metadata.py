import pytest
import os
import pandas 
import pyarrow
import gcsfs
from pyarrow import parquet
from get_remote_parquet import ParquetMetadata

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

MOCK_DATA_SCHEMA_FIRST = pyarrow.schema([
                        pyarrow.field('user_name',pyarrow.string()),
                        pyarrow.field('password',pyarrow.string()),
                        pyarrow.field('birthday',pyarrow.date32()),
                        pyarrow.field('level',pyarrow.int32()),
                        pyarrow.field('signup_timestamp',pyarrow.timestamp('s'))])

MOCK_DATA_SCHEMA_SECOND = pyarrow.schema([
                        pyarrow.field('user_name',pyarrow.string()),
                        pyarrow.field('password',pyarrow.string()),
                        pyarrow.field('birthday',pyarrow.date32()),
                        pyarrow.field('level',pyarrow.int32()),
                        pyarrow.field('type',pyarrow.string()),
                        pyarrow.field('signup_timestamp',pyarrow.timestamp('s'))])

MOCK_DATA_FIRST = [['user_name','password','birthday','level','signup_timestamp'],['docker','docker222','10-12-1990',32,'20-12-2021 00:00:00'],['amundsen','trail22','10-12-1997',54,'20-12-2021 07:00:00']]

MOCK_DATA_SECOND = [['user_name','password','birthday','level','type','signup_timestamp'],['docker','docker222','10-12-1990',23,'buble','20-12-2021 00:00:00'],['amundsen','trail22','10-12-1997',52,'water','20-12-2021 07:00:00']]


class TestParquetSchema():
    
    def __init__(self):
        pass
    
    def mock_parquet_df(self):
        df = pandas.DataFrame(MOCK_DATA_FIRST[1:],columns=MOCK_DATA_FIRST[0])
        df['signup_timestamp'] = pandas.to_datetime(df['signup_timestamp'])
        df['birthday'] = pandas.to_datetime(df['birthday'])
        pyarrow_table = pyarrow.Table.from_pandas(df,schema=MOCK_DATA_SCHEMA_FIRST)
        parquet.write_table(pyarrow_table,DIR_PATH+'/mock_data_1.parquet')

        df = pandas.DataFrame(MOCK_DATA_SECOND[1:],columns=MOCK_DATA_SECOND[0])
        df['signup_timestamp'] = pandas.to_datetime(df['signup_timestamp'])
        df['birthday'] = pandas.to_datetime(df['birthday'])
        pyarrow_table = pyarrow.Table.from_pandas(df,schema=MOCK_DATA_SCHEMA_SECOND)
        parquet.write_table(pyarrow_table,DIR_PATH+'/mock_data_2.parquet')


    def test_read_single_file(self):
        parquet_obj = ParquetMetadata(file_path=DIR_PATH)
        metadata = parquet_obj.get_metadata()
        
        assert metadata == [{"timestamp": "2021-01-01 12:00:00","schema":{'user_name':"string",'password':"string",'birthday':"date",'level':"integer",'signup_timestamp':"timestamp"},"size":"1000","partition":1}]

    def test_read_multiple_file(self):
        parquet_obj = ParquetMetadata(file_path=DIR_PATH+'/mock_data*')
        metadata = parquet_obj.get_metadata()
        
        assert metadata == [{"timestamp": "2021-01-01 12:00:00","schema":{'user_name':"string",'password':"string",'birthday':"date",'level':"integer",'signup_timestamp':"timestamp"},"size":"1000","partition":1},
                            {"timestamp": "2021-01-04 12:00:00","schema":{'user_name':"string",'password':"string",'birthday':"date",'level':"integer","type":"string",'signup_timestamp':"timestamp"},"size":"1040","partition":2}]


if __name__ == '__main__':
    TestParquetSchema().mock_parquet_df()