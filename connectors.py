from pyspark.sql import DataFrame
from querie import Metrics_Queries
import logging 

class BaseConnector:

    def read_parquet(self):
        pass

    def read_csv(self):
        pass

    def send(self):
        pass

    def execute(self):
        pass

class SparkConnector:

    def __init__(self, spark_session,source=None):
        self.spark_session  = spark_session
        self.source         = source
        self.temp_table     = None
        self.dataframe      = None
        self.metric         = {}

    def read_parquet(self, source_path=None):
        # Defining the source_path
        if source_path is None and self.source is not None:
            self.source_path = self.source
        else:
            raise ValueError("Source is not defined.")
        
        # Checking if there is already a dataframe defined
        if self.dataframe is None:
            self.dataframe = self.spark_session.read.parquet(self.source)            
            # Naming the temp table
            self.temp_table = source_path.split('/')[-1]
            self.__create_temp_table(dataframe=self.dataframe,temp_table_name=self.temp_table)
        else: 
            raise ValueError("DataFrame was already defined")
    
    # Create temporary table to execute count o metrics
    def __create_temp_table(self, dataframe: DataFrame,temp_table_name):
        self.dataframe.CreateTempView(temp_table_name)

    def add_metric(self, metric:str,alias:str, column=None):
        """The function that adds the metric to the metric list

        Args:
            metric (str): the metric to be calculated
            alias (str): alias of the metric that will show on the json
            column (str, optional): [description]. name of the column
        """        
        
        # Separating in case is not already a dict
        if isinstance(str,metric) and column is not None:
            if metric == 'count_null':
                querie = Metrics_Queries.count_null.format(column,alias)

            self.metric[title] = querie
            logging.info(querie)

    def execute_query(self,queries:dict,dataframe:DataFrame=None):
        """Executes the counts for the metrics

        Args:
            queries (list): dict with alias and query to be calculated
            dataframe (DataFrame): dataframe to be tested
        """ 
        # Checking if by any chance the dataframe was not defined    
        if dataframe is None:
            if self.dataframe is None:
                raise ValueError("DataFrame not defined")
            dataframe = self.dataframe

        # Separating queries that have to run separetaly from case queries and gain speed
        case_count_queries = ',\n'.join([querie for title,querie self.metric.items() if 'CASE' in querie])
        not_case_queries   = {title: querie  for title,querie self.metric.items() if 'CASE' not in querie}

        if case_count_queries:
            self.results_case = self.spark.sql(case_count_queries).collect()

        if not_case_count_queries:
            self.result_other_queries = [self.spark.sql(not_case_queries)].collect()]


    def format_json(self,result):
        pass

