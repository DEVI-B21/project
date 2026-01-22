# Packages
from psycopg2 import sql
import pandas as pd
import json

from pydantic import ValidationError
from fastapi.exceptions import RequestValidationError




class CategoricalRelationalMapping:
    def __init__(self) -> None:
        self.schema_name = "data_exploration"
        self.table_name = "categorical_relation_mapping"
        self.returning_key = "id"
        self.base_executor = "Execute"

    def fetchByDatasetId(self,):
        query = sql.SQL(
            f""" select 
            id as category_relation_id, 
            original_key_column_id, 
            original_value_column_id, 
            is_active as relation_mapping_active_status, 
            created_by, 
            modified_by, 
            created_on, 
            modified_on from {self.schema_name}.{self.table_name} where dataset_id=%s """
        )
        dataframe , message = self.base_executor.executeSelect(query=query,)
        response_data = (dataframe)
        return 200, response_data

    def insertCategoricalRelationDataFrame(self,):
        table_column_and_dtype_mapper = [
            ("dataset_id", "%s"),
            ("original_key_column_id", "%s"),
            ("original_value_column_id", "%s"),
            
            ("is_active", "%s::bool"),
            ("created_by", "%s"),
            ("modified_by", "%s"),
            ("created_on", "current_timestamp"),
            ("modified_on", "current_timestamp"),
        ]

        query = (
            self.schema_name,
            self.table_name,
            self.returning_key,
            table_column_and_dtype_mapper,
        )

        # TODO: Need to test this
        insert_status , message = self.base_executor.executeBatch(
            query=query,
        )
        
        if insert_status != -1:
            response_code, response_data = self.fetchByDatasetId()
            return 200, response_data

        
    ## TODO: Make a column name to column ID translation
    
    