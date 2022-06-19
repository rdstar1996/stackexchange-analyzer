from dataclasses import replace
from pyspark.sql.types import StructType,StructField
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit
from typing import Any, List

def getMembers(dataSchema:StructType)->List[str]:
    """ Gets the members from a StructType class and returns a list of members
        StructType([
            StructField("col1",StringType(),nullable=True),
            StructField("col2",StringType(),nullable=True),
            StructField("col3",IntegerType(),nullable=True)
        ])
    Returns:
        list: list of members names 
    """
    return dataSchema.fieldNames()

def columnUpdater(oldColName:str,replaceStr:str,dataFrame:DataFrame)->DataFrame:
    """ Functionality to replace any column of a dataFrame with a new column

    Args:
        oldColName (str): old column name of dataframe
        replaceStr (str): new column name of dataframe
        dataFrame (DataFrame): Dataframe on which the operation will be applied
    Returns:
        DataFrame: new dataframe with updated column
    """

    return dataFrame.withColumnRenamed(oldColName,replaceStr)

def columnChecker(dataSchema:StructType,dataFrame:DataFrame,**kwargs)-> List[col]:
    """ Checks the input dataframe schema matches with the actual schema intended for the application.
    If a particular column is not matching with the schema None Type will be assigned and returned with col name as per the schema.
    Only the columns names matching with the schema will be returned as its.

    Args:
        dataSchema (StructType): The schema inteded to be used for application. 
        dataFrame (DataFrame): The dataframe for which the schema will be compared 

    Returns:
        List[col]: List of all col types matching with the actual schema
    """
    default_value:Any=kwargs.get("default_value")
    actual_cols:set=set(getMembers(dataSchema))
    dataFrame_cols:set=set(getMembers(dataFrame.schema))
    col_diff:set=actual_cols.difference(dataFrame_cols)
    # print(f"Actual col {actual_cols}")
    # print(f"Data col {dataFrame_cols}")
    # print(col_diff)
    selected_col=[]
    for col_name in actual_cols:
        if col_name in col_diff:
            selected_col.append(lit(default_value).alias(col_name))
        else:
            selected_col.append(col(col_name))
    print(selected_col)
    return selected_col
        

