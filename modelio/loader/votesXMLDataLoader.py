from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import votes_schema
import api
import os

def loadVotesXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the Votes XML data and process it into a dataframe
        Sample users data looks like this 
        <row Id="1" PostId="1" VoteTypeId="2" CreationDate="2016-01-12T00:00:00.000" />
        .
        .
        <row Id="3" PostId="3" VoteTypeId="2" CreationDate="2016-01-12T00:00:00.000" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """
    FILE_NAME="Votes.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=(SparkSession
    .builder
    .appName("stackexchange-analyzer")
    .getOrCreate()
    )

    # read the xml file
    votesRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "votes")
        .format("xml").load(filePath)
    )

    votesDataExplode:DataFrame =(
        votesRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(votes_schema)))
            )
    )
    
    votesDataExplode.printSchema()
    return votesDataExplode