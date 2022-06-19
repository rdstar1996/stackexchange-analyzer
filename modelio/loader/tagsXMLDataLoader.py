from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import tags_schema
import api
import os

def loadTagsXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the tags XML data and process it into a dataframe
        Sample post data looks like this 
         <row Id="1" TagName="resolution" Count="25" ExcerptPostId="434" WikiPostId="433" />
        .
        .
        <row Id="2" TagName="speed" Count="45" ExcerptPostId="112" WikiPostId="111" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """
    FILE_NAME="Tags.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=(SparkSession
    .builder
    .appName("stackexchange-analyzer")
    .getOrCreate()
    )

    # read the xml file
    tagsRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "tags")
        .format("xml").load(filePath)
    )

    tagsDataExplode:DataFrame =(
        tagsRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(tags_schema)))
            )
    )
    
    tagsDataExplode.printSchema()
    return tagsDataExplode