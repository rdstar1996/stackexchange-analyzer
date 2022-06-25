from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import postLinks_schema
import api
import os
def loadPostLinksXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the post Links XML data and process it into a dataframe
        Sample post Link data looks like this 
        <row Id="33" CreationDate="2016-01-12T20:16:38.260" PostId="49" RelatedPostId="2" LinkTypeId="1" />
        .
        .
        <row Id="382" CreationDate="2016-01-13T20:41:04.500" PostId="211" RelatedPostId="181" LinkTypeId="1" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """

    FILE_NAME="PostLinks.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=SparkSession.builder.getOrCreate()

    # read the xml file
    postLinksRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "postlinks")
        .format("xml").load(filePath)
    )

    postLinkDataExplode:DataFrame =(
        postLinksRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(postLinks_schema)))
            )
    )
    
    postLinkDataExplode.printSchema()
    return postLinkDataExplode