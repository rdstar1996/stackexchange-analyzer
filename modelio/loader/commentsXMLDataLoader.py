from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import comments_schema
import api
import os
def loadCommentsXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the comments XML data and process it into a dataframe
        Sample comments data looks like this 
        <row Id="1" PostId="1" Score="4" Text="Did I just place the first upvote?  Congrats on getting this site off the ground!" CreationDate="2016-01-12T18:47:12.573" UserId="23" ContentLicense="CC BY-SA 3.0" />
        .
        .
        <row Id="22" PostId="54" Score="1" Text="What printing technology and material are you using?" CreationDate="2016-01-12T20:29:22.827" UserId="48" ContentLicense="CC BY-SA 3.0" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """

    FILE_NAME="Comments.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=(SparkSession
    .builder
    .appName("stackexchange-analyzer")
    .getOrCreate()
    )

    # read the xml file
    commentsRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "comments")
        .format("xml").load(filePath)
    )

    commentsDataExplode:DataFrame =(
        commentsRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(comments_schema)))
            )
    )
    
    commentsDataExplode.printSchema()
    return commentsDataExplode