from pyspark.sql import SparkSession, DataFrame, Row, Column
from pyspark.sql.functions import col,explode
from modelio.dataschema import badges_schema
import api
import os
def loadBadgesXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the Badges Xml data and process it into dataframe.

        The sample badges data look like this.
        <badges>
            <row Id="2" UserId="23" Name="Autobiographer" Date="2016-01-12T18:44:49.267" Class="3" TagBased="False" />
            .
            .
            <row Id="3" UserId="26" Name="biographer" Date="2017-03-11T13:40:55.290" Class="1" TagBased="False" />
        </badges>
        
        The function will create a Data frame by extracting the "UserId":LongType,"Date":String,"Name":String from the Badges.xml
    Args:
        xmlPath (str): input path for data 
    
    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        DataFrame: 
    """
    FILE_NAME="Badges.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError

    spark=SparkSession.builder.getOrCreate()

    # read the xml file
    badgesRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "badges")
        .format("xml").load(xmlPath)
    )

    badgesDataExplode:DataFrame =(
        badgesRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(badges_schema)))
            )
    )
    
    badgesDataExplode.printSchema()
    return badgesDataExplode