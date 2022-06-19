from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import postHistory_schema
import api
import os
def loadPostHistoryXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the postHistory XML data and process it into a dataframe
        Sample post history data looks like this 
        <row Id="1" PostHistoryTypeId="2" PostId="1" RevisionGUID="6deffe8b-79c7-467b-a173-a3497b4fb476" CreationDate="2016-01-12T18:45:19.963" UserId="16" Text="When I've printed an object I've had to choose between high resolution and quick prints.  What techniques or technologies can I use or deploy to speed up my high resolution prints?" ContentLicense="CC BY-SA 3.0" />
        .
        .
        <row Id="3" PostHistoryTypeId="3" PostId="1" RevisionGUID="6deffe8b-79c7-467b-a173-a3497b4fb476" CreationDate="2016-01-12T18:45:19.963" UserId="16" Text="&lt;resolution&gt;&lt;speed&gt;&lt;quality&gt;" ContentLicense="CC BY-SA 3.0" />

    Args:
        xmlPath (str): input path for Data

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """

    FILE_NAME="PostHistory.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=(SparkSession
    .builder
    .appName("stackexchange-analyzer")
    .getOrCreate()
    )

    # read the xml file
    postHistoryRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "posthistory")
        .format("xml").load(filePath)
    )

    # select the row and use explode method to convert the array of entities to rows 
    # Since the individual row is of structType `col`, to access individual structValue of `col`, use 'col.*'
    postHistoryExplode:DataFrame =(
        postHistoryRawDF
        .select(explode(col("row"))).select("col.*"))

    # update the col names by removing _ from the start and convert int into title case, so that all rows have uniform case and matches with Schema structure
    cleanedPostHistoryColNames:list=(list(map(lambda x: x.strip('_'),api.getMembers(postHistoryExplode.schema))))
    postHistoryColumsUpdated:DataFrame=postHistoryExplode.toDF(*cleanedPostHistoryColNames)

    # Now check if the data after cleaning is matching with the expected schema, if any column is not present with the schema , 
    # it will not be selected, or if it present in schema but not in dataframe null will be assigned
    columnsToBeSelected:list=api.columnChecker(postHistory_schema,postHistoryColumsUpdated,default_value="default_value")
    postHistoryData:DataFrame=postHistoryColumsUpdated.select(*columnsToBeSelected)
    
    postHistoryData.printSchema()
    return postHistoryData