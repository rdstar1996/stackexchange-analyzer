from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import post_schema
import api
import os

def loadPostXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the post XML data and process it into a dataframe
        Sample post data looks like this 
        <row Id="1" PostTypeId="1" AcceptedAnswerId="51" CreationDate="2016-01-12T18:45:19.963" Score="10" ViewCount="389" Body="&lt;p&gt;When I've printed an object I've had to choose between high resolution and quick prints.  What techniques or technologies can I use or deploy to speed up my high resolution prints?&lt;/p&gt;&#xA;" OwnerUserId="16" LastActivityDate="2017-10-31T02:31:08.560" Title="How to obtain high resolution prints in a shorter period of time?" Tags="&lt;resolution&gt;&lt;speed&gt;&lt;quality&gt;" AnswerCount="2" CommentCount="6" ContentLicense="CC BY-SA 3.0" />
        .
        .
        <row Id="14" PostTypeId="1" AcceptedAnswerId="47" CreationDate="2016-01-12T19:22:14.277" Score="19" ViewCount="3470" Body="&lt;p&gt;I would like to print parts (e.g. jewellery) for use which I don't want to look or feel like a plastic, but metal-like, so briefly people won't see much difference.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Are there any specific type of home-printers that can achieve that? Or it's rather kind of filament that you should use?&lt;/p&gt;&#xA;" OwnerUserId="20" LastActivityDate="2019-06-18T14:32:01.577" Title="How to print metal-like parts?" Tags="&lt;filament&gt;&lt;metal-parts&gt;" AnswerCount="7" CommentCount="2" FavoriteCount="4" ContentLicense="CC BY-SA 3.0" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """
    FILE_NAME="Posts.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=(SparkSession
    .builder
    .appName("stackexchange-analyzer")
    .getOrCreate()
    )

    # read the xml file
    postRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "posts")
        .format("xml").load(filePath)
    )

    postDataExplode:DataFrame =(
        postRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(post_schema)))
            )
    )
    
    postDataExplode.printSchema()
    return postDataExplode