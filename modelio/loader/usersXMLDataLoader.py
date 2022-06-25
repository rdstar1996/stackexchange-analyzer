from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,explode
from modelio.dataschema import users_schema
import api
import os

def loadUsersXMLData(xmlPath:str) -> DataFrame:
    """ The function loads the users XML data and process it into a dataframe
        Sample users data looks like this 
        <row Id="-1" Reputation="1" CreationDate="2016-01-11T22:16:10.830" DisplayName="Community" LastAccessDate="2016-01-11T22:16:10.830" Location="on the server farm" AboutMe="&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xD;&#xA;&lt;ul&gt;&#xD;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xD;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackoverflow.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xD;&#xA;&lt;/ul&gt;" Views="139" UpVotes="51" DownVotes="3416" AccountId="-1" />
        .
        .
        <row Id="3" Reputation="101" CreationDate="2016-01-12T18:04:39.963" DisplayName="animuson" LastAccessDate="2021-10-12T22:44:13.300" WebsiteUrl="https://animuson.me" Location="Kansas City, MO, United States" AboutMe="&#xA;&#xA;&lt;p&gt;I work for Stack Overflow as a Senior Product Support Specialist.&lt;/p&gt;&#xA;" Views="6" UpVotes="0" DownVotes="0" ProfileImageUrl="https://i.stack.imgur.com/ugRaY.jpg?s=128&amp;g=1" AccountId="89201" />

    Args:
        xmlPath (str): input path for data 

    Raises:
        FileNotFoundError: Error raised when given File path not found

    Returns:
        Dataframe: returns a pyspark.sql.Dataframe
    """
    FILE_NAME="Users.xml"
    filePath:str=os.path.join(xmlPath,FILE_NAME)

    if not os.path.exists(filePath):
        raise FileNotFoundError
        
    spark=SparkSession.builder.getOrCreate()

    # read the xml file
    usersRawDF:DataFrame = (
        spark
        .read
        .option("rowTag", "users")
        .format("xml").load(filePath)
    )

    usersDataExplode:DataFrame =(
        usersRawDF
        .select(explode(col("row")))
        .select(
            list(map(lambda x:col("col._"+x).alias(x),api.getMembers(users_schema)))
            )
    )
    
    usersDataExplode.printSchema()
    return usersDataExplode