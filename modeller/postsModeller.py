from ntpath import join
from tokenize import String
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id,col
import api
from modelio.loader import commentsXMLDataLoader,postHistoryXMLDataLoader,postLinksXMLDataLoader,postXMLDataLoader,votesXMLDataLoader
from modeller.modelschema import postHistoryModel_schema,postLinks_schema,postComments_schema,postVotes_schema
from typing import List

def createPostHistoryModel(xml_path:str)->DataFrame:
    postHistoryDf:DataFrame=postHistoryXMLDataLoader.loadPostHistoryXMLData(xml_path).drop("Id","CreationDate")
    postDf:DataFrame=postXMLDataLoader.loadPostXMLData(xml_path)

    postsHistoryDf:DataFrame=postDf.join(postHistoryDf,postDf.Id==postHistoryDf.PostId)

    postHistoryFinalDf:DataFrame= (postsHistoryDf
            .drop("Id")
            .withColumn("Id",monotonically_increasing_id())
            .select(*list(map(lambda x: col(x),api.getMembers(postHistoryModel_schema))))
        )
    
    return postHistoryFinalDf

def createPostLinksModel(xml_path:str)->DataFrame:

    postLinksDf:DataFrame =(postLinksXMLDataLoader
            .loadPostLinksXMLData(xml_path)
            .drop("Id","CreationDate")
        )
    
    postDf:DataFrame = postXMLDataLoader.loadPostXMLData(xml_path)

    postsLinkDf:DataFrame = postDf.join(postLinksDf,postDf.Id==postLinksDf.PostId)

    postsLinkFinalDf:DataFrame = (postsLinkDf
                .drop("Id")
                .withColumn("Id",monotonically_increasing_id())
                .select(*list(map(lambda x: col(x),api.getMembers(postLinks_schema)))))
    
    return postsLinkFinalDf

def createPostCommentsModel(xml_path:str)->DataFrame:

    commentsDf:DataFrame=(commentsXMLDataLoader
            .loadCommentsXMLData(xml_path)
            .withColumnRenamed("Score","CommentScore")
            .drop("Score","Id","CreationDate"))
    
    postDf:DataFrame = postXMLDataLoader.loadPostXMLData(xml_path)

    postCommentsDf:DataFrame = (postDf
            .join(commentsDf,postDf.Id == commentsDf.PostId))
    
    postCommentsFinalDf:DataFrame = (postCommentsDf
            .drop("Id")
            .withColumn("Id",monotonically_increasing_id())
            .select(*list(map(lambda x: col(x),api.getMembers(postComments_schema)))))
    
    return postCommentsFinalDf

def createPostVotesModel(xml_path:str)->DataFrame:
    votesDf:DataFrame=(votesXMLDataLoader
            .loadVotesXMLData(xml_path)
            .drop("CreationDate"))
    
    postDf:DataFrame = postXMLDataLoader.loadPostXMLData(xml_path)

    postVotesDf:DataFrame=(postDf
        .join(votesDf,postDf.Id == votesDf.PostId))

    postVotesFinalDf:DataFrame = (postVotesDf
        .drop("Id")
        .withColumn("Id",monotonically_increasing_id())
        .select(*list(map(lambda x: col(x),api.getMembers(postVotes_schema)))))
    
    return postVotesFinalDf
