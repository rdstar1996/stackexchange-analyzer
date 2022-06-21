import os
import sys
ROOT_DIR=os.path.dirname(__file__)
sys.path.append(ROOT_DIR)
from modelio.loader.commentsXMLDataLoader import loadCommentsXMLData
from modelio.loader.postHistoryXMLDataLoader import loadPostHistoryXMLData
from modelio.loader.postLinksXMLDataLoader import loadPostLinksXMLData
from modelio.loader.tagsXMLDataLoader import loadTagsXMLData
from modelio.loader.votesXMLDataLoader import loadVotesXMLData

from modeller.userModeller import createUserBadgesModel,createUserPostsModel,createUserCommentsVotesModel
from modeller.postsModeller import createPostHistoryModel,createPostLinksModel,createPostCommentsModel,createPostVotesModel
from pyspark.sql.functions import col
from pyspark.sql import Column

def main(raw_data_path=None):
    XML_INPUT_DIR=os.path.realpath(raw_data_path)

    # userBadgesJoinedDF=createUserBadgesModel(XML_INPUT_DIR)
    # userBadgesJoinedDF.printSchema()
    # userBadgesJoinedDF.show(10)
    # print(f"Num elements in userBadgesJoinedDf is {userBadgesJoinedDF.count()}")

    # userPostsJoinedDF=createUserPostsModel(XML_INPUT_DIR)
    # userPostsJoinedDF.printSchema()
    # userPostsJoinedDF.show(10)
    # print(f"Num elements in userPostsJoinedDf is {userPostsJoinedDF.count()}")

    # userCommentsVotesDf=createUserCommentsVotesModel(XML_INPUT_DIR)
    # userCommentsVotesDf.printSchema()
    # userCommentsVotesDf.show(10)
    # print(f"Num elements in userCommentsVotesDf is {userCommentsVotesDf.count()}")

    # postHistoryModelDf=createPostHistoryModel(XML_INPUT_DIR)
    # postHistoryModelDf.printSchema()
    # postHistoryModelDf.show(10)
    # print(f"Num of elements in postHistory model is {postHistoryModelDf.count()}")

    # postLinksModelDf=createPostLinksModel(XML_INPUT_DIR)
    # postLinksModelDf.printSchema()
    # postLinksModelDf.show(10)
    # print(f"Num of elements in postLinksModelDf is {postLinksModelDf.count()}")

    # postCommentsModelDf=createPostCommentsModel(XML_INPUT_DIR)
    # postCommentsModelDf.printSchema()
    # postCommentsModelDf.show(10)
    # print(f"Num of elements in postCommentsModelDf is {postCommentsModelDf.count()}")

    postVotesModelDf=createPostVotesModel(XML_INPUT_DIR)
    postVotesModelDf.printSchema()
    postVotesModelDf.show(10)
    print(f"Num of elements in postVotesModelDf is {postVotesModelDf.count()}")



    # comments_loaded_df=loadCommentsXMLData(XML_INPUT_DIR)
    # comments_loaded_df.show(10)
    # print(f"Num elements in comments dataframe is :{comments_loaded_df.count()}")

    # postHistory_loaded_df=loadPostHistoryXMLData(XML_INPUT_DIR)
    # postHistory_loaded_df.show(10)
    # print(f"Num elements in post history dataframe is:{postHistory_loaded_df.count()}")

    # postLinks_loaded_df=loadPostLinksXMLData(XML_INPUT_DIR)
    # postLinks_loaded_df.show(10)
    # print(f"Num elements in post links dataframe is:{postLinks_loaded_df.count()}")

    # post_loaded_df=loadPostXMLData(XML_INPUT_DIR)
    # post_loaded_df.show(10)
    # print(f"Num elements in post dataframe is:{post_loaded_df.count()}")

    # tags_loaded_df=loadTagsXMLData(XML_INPUT_DIR)
    # tags_loaded_df.show(10)
    # print(f"Num elements in tags dataframe is:{tags_loaded_df.count()}")

    # votes_loaded_df=loadVotesXMLData(XML_INPUT_DIR)
    # votes_loaded_df.show(10)
    # print(f"Num of elements in votes dataframe is :{votes_loaded_df.count()} ")

if __name__ == "__main__" :
    try:
        main("./resources/testdata/3dprinting.stackexchange.com")
    except Exception as e:
        print(e)
    
    