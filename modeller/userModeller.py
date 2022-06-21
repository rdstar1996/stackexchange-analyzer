
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id,col
import api
from modelio.loader import badgesXMLDataLoader,commentsXMLDataLoader,postXMLDataLoader,usersXMLDataLoader,votesXMLDataLoader
from modeller.modelschema import userBadges_schema,userPosts_schema,userCommentsVotes_schema
from typing import List

def createUserBadgesModel(xml_path:str)-> DataFrame:
    usersDf:DataFrame=usersXMLDataLoader.loadUsersXMLData(xml_path)
    badgesDf:DataFrame=badgesXMLDataLoader.loadBadgesXMLData(xml_path)

    userBadgesDf:DataFrame=(usersDf
                .join(badgesDf,usersDf.Id==badgesDf.UserId)
                .select(*list(map(lambda x: col(x),api.getMembers(userBadges_schema))))
                )
    return userBadgesDf

def createUserPostsModel(xml_path:str)->DataFrame:
    usersDf:DataFrame=usersXMLDataLoader.loadUsersXMLData(xml_path)
    postsDf:DataFrame=postXMLDataLoader.loadPostXMLData(xml_path).withColumnRenamed("Id","PostId")

    userPostsDf:DataFrame=(usersDf
                .join(postsDf,usersDf.Id==postsDf.OwnerUserId)
                .drop("Id","CreationDate")
                .withColumn("Id",monotonically_increasing_id())
                .select(*list(map(lambda x: col(x),api.getMembers(userPosts_schema))))
                )
    
    return userPostsDf

def createUserCommentsVotesModel(xml_path:str)->DataFrame:
    usersDf:DataFrame=usersXMLDataLoader.loadUsersXMLData(xml_path)
    commentsDf:DataFrame=commentsXMLDataLoader.loadCommentsXMLData(xml_path)
    votesDf:DataFrame=votesXMLDataLoader.loadVotesXMLData(xml_path)

    userCommentsDf:DataFrame=(usersDf
                    .join(commentsDf,usersDf.Id==commentsDf.UserId)
                    .drop("UserId","PostId")
                    )
    # userCommentsDf.show(10)
    userCommentsVotesDf:DataFrame=(
        votesDf
        .join(userCommentsDf,votesDf.UserId==userCommentsDf.Id)
        .drop("Id","CreationDate")
        .withColumn("Id",monotonically_increasing_id())
        .select(*list(map(lambda x: col(x),api.getMembers(userCommentsVotes_schema))))
    )

    return userCommentsVotesDf



