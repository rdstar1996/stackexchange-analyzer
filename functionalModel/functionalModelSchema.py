from functionalModel.postUserRelationalModel import userPostVotesCountModel
from modeller import postsModeller,userModeller
from pyspark.sql.functions import count,col,max
from pyspark.sql import DataFrame

def userPostVotesCount(input_path):

    postCommentsModelDf:DataFrame = (postsModeller
                .createPostCommentsModel(input_path)
                .withColumnRenamed("PostId","CommentsPostId"))
    
    userVotesModelDf:DataFrame = (userModeller
                .createUserCommentsVotesModel(input_path)
                .withColumnRenamed("Score","CommentsScore")
                )

    userPostVotesCountDf:DataFrame = (postCommentsModelDf
                .join(userVotesModelDf,postCommentsModelDf.UserId==userVotesModelDf.UserId)
                .select("OwnerUserId","PostId","Views","Reputation","CommentsScore","Location")
                .groupBy("Views","Reputation","Location")
                .agg(count("PostId"),count("OwnerUserId"),max(col("CommentsScore")))
                .withColumnRenamed("count(PostId)","CountPostId")
                .withColumnRenamed("count(ownerUserId)","CountUserId")
                .withColumnRenamed("max(commentsScore)","MaxCommentsScore"))
    
    return userPostVotesCountDf


