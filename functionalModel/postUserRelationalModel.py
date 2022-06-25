from pyspark.sql.types import StructType,StructField,LongType,StringType

userPostVotesCountModel:StructType=StructType(
    [
        StructField("countUserId",LongType(),nullable=True),
        StructField("countPostId",LongType(),nullable=True),
        StructField("Views",LongType(),nullable=True),
        StructField("Reputation",LongType(),nullable=True),
        StructField("MaxCommentsScore",LongType(),nullable=True),
        StructField("Location",StringType(),nullable=True)
    ]
)