import functionalModel as fm
from pyspark.sql import DataFrame
import os
def runbatchJob(input_path,output_path,functionalModel):
    outputDf:DataFrame=fm.userPostVotesCount(input_path)
    outputDf.show(10)
    if os.path.exists(output_path):
        outputDf.coalesce(1).write.mode("Overwrite").csv(output_path+"userPostCountVote.csv")
    else:
        raise FileNotFoundError

