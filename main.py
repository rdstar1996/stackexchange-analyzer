import os
import sys
ROOT_DIR=os.path.dirname(__file__)
sys.path.append(ROOT_DIR)
from jobs import *
import argparse
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import configs
parser=argparse.ArgumentParser(description="Argument parser for understanding the pyspark project")
parser.add_argument("--input",action="store",dest="inputPath",required=True,help="Input path for all the xml files")
parser.add_argument("--output",action="store",dest="outputPath",required=True,help="Output destination path for all the xml files")
parser.add_argument("--functional-model",action="store",dest="functionalModel",required=True,help="Name of functional model to execute")
args=parser.parse_args()

def readSparkConfig()->dict:
    return configs.getSparkFromJson()
def createSparkApp(**kwargs):
    sc=SparkConf()
    sc.setAll(list(readSparkConfig().items()))
    spark=(SparkSession.builder
            .config(conf=sc)
            .getOrCreate()
        )
    
def main():
    inputpath=getattr(args,"inputPath")
    outputPath=getattr(args,"outputPath")
    functionalModel=getattr(args,"functionalModel")
    createSparkApp()
    runbatchJob(inputpath,outputPath,functionalModel)

if __name__ == "__main__" :
    try:
        main()
    except Exception as e:
        print(e)
    
    