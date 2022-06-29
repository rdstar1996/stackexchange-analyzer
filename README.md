# Stackexchnage-Analyzer

## About
The idea behind this project is to design a salable modular ETL pipeline using pyspark.
The project uses open source [stack overflow data](https://archive.org/details/stackexchange) to process the raw data into meaningful information which can further used for analytics. 
Most of the concepts in this project is based from the book [Learn Spark 2.0 Databricks version](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf).

The designing aspect of the project is based on the git hub repo:[stackexchnage-spark-scala-alayzer](https://github.com/prompt-spark/stackexchange-spark-scala-analyser)

## Prerequisite for the project(Windows10 OS)
- Download [python](https://www.python.org/downloads/) from here install. 
- Download spark spark-2.3.0-bin-hadoop2.7 from [Download Spark](https://spark.apache.org/downloads.html) in the directory  *C:\spark*.
- For installation of JAVA and setting up the environment variables on windows 10, follow the steps from [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/)
- Create a environment variable PYTHONPATH and add the values:
    - *C:\spark\spark-2.3.0-bin-hadoop2.7\python*
    - *C:\spark\spark-2.3.0-bin-hadoop2.7\python\lib\py4j-0.10.6-src.zip*
- Create a directory in C drive for storing the spark logs, example *C:\spark-eventlog*
- Go to the spark configs present at *C:\spark\spark-2.3.0-bin-hadoop2.7\conf* and create a copy of the file **spark-defaults.conf.template** in the same directory and rename the duplicate copy as **spark-defaults.conf**
- Remove the default contents and paste these lines and save:
    ```
    spark.master                     spark://master:7077
    spark.eventLog.enabled           true
    spark.history.fs.logDirectory    file:///c:/spark-eventlog/
    spark.eventLog.dir               file:///c:/spark-eventlog/
    spark.serializer                 org.apache.spark.serializer.KryoSerializer
    spark.driver.memory              5g
    spark.debug.maxToStringFields    100
    spark.sql.catalogImplementation  hive
    spark.executor.extraJavaOptions  -XX:+PrintGCDetails -Dkey=value -Dnumbers="one two three"
    ```
- Download the [spark-xml](https://jar-download.com/?search_box=databricks%20xml) jar, as in this project the raw data is in the form of excel, put the jar file in *C:\spark\spark-2.3.0-bin-hadoop2.7\jars*, for this project the jar used is  **spark-xml-0.5.0.jar**
## Usage 
- Clone the repo.
- Create a virtual environment using cmd
    ```
        python -m venv .venv
    ```
- To check if the pyspark is availiable in the virtual environment use cmd
    ```
        pip list
    ```
    and see pyspark 2.3.0
- To see all the  cmd line options use:
    ```
        python main.py -h
    ``` 
- To execute the main file of the project use the cmd:
    Before executing create a folder where you want to store the ouput
    ```
        python main.py --input .\resources\testdata\3dprinting.stackexchange.com --output <output path>
            --functional-model <Can be ignored for now>
    ```
    After the execution is complete a csv output will be generated in the output directory specified.
- To modify the spark configuration based on **spark-defaults.conf** used the *.configs/sparkConfigs.json* present in the project directory.
```
{
    "sparkConfigs":{
        "spark.app.name":"stackexchange-analyzer",
        "spark.history.fs.logDirectory":"file:///E:/programming/stack-analyzer-event-log/",
        "spark.eventLog.dir":"file:///E:/programming/stack-analyzer-event-log/"
    }
}
```




