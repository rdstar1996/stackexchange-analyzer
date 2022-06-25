import os
import json
import os
def _readConfigFileData()->dict:
    jsonData:dict={}
    configFilePath=os.path.join(os.path.dirname(__file__),"sparkConfigs.json")
    with open(configFilePath) as fp:
        jsonData=json.load(fp)
    return jsonData

def getSparkFromJson()->dict:
    jsonData=_readConfigFileData()
    return jsonData.get("sparkConfigs",None)

