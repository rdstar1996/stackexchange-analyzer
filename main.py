import os
import sys
ROOT_DIR=os.path.dirname(__file__)
sys.path.append(ROOT_DIR)
from modelio.loader.batchXMLDataLoader import loadBadgesXMLData
from modelio.loader.commentsXMLDataLoader import loadCommentsXMLData
from modelio.loader.postHistoryXMLDataLoader import loadPostHistoryXMLData
from modelio.loader.postLinksXMLDataLoader import loadPostLinksXMLData
from modelio.loader.postXMLDataLoader import loadPostXMLData
from modelio.loader.tagsXMLDataLoader import loadTagsXMLData
from modelio.loader.usersXMLDataLoader import loadUsersXMLData
from modelio.loader.votesXMLDataLoader import loadVotesXMLData

def main(raw_data_path=None):
    XML_INPUT_DIR=os.path.realpath(raw_data_path)
    # batch_loaded_df=loadBadgesXMLData(XML_INPUT_DIR)
    # batch_loaded_df.show(10)
    # print(f"Num elements in badges dataframe is :{batch_loaded_df.count()}")

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

    # users_loaded_df=loadUsersXMLData(XML_INPUT_DIR)
    # users_loaded_df.show(10)
    # print(f"Num elements in users dataframe is :{users_loaded_df.count()}")


    votes_loaded_df=loadVotesXMLData(XML_INPUT_DIR)
    votes_loaded_df.show(10)
    print(f"Num of elements in votes dataframe is :{votes_loaded_df.count()} ")

if __name__ == "__main__" :
    try:
        main("./resources/testdata/3dprinting.stackexchange.com")
    except Exception as e:
        print(e)
    
    