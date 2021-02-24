import logging
import sys, os
import urllib.request, json 
from submodule import extractAndInsert
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from functools import partial

if __name__ == '__main__':
	global threadId
    if(len(sys.argv)) != 5:
        raise Exception("Insufficient input arguments provided......")
    logging.info(sys.argv)
    db_loc = sys.argv[1]
    table = sys.argv[2]
    url = sys.argv[3]
    countyStr = sys.argv[4]
    countyLst = countyStr.split(",")
    obj = extractAndInsert(url,db_loc,table)

    with ThreadPoolExecutor() as executor:
    	fn = partial(obj.get_data)
    	executor.map(fn, countyLst)
    