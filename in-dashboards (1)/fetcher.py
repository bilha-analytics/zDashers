from appmodel.utils import lazy_logger
from appmodel import model_commons 
import os, sys
import pandas as pd

###
# addon - heroku addons:create scheduler:standard
# deploy task run/test - heroku run python mytask.py - heroku run rake update_feed
# dashboard open - heroku addons:open scheduler
# 
######
def fetch_data_to_file():
    lazy_logger("fetch_data_to_file", "START fetch from db and save to file local") 
    data_dir = model_commons._data_dir 
    model_commons.config_load()
    try:
        model_commons.fetch_cle_sql().to_csv( "{}/{}".format(data_dir, model_commons.DB_CLE), encoding='utf-8')
        lazy_logger("fetch_data_to_file", "FIN DB_CLE")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_CLE - {} {}".format(et, ev) ) 
    try:
        model_commons.fetch_clh_sql().to_csv( "{}/{}".format(data_dir, model_commons.DB_CLH) ,encoding='utf-8')
        lazy_logger("fetch_data_to_file", "FIN DB_CLH")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_CLH - {} {}".format(et, ev) ) 
    try:
        model_commons.fetch_pa_sql().to_csv( "{}/{}".format(data_dir, model_commons.DB_PA) , encoding='utf-8')
        lazy_logger("fetch_data_to_file", "FIN DB_PA")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_PA- {} {}".format(et, ev) ) 
    try:
        model_commons.fetch_pa_r_sql().to_csv( "{}/{}".format(data_dir, model_commons.DB_PA_R), encoding='utf-8' )
        lazy_logger("fetch_data_to_file", "FIN DB_PA_R")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_PA_R - {} {}".format(et, ev) ) 
        


####
##
##
####
if __name__ == "__main__":
    lazy_logger('fetcher.main', "starting")
    fetch_data_to_file()
