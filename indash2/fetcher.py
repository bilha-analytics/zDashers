from appmodel.utils import lazy_logger
from appmodel import model_commons 
import os, sys
import pandas as pd

import boto3 
from io import StringIO 

###
# addon - heroku addons:create scheduler:standard
# deploy task run/test - heroku run python mytask.py - heroku run rake update_feed
# dashboard open - heroku addons:open scheduler
# 50.19.103.36
# TODO: pass instance along instead b/c breaks
######
def fetch_data_to_file():
    lazy_logger("fetch_data_to_file", "START fetch from db and save to file local") 
    
    model_commons.config_load()
    try:
        save_to_s3( model_commons.fetch_cle_sql(), model_commons.DB_CLE)
        lazy_logger("fetch_data_to_file", "FIN DB_CLE")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_CLE - {} {}".format(et, ev) ) 
    
    
    try:
        save_to_s3( model_commons.fetch_clh_sql(), model_commons.DB_CLH)           
        lazy_logger("fetch_data_to_file", "FIN DB_CLH")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_CLH - {} {}".format(et, ev) ) 
   
    
    try:
        save_to_s3( model_commons.fetch_pa_sql(), model_commons.DB_PA)
        lazy_logger("fetch_data_to_file", "FIN DB_PA")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_PA- {} {}".format(et, ev) ) 
    
    
    try:
        save_to_s3( model_commons.fetch_pa_r_sql(), model_commons.DB_PA_R) 
        lazy_logger("fetch_data_to_file", "FIN DB_PA_R")  
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_to_file", "ERROR DB_PA_R - {} {}".format(et, ev) ) 
        

def save_to_s3(db, csvname):
    lazy_logger("save_to_s3", "START for {} of shape {} to {}".format( csvname, db.shape, model_commons._awsid ) )
    data_dir = "dat" #model_commons.getDataDir() #_data_dir 
    cb = StringIO() 
    db.to_csv( cb, encoding='utf-8')
    s3 = boto3.resource("s3", aws_access_key_id=model_commons._awsid, aws_secret_access_key=model_commons._awskey)
    s3.Object(model_commons.S3_BUCKET, "{}/{}".format(data_dir, csvname) ).put(Body=cb.getvalue() ) 
    lazy_logger("save_to_s3", "FIN {}".format(csvname) )   

####
##
##
####
if __name__ == "__main__":
    lazy_logger('fetcher.main', "starting")
    fetch_data_to_file()
