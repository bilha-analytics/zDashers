import os
import datetime as dt 


####
##
##
####
def is_log_enabled():
    return True


####
##
##
####
def lazy_logger(src, msg): 
    tstamp = dt.datetime.now() 

    if( is_log_enabled() ):
        print( ">>> {0} : {1} :  {2}".format(tstamp, src, msg) )


####
##
##
####