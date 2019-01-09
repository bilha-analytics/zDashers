import os, sys 
import base64, re 
from io import StringIO

import ConfigParser

## data structures and manipulation 
import pandas as pd
import numpy as np 
import datetime as dt 

import paramiko, sshtunnel 
from sshtunnel import SSHTunnelForwarder , create_logger 

import psycopg2 

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker 

from appmodel.utils import lazy_logger 

_pkey = None 
_uword = None
_uwords = None

_db_cle = pd.DataFrame() 
_db_clh = pd.DataFrame() 
_db_pa = pd.DataFrame() 
_db_pa_r = pd.DataFrame()  


var_bucket_unit = "health_facility"
var_pa_risk_type = "Risk Type"
var_bucket_reasons =  "Category Referral Reason"
var_all_reasons = "All"
var_Unsupported = "Unsupported Cases"
var_HIVST = "HIVST Refer"
var_HIVST_ED = "HIVST Ed"
var_Display_Colz = ["reported_date", "chv_name", "patient_name" ,"reason_for_referral", var_bucket_reasons]


reasons_groupz = { 
		"Neonates" : [ "months", "umbilical", "umbllical", "Survical"], 
		"Pregnancy" : ["delivery"], 
		"U5 Danger Signs" : [ "Danger", "Diarrhoea", "Diarrhea", "Dairrhea", "Coughing", "Cough", "Fever","Pneumonia", "Pnuemonia",
							"Pheumonia", "Malaria", "voting"],
		"HIVST Refer" : ["reactive", "partner", "intimate"], 
		"HIVST Ed" : ["HIVST_Assessed", ], 
		"Unsupported Cases" : []
	}

####
##
##
####
def fetch_key():
    global _pkey 
    ##TODO thread safe lock 
    if ( _pkey is None):
        try:
            _pkey = paramiko.RSAKey.from_private_key(StringIO(str(os.environ.get("DPRIV_KEY"))), fetch_uword_key() ) 
            lazy_logger('fetch_key', "Key found in env variable.")
        except:
            _pkey = "..\\id_rsa-lg" 
            lazy_logger('fetch_key', "Key not in env variable. Read from file")
    return _pkey 

def fetch_uword_key(): 
    global _uwords 
    ##TODO thread safe lock 
    if ( _uwords is None):
        try:
            _uwords = str(StringIO(str(os.environ.get("UWORD_KEY"))) )
            if( _uwords is not None): 
                lazy_logger('fetch_uword_key', "UWORD found in env variable.")
            else:
                _uwords = "..\\uword-key-lg" 
                lazy_logger('fetch_uword_key', "UWORD not in env variable. Read from file")
        except:
            lazy_logger('fetch_uword_key', "ERROR : UWORD not found")
    return _uwords 

def fetch_uword_db():
    global _uword
    ##TODO thread safe lock 
    if ( _uword is None):
        try:
            _uword = str(StringIO(str(os.environ.get("UWORD_DB")))) 
            if( _uword is not None): 
                lazy_logger('fetch_uword_db', "UWORD_DB found in env variable.")
            else:
                _uword = "..\\uword-db-lg" 
                lazy_logger('fetch_uword_db', "UWORD_DB not in env variable. Read from file")
        except:
            lazy_logger('fetch_uword_db', "ERROR : UWORD_DB not found")
    return _uword 

####
##
##
####
def fetch_data_psql( ls_tablez): 
    rhost = "rdbms.dev.medicmobile.org"
    host = '127.0.0.1'
    rport = 33696
    db_port = 5432
    ssh_port = 22
    uname = "bilha" 
    uword = fetch_uword_db() 
    uwords = fetch_uword_key() 
    dbname = "lg_innovation_ke"
    sshtunnel.SSH_TIMEOUT = 5.0
    sshtunnel.TUNNEL_TIMEOUT = 5.0

    conn = None
    eng = None 
    tunnel = None
    res_tables = [] 

    try:
        lazy_logger("fetch_data_sql", "START - SSH tunnel create to db")

        with SSHTunnelForwarder(
			(rhost, rport),
			ssh_private_key = fetch_key(), 
			ssh_username=uname,
			ssh_password=uwords,
			remote_bind_address=(host, db_port), 
			#logger = create_logger(loglevel=1) 
        ) as tunnel:
            lazy_logger("fetch_data_sql", "DB Connection set up") 
            eng = create_engine("postgresql://{}:{}@{}:{}/{}".format(uname, uword, host, tunnel.local_bind_port, dbname), echo=False)
            con = eng.connect()
            
            lazy_logger("fetch_data_sql", "DB Connected. Reading tables next") 
            for tbl in ls_tablez:
                lazy_logger("fetch_data_sql", "Fetching table - {}".format( tbl ) )
                res_tables.append( pd.read_sql_table(tbl, eng) ) 			

        lazy_logger("fetch_data_sql", "FIN - data fetched") 
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger("fetch_data_sql", "ERROR fetching data - {} {}".format(et, ev) ) 
    finally:
        try:
            if( conn ):
                conn.close() 
                eng.dispose() 
                tunnel.stop() 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("fetch_data_sql", "ERROR closing connection - {} {}".format(et, ev) )  
    
    return res_tables



####
##
##
####
def get_cle_data():
    global _db_cle
    if( len(_db_cle.index) == 0):  ##TODO: time based refresh 
        try:
            lazy_logger("get_cle_data", "START data load CLE") 

            _db_cle, = fetch_data_psql( ["useview_referral_to_hf"] ) 

            _db_cle["chv_name"] = "CHV " +  pd.Series( pd.factorize( _db_cle["chv_name"])[0] + 1).astype( str)
            _db_cle["patient_name"] = "Patient " +  pd.Series( pd.factorize( _db_cle["patient_name"])[0] + 1).astype( str)
            _db_cle.drop( "chv_phone", axis=1, inplace=True)
            _db_cle.drop( 'month', axis=1, inplace=True)

            ## digital referrals 
            _db_cle["Category Referral Reason"] = _db_cle["reason_for_referral"].apply( lambda x: searchString(x, reasons_groupz) ) 
            
            ###### DATE-TIME
            # 1. set Date of referral to a datetime type 
            _db_cle["reported_date"] = pd.to_datetime( _db_cle["reported_date"], format="%Y-%m-%d") # inplace=True) 

            # 2. Extract Month and year categories 
            _db_cle["Year"] = _db_cle["reported_date"].dt.year 
            _db_cle["Month"] = _db_cle["reported_date"].dt.strftime('%b-%y') 
            _db_cle["Day_of_Week"] = _db_cle["reported_date"].dt.weekday_name.str[:3] 

            _db_cle.sort_values( by='reported_date', inplace=True)


            lazy_logger("get_cle_data", "FIN data loaded for CLE") 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_cle_data", "ERROR fetching CLE data - {} {}".format(et, ev) )
    return _db_cle


####
##
##
####
def get_clh_data():
    global _db_clh
    if( len(_db_clh.index) == 0 ):  ##TODO: time based refresh 
        try:
            lazy_logger("get_clh_data", "START data load CLH") 

            _db_clh, = fetch_data_psql( ["useview_hivst"] ) 

            _db_clh["chv_name"] = "CHV " +  pd.Series( pd.factorize( _db_clh["chv_name"])[0] + 1).astype( str)
            _db_clh["patient_name"] = "Patient " +  pd.Series( pd.factorize( _db_clh["patient_name"])[0] + 1).astype( str)
            _db_clh.drop( "chv_phone", axis=1, inplace=True)

            _db_clh["health_facility"][ pd.isnull(_db_clh["health_facility"] ) ] =  "Assessment Only" #== "[NULL]"
            _db_clh["reason_for_referral"][ pd.isnull(_db_clh["reason_for_referral"] )] =  "HIVST_Assessed" #pd.isnull(dbh["reason_for_referral"])
            _db_clh["Category Referral Reason"] = _db_clh["reason_for_referral"].apply( lambda x: searchString(x, reasons_groupz) ) 
            # shorter descriptions for referral reasons HIVST TODO: refactor regex grep
            _db_clh["reason_for_referral"][ _db_clh["reason_for_referral"] == "CHV referral: reactive or non-disclose" ] =  "Reactive/non-disclose"
            _db_clh["reason_for_referral"][ _db_clh["reason_for_referral"] == "Intimate partner violence case" ] =  "IPV case"
            _db_clh["reason_for_referral"][ _db_clh["reason_for_referral"] == "CHV referral: reactive or non-discloseInvalid test" ] =  "Invalid test"

            
            # 1. set Date of referral to a datetime type 
            #dbh["reported_date"] = pd.to_datetime( dbh["reported_date"].astype(str).str[:10], format="%Y-%m-%d") %d/%m/%Y
            _db_clh["reported_date"] = pd.to_datetime( _db_clh["reported_date"], format="%Y-%m-%d") # inplace=True)

            # 2. Extract Month and year categories 
            _db_clh["Year"] = _db_clh["reported_date"].dt.year 
            _db_clh["Month"] = _db_clh["reported_date"].dt.strftime('%b-%y') 
            _db_clh["Day_of_Week"] = _db_clh["reported_date"].dt.weekday_name.str[:3] 

            _db_clh.sort_values( by='reported_date', inplace=True)

            lazy_logger("get_clh_data", "FIN data loaded for CLH") 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_clh_data", "ERROR fetching CLH data - {} {}".format(et, ev) )

    return _db_clh 


####
##
##
####
def get_pa_data():
    global _db_pa
    if( len(_db_pa.index) == 0):  ##TODO: time based refresh 
        try:
            lazy_logger("get_pa_data", "START data load PA") 

            iccm, hf, neo, = fetch_data_psql( ["pa_task_view_iccm", "pa_task_view_hf_delivery", "pa_task_view_newborn_ds"] ) 

            colz = ["patient_id", "chw", "high_risk", "task_start", "task_end", "task_name"] 
            colz_iccm = ["patient_id", "chw_id", "high_risk", "task_start", "task_end", "task_name"] 	
                
            iccm["task_name"] = "ICCM Monthly" 
            
            iccm = iccm[ colz_iccm]
            iccm.columns = colz 
            hf = hf[ colz] 
            neo = neo[ colz]            
            
            iccm[var_pa_risk_type] = "ICCM" 
            hf[var_pa_risk_type] = "HF Delivery" 
            neo[var_pa_risk_type] = "PNC Neonate" 
            
            _db_pa = pd.concat( [iccm, hf, neo] ) 
            
            _db_pa["task_start"] = pd.to_datetime( _db_pa["task_start"], format="%Y-%m-%d") # inplace=True)
            _db_pa["Month"] = _db_pa["task_start"].dt.strftime('%b-%y') 

            _db_pa["task_end"] = pd.to_datetime( _db_pa["task_end"], format="%Y-%m-%d") # inplace=True)
            _db_pa["Month_end"] = _db_pa["task_end"].dt.strftime('%b-%y') 
            
            _db_pa.sort_values( by='task_start', inplace=True)
            
            lazy_logger("get_pa_data", "FIN data loaded for PA") 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_pa_data", "ERROR fetching PA data - {} : {}".format(et, ev) )

    return _db_pa 

####
##
##
####
def get_pa_risks_data():
    global _db_pa_r
    if( len(_db_pa_r.index) == 0):  ##TODO: time based refresh 
        try:
            lazy_logger("get_pa_risks_data", "START data load PA_R") 

            _db_pa_r, = fetch_data_psql( ["pa_dashboard_view_chw",] ) 
      
            lazy_logger("get_pa_risks_data", "FIN data loaded for PA_R") 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_pa_risks_data", "ERROR fetching PA_R data - {} : {}".format(et, ev) )

    return _db_pa_r
    
####
##
##
####
def get_cle_duration_details():
    STARTED, LAST_UPDATED, DURATION = [1990, 1990, -1]
    if( len(_db_cle.index) > 0 ):   
        STARTED = max(_db_cle.reported_date.min(), pd.to_datetime( '2018-08-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
        LAST_UPDATED = _db_cle["reported_date"].max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION

####
##
##
####
def get_clh_duration_details():
    STARTED, LAST_UPDATED, DURATION = [1990, 1990, -1]
    if( len(_db_clh.index) > 0 ):   
        STARTED = max(_db_clh.reported_date.min(), pd.to_datetime( '2018-10-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
        LAST_UPDATED = _db_clh["reported_date"].max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION

####
##
##
####
def get_pa_duration_details():
    STARTED, LAST_UPDATED, DURATION = [1990, 1990, -1]
    if( len(_db_clh.index) > 0 ):   
        STARTED = pd.to_datetime( '2018-10-26', format="%Y-%m-%d").strftime( '%d-%b-%Y') 
        LAST_UPDATED = _db_clh["reported_date"].max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION



####
##
##
####
def get_cle_options_list():
    if len(_db_cle.index) > 0: 
        return [ var_all_reasons] + _db_cle[var_bucket_reasons].unique().tolist()
    else:
      return [] 


####
##
##
####
def get_clh_options_list():
    if( len(_db_clh.index) > 0 ): 
        return [var_all_reasons] + _db_clh.health_facility.unique().tolist()
    else:
      return [] 


####
##
##
####
def get_pa_options_list():
    if(len(_db_pa.index) > 0 ): 
        return [ var_all_reasons] + _db_pa[var_pa_risk_type].unique().tolist() 
    else:
      return [] 


####
##
##
####
def hget_pa_rates_cu(): 
    try:
        lazy_logger( "hget_pa_rates_cu", "get pivot " )
        return pd.pivot_table( _db_pa_r, index="community_unit", 
            values=["expected_high_risk_delivery_visits", "actual_high_risk_delivery_visits", 
                    "expected_high_risk_pnc_visits", "actual_high_risk_pnc_visits",
                    "expected_high_risk_iccm_visits", "actual_high_risk_iccm_visits", 
                ], 
                aggfunc='sum', margins=True )
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger( "hget_pa_rates_cu", "ERROR pivot - {} : {}".format( et, ev ) ) 
        return pd.DataFrame()  


### TODO: refector below group
def get_pa_pivot_summary(db):
    try:
        t = pd.pivot_table( db, index=var_pa_risk_type, values=["patient_id"], aggfunc='count', margins=True ).T
        t["Total per CHV"] = dl / len( db.chw.unique() )
        t["Per CHV per Month"] = ( dl/ len( db.chw.unique() ) )/ ( pd.to_datetime( dt.datetime.now(), format="%Y-%m-%d" ).to_period('M') - pd.to_datetime( '2018-10-26', format="%Y-%m-%d").to_period('M'))
        t['Number of CHVs'] =  len( db.chw.unique() )
        t['Records per Client'] =  dl /len( db.patient_id.unique() )
        lazy_logger( "get_pa_pivot_summary", "FIN pivot")
        return t.round(2)
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger( "get_pa_pivot_summary", "ERROR pivot - {} : {}".format( et, ev ) )
        return pd.DataFrame()


def get_cle_pivot_summary(db):
    try:
        t = pd.pivot_table( db, index=var_bucket_reasons, values=["referral_uuid"], aggfunc='count', margins=True ).T
        t["Total per CHV"] = dl / len( db.chv_name.unique() )
        t["Per CHV per Month"] = ( dl/ len( db.chv_name.unique() ) )/ (db.reported_date.max().to_period('M') - max(db.reported_date.min().to_period('M'), pd.to_datetime( '2018-08-23', format="%Y-%m-%d").to_period('M')) )
        t['Number of CHVs'] =  len( db.chv_name.unique() ) 
        t['Records per Client'] =  dl /len( db.patient_name.unique() )
        lazy_logger( "get_cle_pivot_summary", "FIN pivot")
        return t.round(2) 
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger( "get_cle_pivot_summary", "ERROR pivot - {} : {}".format( et, ev ) )
        return pd.DataFrame()

def get_clh_pivot_summary(dbh): 
    try:
        t2 = pd.pivot_table( dbh, index="health_facility", values=["hivst_enrollment_uuid"], aggfunc='count', margins=True ).T
        t2["Total per CHV"] = len( dbh ) / len( dbh.chv_name.unique() )
        t2["Per CHV per Month"] = (len( dbh ) / len( dbh.chv_name.unique() ) )/ (dbh.reported_date.max().to_period('M') - max(dbh.reported_date.min().to_period('M'), pd.to_datetime( '2018-10-24', format="%Y-%m-%d").to_period('M')) )
        t2['Number of CHVs'] =  len( dbh.chv_name.unique() ) 
        t2['Records per Client'] =  len(dbh)/len( dbh.patient_name.unique() )
        lazy_logger( "get_clh_pivot_summary", "FIN pivot") 
        return t2.round(2)
    except:
        et, ev, etr = sys.exc_info() 
        lazy_logger( "get_clh_pivot_summary", "ERROR pivot - {} : {}".format( et, ev ) ) 
        return pd.DataFrame()
