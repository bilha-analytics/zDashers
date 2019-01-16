import os, sys 
import base64, re 
from io import StringIO

import configparser

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

_goon = True

DB_CLE = "cle.csv"
DB_CLH = "clh.csv"
DB_PA = "pa.csv"
DB_PA_R = "pa_r.csv"

_data_dir = "res"
_mode_cronned = True

_pkey = None 
_uword = None
_uwords = None
_uwho = None

_db_colz = {
    DB_CLE: [], 
    DB_CLH: [], 
    DB_PA: [], 
    DB_PA_R: [] 
    }

_db_frames = {
    DB_CLE: pd.DataFrame(columns= _db_colz[DB_CLE] ), 
    DB_CLH: pd.DataFrame(columns= _db_colz[DB_CLH] ), 
    DB_PA: pd.DataFrame(columns= _db_colz[DB_PA] ), 
    DB_PA_R: pd.DataFrame(columns= _db_colz[DB_PA_R] ) 
    }

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
		"Unsupported Cases" : [ "", None]
	}

####
## TODO: refactor config reading 
##
####
def config_load(key=None):
    global _uwords, _uword, _uwho 
    if _goon:
        _uword = StringIO(str(os.environ.get("UWORDDB"))).getvalue()
        _uwords = StringIO(str(os.environ.get("UWORDKEY"))).getvalue()
        _uwho = StringIO(str(os.environ.get("UWHO"))).getvalue()
        _mode_cronned = StringIO(str(os.environ.get("UCRON"))).getvalue() == "TRUE"
        lazy_logger("config_load", "FIN LOADED from ENV") 
    else:
        #et, ev, etr = sys.exc_info()
        lazy_logger("config_load", "NOT in ENV - loading from file") # - {} {}".format(et, ev)) 
        try:
            cp = configparser.RawConfigParser()
            cp.read( r'uwords_ignore') 
            lazy_logger("config_load", "parser ready {} -- {}".format( cp.sections() , cp.defaults() ) ) 
            _uword = cp.get('worderz', 'uworddb') #.getvalue()
            _uwords = cp.get('worderz', 'uwordkey') #.getvalue()
            _uwho= cp.get('worderz', 'uname') #.getvalue()
            _mode_cronned = cp.get('worderz', 'cron').upper() == "TRUE"
            lazy_logger("config_load", "FIN config file loaded {} {} {} {}") #.format(_uword, _uwords, _uwho, _mode_cronned) )
        except:
            et, ev, etr = sys.exc_info()
            lazy_logger("config_load", "ERROR config file not found - {} {}".format(et, ev) ) 

def fetch_key():
    global _pkey 
    ##TODO thread safe lock 
    if ( _pkey is None):
        try:
            lazy_logger('fetch_key', "Starting uwords {}") #.format(_uwords) )
            _pkey = paramiko.RSAKey.from_private_key(StringIO(str(os.environ.get("DPRIV_KEY"))), _uwords ) 
            lazy_logger('fetch_key', "Key found in env variable.")
        except: #_pkey = "C:\\Users\\wairimu\\.ssh\\id_rsa-lg" "C:\\Users\\BILHA\\.ssh\\id_rsa"
            _pkey = "C:\\Users\\BILHA\\.ssh\\id_rsa"
            lazy_logger('fetch_key', "Key not in env variable. Read from file")
    return _pkey 


"""
Find a word = check if a cell has one of a given list of words and return the key of the list it has first
"""
def searchString( word, inDict): 
	def get_s(s):
		return str(s).upper() if (s is not None) else "" 
	for k, v in inDict.items():
		v = pd.Series( v ).apply( lambda s: get_s(s) )
		#lazy_log( "%s [%s]"%(k,v ) )
		rgx = re.compile( r'\b(?:%s)\b' % '|'.join( v ) )
		if pd.isnull( word ):
			return "No Response"
		if rgx.search( word.upper() ) :
			return k 

####
##
##
####
def fetch_data_file(db_csv_name):
    db = pd.DataFrame() 
    try:
        lazy_logger("fetch_data_file", "START - Fetching table - {}".format( db_csv_name ) )
        db = pd.read_csv( "{}/{}".format(_data_dir, db_csv_name) ) 
    except:
        et, ev, etr = sys.exc_info()
        lazy_logger( "fetch_data_file" , "ERROR - {} {}".format(et, ev) )

    return db 

####
##
##
####
def fetch_data_psql( ls_tablez): 
    res_tables = [] 
    rhost = "rdbms.dev.medicmobile.org"
    host = '127.0.0.1'
    rport = 33696
    db_port = 5432
    ssh_port = 22
    uname = _uwho  
    uword = _uword 
    uwords = _uwords 
    dbname = "lg_innovation_ke"
    sshtunnel.SSH_TIMEOUT = 5.0
    sshtunnel.TUNNEL_TIMEOUT = 5.0

    conn = None
    eng = None 
    tunnel = None

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
            lazy_logger("fetch_data_sql", "DB Connection set up {}")#.format(_uword)) 
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

def load_data_from_files(): 
	global _db_frames
	for d in _db_colz.keys():
		db = fetch_data_file( d ) 
		#if len(db.index) <= 0: 
			#db = pd.DataFrame( columns=_db_colz[d] )  
		_db_frames[ d ] = db 
		lazy_logger("load_data_from_files", "LOADED - {}".format(d) ) 
	lazy_logger("load_data_from_files", "CHECK loaded" ) 
	for k, v in _db_frames.items():
		lazy_logger("load_data_from_files", "FIN checked - {} shape = {}".format(k, v.shape) )   

####
##
##
####
def get_cle_data():
	lazy_logger("get_cle_data", "db frames found {}".format( _db_frames[DB_CLE].shape ) ) 
	return _db_frames[DB_CLE]

def fetch_cle_sql():
	_db_cle = pd.DataFrame() 
	lazy_logger("get_cle_data", "CLE shape = {}".format( _db_cle.shape ) )
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
			
			lazy_logger("get_cle_data", "FIN data loaded for CLE shape = {}".format( _db_cle.shape ) ) 
		
		except:
			et, ev, etr = sys.exc_info() 
			lazy_logger("get_cle_data", "ERROR fetching CLE data - {} {}".format(et, ev) )
	else:
		lazy_logger("get_cle_data", "ALREADY fetched CLE - reading local")
	return _db_cle


####
##
##
####
def get_clh_data():
    return _db_frames[DB_CLH]

def fetch_clh_sql():
    _db_clh  = pd.DataFrame()
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

            lazy_logger("get_clh_data", "FIN data loaded for CLH shape = {}".format( _db_clh.shape ) ) 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_clh_data", "ERROR fetching CLH data - {} {}".format(et, ev) )

    return _db_clh 


####
##
##
####
def get_pa_data():
    return _db_frames[DB_PA] 

def fetch_pa_sql():
    _db_pa = pd.DataFrame()
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
            
            lazy_logger("get_pa_data", "FIN data loaded for PA shape = {}".format( _db_pa.shape ) ) 
        except:
            et, ev, etr = sys.exc_info() 
            lazy_logger("get_pa_data", "ERROR fetching PA data - {} : {}".format(et, ev) )

    return _db_pa 

####
##
##
####
def get_pa_risks_data():
    return _db_frames[DB_PA_R]

def fetch_pa_r_sql(): 
    _db_pa_r = pd.DataFrame()
    if( len(_db_pa_r.index) == 0):  ##TODO: time based refresh 
        try:
            lazy_logger("get_pa_risks_data", "START data load PA_R") 

            _db_pa_r, = fetch_data_psql( ["pa_dashboard_view_chw",] ) 
      
            lazy_logger("get_pa_risks_data", "FIN data loaded for PA_R shape = {}".format( _db_pa_r.shape ) ) 
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
    _db_cle = _db_frames[DB_CLE]
    if( len(_db_cle.index) > 0 ):   
        STARTED = max( pd.to_datetime(_db_cle.reported_date, format="%Y-%m-%d").min(), pd.to_datetime( '2018-08-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
        LAST_UPDATED = pd.to_datetime(_db_cle["reported_date"], format="%Y-%m-%d").max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION

####
##
##
####
def get_clh_duration_details():
    STARTED, LAST_UPDATED, DURATION = [1990, 1990, -1]
    _db_clh = _db_frames[DB_CLH]
    if( len(_db_clh.index) > 0 ):   
        STARTED = max(pd.to_datetime(_db_clh.reported_date, format="%Y-%m-%d").min(), pd.to_datetime( '2018-10-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
        LAST_UPDATED = pd.to_datetime(_db_clh["reported_date"], format="%Y-%m-%d").max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION

####
##
##
####
def get_pa_duration_details():
    STARTED, LAST_UPDATED, DURATION = [1990, 1990, -1]
    _db_clh = _db_frames[DB_CLH]
    if( len(_db_clh.index) > 0 ):   
        STARTED = pd.to_datetime( '2018-10-26', format="%Y-%m-%d").strftime( '%d-%b-%Y') 
        LAST_UPDATED = pd.to_datetime(_db_clh["reported_date"],format="%Y-%m-%d").max().strftime( '%d-%b-%Y')
        DURATION = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED, format="%d-%b-%Y").to_period('M')
    return STARTED, LAST_UPDATED, DURATION



####
##
##
####
def get_cle_options_list():
    _db_cle = _db_frames[DB_CLE]
    if len(_db_cle.index) > 0: 
        return [ var_all_reasons] + _db_cle[var_bucket_reasons].unique().tolist()
    else:
      return [] 


####
##
##
####
def get_clh_options_list():
    _db_clh = _db_frames[DB_CLH]
    if( len(_db_clh.index) > 0 ): 
        return [var_all_reasons] + _db_clh.health_facility.unique().tolist()
    else:
      return [] 


####
##
##
####
def get_pa_options_list():
    _db_pa = _db_frames[DB_PA]
    if(len(_db_pa.index) > 0 ): 
        return [ var_all_reasons] + _db_pa[var_pa_risk_type].unique().tolist() 
    else:
      return [] 


####
##
##
####
def hget_pa_rates_cu():
	_db_pa_r = _db_frames[DB_PA_R] 
	try:
		lazy_logger( "hget_pa_rates_cu", "get pivot " )
		t = pd.pivot_table( _db_pa_r, index="community_unit", 
			values=["expected_high_risk_delivery_visits", "actual_high_risk_delivery_visits", 
					"expected_high_risk_pnc_visits", "actual_high_risk_pnc_visits",
					"expected_high_risk_iccm_visits", "actual_high_risk_iccm_visits", 
					], 
			aggfunc='sum', margins=True )
		t.columns = ["HF Delivery - Target", "HF Delivery - Actual","PNC Visits - Target", "PNC Visits - Actual", "ICCM - Target", "ICCM - Actual"] 
		return t
	except:
		et, ev, etr = sys.exc_info() 
		lazy_logger( "hget_pa_rates_cu", "ERROR pivot - {} : {}".format( et, ev ) ) 
		return pd.DataFrame()  


### TODO: refector below group
def get_pa_pivot_summary(db):
	dl = len( db )
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
	dl = len( db )
	lazy_logger("get_cle_pivot_summary", "db shape = {}".format( db.shape ) )
	try:
		db["reported_date"] = pd.to_datetime( db["reported_date"], format="%Y-%m-%d")
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
		dbh["reported_date"] = pd.to_datetime( dbh["reported_date"], format="%Y-%m-%d")
		lazy_logger( "get_clh_pivot_summary", "START pivot DB.SHAPE = {}".format( dbh.shape ) ) 
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
