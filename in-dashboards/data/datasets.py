import os

## data structures and manipulation 
import pandas as pd
import numpy as np 

import re 

import datetime as dt 


"""
Find a word = check if a cell has one of a given list of words and return the key of the list it has first
"""
def searchString( word, inDict): 
    for k, v in inDict.items():
        v = pd.Series( v ).apply( lambda s: s.upper() )
        #print( "%s [%s]"%(k,v ) )
        rgx = re.compile( r'\b(?:%s)\b' % '|'.join( v ) ) 
        if pd.isnull( word ):
            return "No Response"
        
        if rgx.search( word.upper() ) :
            return k 








##### LOAD 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker 

import sshtunnel 
from sshtunnel import SSHTunnelForwarder , create_logger

import psycopg2 

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0


def load_eng(): 
	global db, dbh 
	
	eng = create_engine( "" , echo=False)
	Session = sessionmaker( bind=eng)
	session = Session()
	Base = declarative_base() 
	
	tb_cle = Table( "useview_referral_to_hf", 
		Base.meta,
		autoload=True,
		autoload_with=eng,
		schema="public.useview_referral_to_hf" )
		
	q = session.query( tb_cle )
	
	db = pd.DataFrame( q.all() )
	


def load_psql():
	global db, dbh
	rhost = "rdbms.dev.medicmobile.org"
	host = 'localhost'
	rport = 33696
	db_port = 5432
	ssh_port = 22
	uname = "bilha" 
	uword = "cpk4zgqq"
	uwords = "@N#&user123"
	dbname = "lg_innovation_ke"
	
	sshtunnel.SSH_TIMEOUT = 5.0
	sshtunnel.TUNNEL_TIMEOUT = 5.0
	
	print( ">>> creating SSH tunnel") 
	
	with SSHTunnelForwarder(
		(rhost, rport),
		ssh_private_key = "c:\\Users\\Bilha\\.ssh\\id_rsa",  
		ssh_username=uname,
		ssh_password=uwords,
		remote_bind_address=(host, db_port),
		local_bind_address=(host,db_port),
		logger = create_logger(loglevel=1)
	) as tunnel:
	
		print( ">>> Connecting to DB") 
		eng = create_engine("postgresql://{}:{}@{}:{}/{}".format(uname, uword, host, tunnel.local_bind_port, dbname), echo=False)
		
		con = eng.connect()
		db = pd.read_sql_table( "useview_referral_to_hf", eng)
		dbh = pd.read_sql_table( "useview_hivst", eng)
		print(">>>>>loaded")


def load_tables_simple():
	global db, dbh
	
	try:
		eng = create_engine("postgresql://bilha:cpk4zgqq@rdbms.dev.medicmobile.org:33696/lg_innovation_ke", echo=False)
		con = eng.connect()
		db = pd.read_sql_table( "useview_referral_to_hf", eng)
		dbh = pd.read_sql_table( "useview_hivst", eng)
		print(">>>>>loaded")
	except:
		db = pd.read_csv( "data/referral_to_hf.csv")  
		dbh = pd.read_csv( "data/hivst.csv" ) 
		print(">>>>>failed") 


#load_tables_simple()
db = pd.read_csv( "data/referral_to_hf.csv")  
dbh = pd.read_csv( "data/hivst.csv" ) 
	
##### ANON
db["chv_name"] = "CHV " +  pd.Series( pd.factorize( db["chv_name"])[0] + 1).astype( str)
db["patient_name"] = "Patient " +  pd.Series( pd.factorize( db["patient_name"])[0] + 1).astype( str)
db.drop( "chv_phone", axis=1, inplace=True)
db.drop( 'month', axis=1, inplace=True)

dbh["chv_name"] = "CHV " +  pd.Series( pd.factorize( dbh["chv_name"])[0] + 1).astype( str)
dbh["patient_name"] = "Patient " +  pd.Series( pd.factorize( dbh["patient_name"])[0] + 1).astype( str)
dbh.drop( "chv_phone", axis=1, inplace=True)


##### CLASSES

reasons_groupz = { 
    "Neonates" : [ "months", "umbilical", "umbllical"], 
    "Pregnancy" : ["delivery"], 
    "U5 Danger Signs" : [ "Danger", "Diarrhoea", "Diarrhea", "Dairrhea", "Coughing", "Cough", "Fever","Pneumonia", "Pnuemonia",
                        "Pheumonia"],
    "HIVST Refer" : ["reactive", "partner", "intimate"], 
    "HIVST Ed" : ["HIVST_Assessed", ], 
    "Unsupported Cases" : []
}

## digital referrals 
db["Category Referral Reason"] = db["reason_for_referral"].apply( lambda x: searchString(x, reasons_groupz) ) 

## HIVST assessments 
dbh["health_facility"][ pd.isnull(dbh["health_facility"] ) ] =  "Assessment Only" #== "[NULL]"
dbh["reason_for_referral"][ pd.isnull(dbh["reason_for_referral"] )] =  "HIVST_Assessed" #pd.isnull(dbh["reason_for_referral"])
dbh["Category Referral Reason"] = dbh["reason_for_referral"].apply( lambda x: searchString(x, reasons_groupz) ) 
# shorter descriptions for referral reasons HIVST TODO: refactor regex grep
dbh["reason_for_referral"][ dbh["reason_for_referral"] == "CHV referral: reactive or non-disclose" ] =  "Reactive/non-disclose"
dbh["reason_for_referral"][ dbh["reason_for_referral"] == "Intimate partner violence case" ] =  "IPV case"
dbh["reason_for_referral"][ dbh["reason_for_referral"] == "CHV referral: reactive or non-discloseInvalid test" ] =  "Invalid test"

var_bucket_reasons =  "Category Referral Reason"
var_all_reasons = "All"
var_Unsupported = "Unsupported Cases"
var_HIVST = "HIVST Refer"
var_HIVST_ED = "HIVST Ed"
var_Display_Colz = ["reported_date", "chv_name", "patient_name" ,"reason_for_referral", var_bucket_reasons]



###### DATE-TIME
# 1. set Date of referral to a datetime type 
db["reported_date"] = pd.to_datetime( db["reported_date"], format="%Y-%m-%d") # inplace=True)
#db["reported_date"] = pd.to_datetime( db["reported_date"].astype(str).str[:10], format="%Y-%m-%d") #%H:%M:%S

# 2. Extract Month and year categories 
db["Year"] = db["reported_date"].dt.year 
db["Month"] = db["reported_date"].dt.strftime('%b-%y') 
db["Day_of_Week"] = db["reported_date"].dt.weekday_name.str[:3] 

db.sort_values( by='reported_date', inplace=True)

STARTED_CLE = max(db.reported_date.min(), pd.to_datetime( '2018-08-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
LAST_UPDATED = db["reported_date"].max().strftime( '%d-%b-%Y')


# 1. set Date of referral to a datetime type 
#dbh["reported_date"] = pd.to_datetime( dbh["reported_date"].astype(str).str[:10], format="%Y-%m-%d") %d/%m/%Y
dbh["reported_date"] = pd.to_datetime( dbh["reported_date"], format="%Y-%m-%d") # inplace=True)

# 2. Extract Month and year categories 
dbh["Year"] = dbh["reported_date"].dt.year 
dbh["Month"] = dbh["reported_date"].dt.strftime('%b-%y') 
dbh["Day_of_Week"] = dbh["reported_date"].dt.weekday_name.str[:3] 

dbh.sort_values( by='reported_date', inplace=True)

STARTED_HIVST = max(dbh.reported_date.min(), pd.to_datetime( '2018-10-23', format="%Y-%m-%d")).strftime( '%d-%b-%Y')
LAST_UPDATED_HIVST  = db["reported_date"].max().strftime( '%d-%b-%Y')



### SUmmary table Digital Referral 
#t = pd.pivot_table( db, index=var_bucket_reasons, values=["referral_uuid"], aggfunc='count', margins=True ).T
#t["Total per CHV"] = len( db ) / len( db.chv_name.unique() )
#t[ "Total per CHV per Month"] = (len( db ) / len( db.chv_name.unique() ) )/ (db.reported_date.max().to_period('M') - max(db.reported_date.min().to_period('M'), pd.to_datetime( '2018-08-23', format="%Y-%m-%d").to_period('M')) )
#t['Number of CHVs'] =  len( db.chv_name.unique() ) 
#t['Records per Client'] =  len(db)/len( db.patient_name.unique() )
#t = t.round(2)


#### SUmmary Table HIVST
#display(HTML('<b>Number of records Table - HIVST Assessments</b>'))
#t2 = pd.pivot_table( dbh, index="health_facility", values=["hivst_enrollment_uuid"], aggfunc='count', margins=True ).T
#t2["Total per CHV"] = len( dbh ) / len( dbh.chv_name.unique() )
#t2[ "Total per CHV per Month"] = (len( dbh ) / len( dbh.chv_name.unique() ) )/ (dbh.reported_date.max().to_period('M') - max(dbh.reported_date.min().to_period('M'), pd.to_datetime( '2018-10-24', format="%Y-%m-%d").to_period('M')) )
#t2['Number of CHVs'] =  len( dbh.chv_name.unique() ) 
#t2['Records per Client'] =  len(dbh)/len( dbh.patient_name.unique() )
#t2 = t2.round(2)


## options list = referral reasons for CLE
options_reasons = [ var_all_reasons] + db[var_bucket_reasons].unique().tolist()

## options list = health facilities or CU for CLH
options_facility = [var_all_reasons] + dbh.health_facility.unique().tolist()
var_bucket_unit = "health_facility"


dbp = pd.DataFrame()
var_pa_risk_type = "Risk Type"
STARTED_PA = pd.to_datetime( '2018-10-26', format="%Y-%m-%d").strftime( '%d-%b-%Y')

def mend_PA():
	global dbp, var_pa_risk_type
	
	colz = ["patient_id", "chw", "high_risk", "task_start", "task_end", "task_name"] 
	colz_iccm = ["patient_id", "chw_id", "high_risk", "task_start", "task_end", "task_name"] 	
	
	dbp_iccm = pd.read_csv( "data/pa_iccm.csv" ) 
	dbp_hf = pd.read_csv( "data/pa_hf.csv" ) 
	dbp_neo = pd.read_csv( "data/pa_neo.csv" ) 
	
	dbp_iccm["task_name"] = "ICCM Monthly" 
	
	dbp_iccm = dbp_iccm[ colz_iccm]
	dbp_iccm.columns = colz 
	dbp_hf = dbp_hf[ colz]
	dbp_neo = dbp_neo[ colz]
	
	
	dbp_iccm[var_pa_risk_type] = "ICCM" 
	dbp_hf[var_pa_risk_type] = "HF Delivery" 
	dbp_neo[var_pa_risk_type] = "PNC Neonate" 
	
	dbp = pd.concat( [dbp_iccm, dbp_hf, dbp_neo] ) 
	
	dbp["task_start"] = pd.to_datetime( dbp["task_start"], format="%Y-%m-%d") # inplace=True)
	dbp["Month"] = dbp["task_start"].dt.strftime('%b-%y') 

	dbp["task_end"] = pd.to_datetime( dbp["task_end"], format="%Y-%m-%d") # inplace=True)
	dbp["Month_end"] = dbp["task_end"].dt.strftime('%b-%y') 
	
	dbp.sort_values( by='task_start', inplace=True)
	
	#print( dbp.sample( 5 ) )
	
	#return dbp 
	
mend_PA() 
options_pa = [ var_all_reasons] + dbp[var_pa_risk_type].unique().tolist()	

def get_pivot_summary_PA(db): 
	dl = len( db )
	try:
		t = pd.pivot_table( db, index=var_pa_risk_type, values=["patient_id"], aggfunc='count', margins=True ).T
		t["Total per CHV"] = dl / len( db.chw.unique() )
		t["Per CHV per Month"] = ( dl/ len( db.chw.unique() ) )/ ( pd.to_datetime( dt.datetime.now(), format="%Y-%m-%d" ).to_period('M') - pd.to_datetime( '2018-10-26', format="%Y-%m-%d").to_period('M'))
		t['Number of CHVs'] =  len( db.chw.unique() ) 
		t['Records per Client'] =  dl /len( db.patient_id.unique() )
		return t.round(2)
	except:
		return pd.DataFrame()


def get_pivot_summary_referrals(db):
	### SUmmary table Digital Referral 
	dl = len( db )
	try:
		t = pd.pivot_table( db, index=var_bucket_reasons, values=["referral_uuid"], aggfunc='count', margins=True ).T
		t["Total per CHV"] = dl / len( db.chv_name.unique() )
		t["Per CHV per Month"] = ( dl/ len( db.chv_name.unique() ) )/ (db.reported_date.max().to_period('M') - max(db.reported_date.min().to_period('M'), pd.to_datetime( '2018-08-23', format="%Y-%m-%d").to_period('M')) )
		t['Number of CHVs'] =  len( db.chv_name.unique() ) 
		t['Records per Client'] =  dl /len( db.patient_name.unique() )
		return t.round(2)
	except:
		return pd.DataFrame()

def get_pivot_summary_hivst(dbh):
	#### SUmmary Table HIVST
	#display(HTML('<b>Number of records Table - HIVST Assessments</b>'))
	try:
		t2 = pd.pivot_table( dbh, index="health_facility", values=["hivst_enrollment_uuid"], aggfunc='count', margins=True ).T
		t2["Total per CHV"] = len( dbh ) / len( dbh.chv_name.unique() )
		t2["Per CHV per Month"] = (len( dbh ) / len( dbh.chv_name.unique() ) )/ (dbh.reported_date.max().to_period('M') - max(dbh.reported_date.min().to_period('M'), pd.to_datetime( '2018-10-24', format="%Y-%m-%d").to_period('M')) )
		t2['Number of CHVs'] =  len( dbh.chv_name.unique() ) 
		t2['Records per Client'] =  len(dbh)/len( dbh.patient_name.unique() )
		return t2.round(2)
	except:
		return pd.DataFrame()
		

#pd.to_datetime( '2018-08-23', format="%Y-%m-%d"))	
DURATION_CLE = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED_CLE, format="%d-%b-%Y").to_period('M')
DURATION_CLH = pd.to_datetime( LAST_UPDATED_HIVST, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED_HIVST, format="%d-%b-%Y").to_period('M')
DURATION_PA = pd.to_datetime( LAST_UPDATED, format="%d-%b-%Y").to_period('M') - pd.to_datetime( STARTED_PA, format="%d-%b-%Y").to_period('M')