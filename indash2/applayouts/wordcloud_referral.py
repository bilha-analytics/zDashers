import flask
import dash_core_components as dcc 
import dash_html_components as dht 
from dash.dependencies import Input, Output, State

from applayouts import ui_commons 
from appmodel import model_commons 

from appmodel.model_commons import *  

import pandas as pd 

import os 

####
##
##
####
def get_layout( ):
	zstart, zend, ztot = model_commons.get_cle_duration_details()
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row form-group", children=[ 
			dht.Div( className="col-sm-2 col-form-label", 
				children=[ dht.P( "Select Referral Reason: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					ui_commons.get_Filter_Dropdown( model_commons.get_cle_options_list(), 'Referral Reason', 'wc-filters-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format(zstart, zend, ztot), className="card-title" ) 
				]),
			
		]),
				
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-9 stretch-card card", id='wc-r1c1', children=[
				#ui_commons.get_graph_holder('wc-r1g1')
				dht.Img( src=ui_commons.plot_word_cloud( pd.Series(["loading. . .",])  , "holder"), id='wc1-img', width="80%", height="80%" )
			]),
			
			## by facility 
			dht.Div(className="col-3 stretch-card card", id='wc-r1c2', children=[
				ui_commons.get_graph_holder('wc-r1g2')
			]),
			
		]),
				
	])	 



####
##
##
####
def register_callback(app):
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('wc1-img', 'src'), [Input('wc-filters-id', 'value'), Input( 'dbloader', 'value')], [State('wc1-img', 'src')] )
	def update_wc_image(ref_reason, n, old):
		lazy_logger("update_wcimage", "START" )
		db = model_commons.get_cle_data()
		lazy_logger("update_wcimage", "wc dat = {}".format( db.shape ) ) 
		if len(db.index) > 0:
			db = db if ref_reason == model_commons.var_HIVST else db[ db[model_commons.var_bucket_reasons] != model_commons.var_HIVST ]
			lazy_logger("update_wcimage", "START wc dat filtered = {}".format( db.shape ) )

			if ref_reason != model_commons.var_all_reasons:
				db = db[ db[model_commons.var_bucket_reasons] == ref_reason] 

			lazy_logger("update_wcimage", "DONE wc dat filtered = {}".format( db.shape ) )
			return ui_commons.plot_word_cloud( db["reason_for_referral"] , ref_reason) #dht.Img( src=ui_commons.plot_word_cloud( df["reason_for_referral"] , ref_reason) , id='wc1-img', width="80%", height="80%" )
		else:
			return  old #ui_commons.plot_word_cloud( pd.Series(["No Data Yet!",])  , "holder") 
	'''
	STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
	@app.server.route('/output/<resource>' )
	def serve_image(resource): 
		lazy_logger("serve_image", "fetching from static path{}".format(STATIC_PATH )) 
		return flask.send_from_directory(STATIC_PATH, resource) 
	''' 
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('wc-r1c2', 'children'), [Input('wc-filters-id', 'value'), Input( 'dbloader', 'value')], [State('wc-r1c2', 'children')]) 
	def update_graph2wc(risk, n, old):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_bucket_reasons] == risk]
			d = df["source_form_name"].value_counts()
			return ui_commons.get_Bar_Chart('wc-r1g2', d.index,  d,title= "Source Form - {} ".format( risk) ,horizontal=True	)
		else:
			return old #ui_commons.get_graph_holder('wc-r1g2')
		
	'''
	@app.callback(Output('wc-r1c1', 'children'), [Input( 'dbloader', 'value')], [State('wc-r1c1', 'children')])
	def first_load_wc(n,old):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			return ui_commons.plot_word_cloud( db["reason_for_referral"] , model_commons.var_all_reasons)
		else:
			return old
	'''
	
	