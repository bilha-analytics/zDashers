import dash_core_components as dcc 
import dash_html_components as dht 
from dash.dependencies import Input, Output

from applayouts import ui_commons 
from appmodel import model_commons 

####
##
##
####
def get_layout( ):
	zstart, zend, ztot = model_commons.get_cle_duration_details()
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row form-group", children=[ 
			dht.Div( className="col-sm-2 col-form-label", id="c-tit",
				children=[ dht.P( "Select Referral Reason: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					ui_commons.get_Filter_Dropdown(  model_commons.get_cle_options_list(), 'Referral Reason', 'cle-filters-id') #model_commons.get_cle_options_list()
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format(zstart, zend, ztot) , className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
           ui_commons.make_stats_cards_div( [], "cle-card-summaries" ) 
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c1', children=[
				ui_commons.get_graph_holder('cle-r1g1')
			]),
			
			## by facility 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c2', children=[
				ui_commons.get_graph_holder('cle-r1g2')
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c3', children=[
				ui_commons.get_graph_holder('cle-r1g3')
			]),
			
			## by confirmation of referral 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c4', children=[
				ui_commons.get_graph_holder('cle-r1g4')
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
	@app.callback(Output('cle-card-summaries', 'children'),[Input('cle-filters-id', 'value')])
	def update_cards(ref_reason):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_bucket_reasons] == ref_reason]
			
			t = model_commons.get_cle_pivot_summary( df )
			
			lt = len( db[model_commons.var_bucket_reasons].unique() )
			
			t2 = t.columns[lt-1:] if ref_reason == model_commons.var_all_reasons else t.columns[1:]
			
			return [ ui_commons.make_stats_card(":{} - {}:".format(c, t[c][0]) , c) for c in t2]
		else:
			return []
	
	#####
    ## callbacks for card summaries
    #####	
	@app.callback(Output('r1c2', 'children'), [Input('cle-filters-id', 'value')])
	def update_graph2(ref_reason):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_bucket_reasons] == ref_reason]
			
			d = df["health_facility"].value_counts()
			
			return ui_commons.get_Bar_Chart('cle-r1g2', 
				d.index,  
				d,
				title= "By Facility - {} ".format( ref_reason) ,
				horizontal=True	
			)
		else:
			return ui_commons.get_graph_holder('cle-r1g2')
	
	#####
    ## callbacks for card summaries
    #####	
	@app.callback(Output('r1c3', 'children'), [Input('cle-filters-id', 'value')]) 
	def update_graph3(ref_reason):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_bucket_reasons] == ref_reason]
			
			d = df["Month"].groupby(df["Month"], sort=False).count()
			
			return get_Line_Chart('cle-r1g3', d.index,  d, title= "Monthly - {} ".format( ref_reason ) )
		else:
			return ui_commons.get_graph_holder('cle-r1g3')
	
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('r1c4', 'children'), [Input('cle-filters-id', 'value')])
	def update_graph4(ref_reason):
		db = model_commons.get_cle_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else: 
				df = db[ db[model_commons.var_bucket_reasons] == ref_reason]
		
			d = df["health_facility_confirmation"].value_counts() 
		
			return get_Pie_Chart('cle-r1g4', d.index,  d, title = "Facility Confirmation - {}".format(ref_reason ) )
		else: 
			return ui_commons.get_graph_holder('cle-r1g4')
				
