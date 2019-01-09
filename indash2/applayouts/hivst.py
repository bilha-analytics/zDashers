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
			dht.Div( className="col-sm-2 col-form-label", 
				children=[ dht.P( "Select Health Facility: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					ui_commons.get_Filter_Dropdown( model_commons.get_clh_options_list() , 'Health Facility', 'clh-filters-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format(zstart, zend, ztot), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
           ui_commons.make_stats_cards_div( [], "clh-card-summaries" ) 
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c1', children=[
				ui_commons.get_graph_holder('clh-r1g1')
			]),
			
			## by facility 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c2', children=[
				ui_commons.get_graph_holder('clh-r1g2')
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c3', children=[
				ui_commons.get_graph_holder('clh-r1g3')
			]),
			
			## by confirmation of referral 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c4', children=[
				ui_commons.get_graph_holder('clh-r1g4')
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
	@app.callback(Output('clh-card-summaries', 'children'), [Input('clh-filters-id', 'value')])
	def update_cards(ref_reason):
		db = model_commons.get_clh_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_bucket_unit] == ref_reason]
			
			t = model_commons.get_clh_pivot_summary( df )
			
			lt = len( db[model_commons.var_bucket_unit].unique() )
			
			t2 = t.columns[3:] if ref_reason == model_commons.var_all_reasons else t.columns[1:] 
			
			return [ ui_commons.make_stats_card(":{} - {}:".format(c, t[c][0]) , c) for c in t2]
		else:
			return [] 
			
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('clh-r1c2', 'children'), [Input('clh-filters-id', 'value')])
	def update_graph2_clh(ref_reason):
		dbh = model_commons.get_clh_data()
		if( len( dbh.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = dbh
			else:
				df = dbh[ dbh[model_commons.var_bucket_unit] == ref_reason]
			d = df[ df["reason_for_referral"] != 'HIVST_Assessed' ]["reason_for_referral"].value_counts()

			return get_Bar_Chart('clh-r1g2', d.index,  d, title= "HIVST Referral Reasons - {} ".format( ref_reason ), horizontal=True )
		else:
			return ui_commons.get_graph_holder('cle-r1g2')
	
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('clh-r1c3', 'children'), [Input('clh-filters-id', 'value')]) 
	def update_graph3_clh(ref_reason):
		dbh = model_commons.get_clh_data()
		if( len( dbh.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = dbh
			else:
				df = dbh[ dbh[model_commons.var_bucket_unit] == ref_reason]
			d = df["Month"].groupby(df["Month"], sort=False).count()
			
			return get_Line_Chart('clh-r1g3', d.index,  d, title= "Monthly - {} ".format( ref_reason ) )
		else:
			return ui_commons.get_graph_holder('clh-r1g3')
	
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('clh-r1c4', 'children'), [Input('clh-filters-id', 'value')]) 
	def update_graph4_clh(ref_reason):
		dbh = model_commons.get_clh_data()
		if( len( dbh.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = dbh
			else:
				df = dbh[ dbh[model_commons.var_bucket_unit] == ref_reason]
			d = df["health_facility_confirmation"].value_counts() 
			return get_Pie_Chart('clh-r1g4', d.index,  d, title = "Facility Confirmation - {}".format(ref_reason ) )
		else:
			return ui_commons.get_graph_holder('clh-r1g4')

