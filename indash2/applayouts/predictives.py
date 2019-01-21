import dash_core_components as dcc 
import dash_html_components as dht 
from dash.dependencies import Input, Output, State

from applayouts import ui_commons 
from appmodel import model_commons  
from appmodel.model_commons import * 

####
##
##
####
def get_layout( ):
	zstart, zend, ztot = model_commons.get_pa_duration_details()
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row form-group", children=[ 
			dht.Div( className="col-sm-2 col-form-label", 
				children=[ dht.P( "Select Risk Group: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					ui_commons.get_Filter_Dropdown( model_commons.get_pa_options_list(), 'PA Risk Group', 'pa-filters-id') 
			]), 
			dht.Div( className="col-sm-4",  id="pa-dated", 
				children=[ dht.P("From: {} To: {} = Month # {}".format(zstart, zend, ztot), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", id ="pa-cards", children=[ 
           ui_commons.make_stats_cards_div( [], "pa-card-summaries" ) 
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-lg-3 stretch-card card", id='pa-r1c1', children=[
				ui_commons.get_graph_holder('pa-r1g1')
			]),
			
			## by facility 
			dht.Div(className="col-lg-3 stretch-card card", id='pa-r1c2', children=[
				ui_commons.get_graph_holder('pa-r1g2')
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='pa-r1c3', children=[
				ui_commons.get_graph_holder('pa-r1g3')
			]),
			
			## by confirmation of referral 
			dht.Div(className="col-lg-3 stretch-card card", id='pa-r1c4', children=[
				ui_commons.get_graph_holder('pa-r1g4')
			]),
		]),
		
		## graphs row 2
		dht.Div(className="row", children=[
			dht.Div(className="col-12 stretch-card card", id='pa-r2c1', children=[
				ui_commons.get_graph_holder('pa-r2g1') 
			])
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
	@app.callback(Output('pa-card-summaries', 'children'), [Input('pa-filters-id', 'value'), Input( 'dbloader', 'value')], [State('pa-cards', 'children')])
	def update_cards(ref_reason, n, old): 		
		db = model_commons.get_pa_data()
		if( len( db.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_pa_risk_type] == ref_reason]
			
			t = model_commons.get_pa_pivot_summary( df )
			
			lt = len( db[model_commons.var_pa_risk_type].unique() )
			
			t2 = t.columns[lt:] if ref_reason == model_commons.var_all_reasons else t.columns[1:]
			
			return [ ui_commons.make_stats_card(c, t[c][0]) for c in t2]
		else:
			return old #[]
			
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c2', 'children'), [Input('pa-filters-id', 'value'), Input( 'dbloader', 'value')], [State('pa-r1c2', 'children')])
	def update_graph2p(risk, n, old):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d = df["high_risk"].value_counts()
			return ui_commons.get_Bar_Chart('pa-r1g2', d.index,  d, title= "Is High Risk - {} ".format( risk) , horizontal=True ) 	
		else:
			return old #ui_commons.get_graph_holder('pa-r1g2')
	
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c3', 'children'), [Input('pa-filters-id', 'value'), Input( 'dbloader', 'value')], [State('pa-r1c3', 'children')]) 
	def update_graph3p(risk, n, old):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d1 = df["Month"].groupby(df["Month"], sort=False).count()
			d2 = df["Month_end"].groupby(df["Month_end"], sort=False).count()
			return ui_commons.get_Line_Chart_2('pa-r1g3', d1.index, d2.index, d1, d2, "task_start", "task_end", title= "Monthly - {} ".format( risk ))
		else:
			return old #ui_commons.get_graph_holder('pa-r1g3')

	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c4', 'children'), [Input('pa-filters-id', 'value'), Input( 'dbloader', 'value')], [State('pa-r1c4', 'children')]) 
	def update_graph4p(risk, n, old):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d = df["task_name"].value_counts()
			return ui_commons.get_Bar_Chart('pa-r1g4', d.index,  d, title= "Task Name - {} ".format( risk) ,	horizontal=True	)
		else:
			return old #ui_commons.get_graph_holder('pa-r1g4')
		
	
	@app.callback(Output('pa-r1c1', 'children'), [Input( 'dbloader', 'value')], [State('pa-r1c1', 'children')])
	def first_load_pa(n, old):
		db = model_commons.get_pa_data()
		if( len( db.index) > 0):
			d = db[model_commons.var_pa_risk_type].value_counts()
			return ui_commons.get_Bar_Chart('pa-r1g1', d.index,  d,  horizontal=True, title="All PA Tasks", marker=ui_commons.bar_color )
		else:
			return old
	
	#### ROW 2 PA
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r2c1', 'children'), [Input( 'dbloader', 'value')], [State('pa-r2c1', 'children')])
	def first_load_pa_r(n, old):
		db = model_commons.hget_pa_rates_cu()
		if( len( db.index) > 0):
			return ui_commons.hget_pa_Bar_Chart('pA-r2g1', db, horizontal=False, title="Number of PA Task Visits By CU - Excepted/Target Vs Actual Visits", marker=ui_commons.bar_color )
		else:
			return old	
	
	
	@app.callback( Output("pa-dated", 'children'), [Input( 'dbloader', 'value')], [State('pa-dated', 'children')]) 
	def update_dated(n, old):
		zstart, zend, ztot = model_commons.get_pa_duration_details()
		if zstart != 1990:
			return dht.P("From: {} To: {} = Month # {}".format(zstart, zend, ztot), className="card-title" ) 
		else:
		  return old
	