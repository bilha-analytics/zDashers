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
				children=[ dht.P( "Select Risk Group: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					ui_commons.get_Filter_Dropdown( model_commons.get_pa_options_list(), 'PA Risk Group', 'pa-filters-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format(zstart, zend, ztot), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
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
				
	])	 



####
##
##
####
def register_callback(app): 
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-card-summaries', 'children'), [Input('pa-filters-id', 'value')])
	def update_cards(ref_reason): 		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if ref_reason == model_commons.var_all_reasons:
				df = db
			else:
				df = db[ db[model_commons.var_pa_risk_type] == ref_reason]
			
			t = model_commons.get_pa_pivot_summary( df )
			
			lt = len( db[model_commons.var_pa_risk_type].unique() )
			
			t2 = t.columns[lt:] if risk == model_commons.var_all_reasons else t.columns[1:]
			
			return [ ui_commons.make_stats_card(":{} - {}:".format(c, t[c][0]) , c) for c in t2]
		else:
			return []
			
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c2', 'children'), [Input('pa-filters-id', 'value')])
	def update_graph2p(risk):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d = df["high_risk"].value_counts()
			return get_Bar_Chart('pa-r1g2', d.index,  d, title= "Is High Risk - {} ".format( risk) , horizontal=True ) 	
		else:
			return ui_commons.get_graph_holder('pa-r1g2')
	
	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c3', 'children'), [Input('pa-filters-id', 'value')]) 
	def update_graph3p(risk):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d1 = df["Month"].groupby(df["Month"], sort=False).count()
			d2 = df["Month_end"].groupby(df["Month_end"], sort=False).count()
			return get_Line_Chart_2('pa-r1g3', d1.index, d2.index, d1, d2, "task_start", "task_end", title= "Monthly - {} ".format( risk ))
		else:
			return ui_commons.get_graph_holder('pa-r1g3')

	#####
    ## callbacks for card summaries
    #####
	@app.callback(Output('pa-r1c4', 'children'), [Input('pa-filters-id', 'value')]) 
	def update_graph4p(risk):		
		dbp = model_commons.get_pa_data()
		if( len( dbp.index) > 0):
			if risk == model_commons.var_all_reasons:
				df = dbp
			else:
				df = dbp[ dbp[model_commons.var_pa_risk_type] == risk]
			d = df["task_name"].value_counts()
			return get_Bar_Chart('pa-r1g4', d.index,  d, title= "Task Name - {} ".format( risk) ,	horizontal=True	)
		else:
			return ui_commons.get_graph_holder('pa-r1g4')
		
	#### ROW 2 PA
	#####
    ## callbacks for card summaries
    #####
