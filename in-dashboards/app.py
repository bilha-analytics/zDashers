import os
import dash, flask 
import dash_core_components as dcc
import dash_html_components as dht
from dash.dependencies import Output, Input, State  

import pandas as pd

#from layouts import digital_referral 

from layouts.utils_commons import *
from data.datasets import *

## offline mode TODO
#import plotly.offline as offline 
#offline.init_notebook_mode()


'''
Tabbed layout; a tab for each project/treatment and general Ogembo as control 

'''
CLE = "cle"
CLH = "clh"
PA = "pa"
BRC = "branch"
WC = "referral_text"

the_tabz = {
	CLE : { 
		"title": "Digital Referral", 
		"body" : "SOmething something digital referral",
		"dataset" : "some-url"
	},
	CLH : { 
		"title": "HIVST", 
		"body" : "SOmething something HIVST",
		"dataset" : "some-url"
	},
	
	WC : {
		"title" : "Referral Reasons Text",
		"body" : "here",
		"dataset" : 'someurl'
	
	}, 
	PA : { 
		"title": "Predictive Algorithms", 
		"body" : "Coming Soon...",
		"dataset" : "some-url"
	},
	BRC : { 
		"title": "Ogembo Branch", 
		"body" : "Coming Soon...",
		"dataset" : "some-url"
	},
}


'''
MAIN APP
'''

app = dash.Dash(__name__)
server = app.server 

app.config['suppress_callback_exceptions']=True
app.title = "IN Initiatives Monitor"


app.layout = dht.Div(className="container-scroller", children=[
	dht.Div( className="navbar default-layout col-lg-12 col-12 p-0 fixed-top d-flex flex-row", children=[
		dht.H1('IN Initiatives Monitor')
	]),
	dht.Div(className="container-fluid page-body-wrapper", style={'width':'100%'},
		children=[
		#sidebar
		#dht.Div(className="sidebar sidebar-offcanvas", id="sidebar", children=[
		#]),
		
		#main content 
		dht.Div( className="main-panel col-12", children=[
			dcc.Tabs( 
				id="in-tabs",  
				value= CLE,
				parent_className='custom-tabs',
				className='custom-tabs-container', 
				children = [ 
					dcc.Tab( 
						label= the_tabz[tab_i]['title'], 
						value = tab_i , 				
						className='custom-tab',
						selected_className='custom-tab--selected'
					) for tab_i in the_tabz.keys()
				]
			), 
			
			
			dht.Div(id='tab-body', className="content-wrapper", style={'width':'100%'}) 
		])
	])
])

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
#app.css.append_css({"external_url": "https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css"}) 

@app.callback(
	Output('tab-body', 'children'), 
	[Input('in-tabs', 'value') ]
)
def display_value( tab ):
	if tab == CLE:
		return get_layout_cle()
	elif tab == CLH:
		return get_layout_clh()
	elif tab == PA:
		return get_layout_pa() #dht.H4( the_tabz[tab]['body'] )
	elif tab == BRC:
		return dht.H4( the_tabz[tab]['body'] )
	elif tab == WC:
		return layout_wordcloud() 
	else:
		return dht.P(className="bg-warning text-white" ,children=[ "Unknown Request or Page Not Found" ])
		
		
		


'''
DIgital Referral Tab/View

'''
bar_color = { 'color': ['rgba(50, 171, 96, 0.8)', 'rgba(219, 64, 82, 0.8)', 'rgba(90,90,90,0.8)'] } 

cards = []
t = get_pivot_summary_referrals(db)
lt = len( db[var_bucket_reasons].unique())
for i in t.columns[lt:] : 
	cards.append( make_Stats_Card(i, t[i][0] ) ) 


def get_layout_cle():
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row form-group", children=[ 
			dht.Div( className="col-sm-2 col-form-label", 
				children=[ dht.P( "Select Referral Reason: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					get_Filter_Dropdown( options_reasons, 'Referral Reason', 'filter-reasons-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format( STARTED_CLE, LAST_UPDATED, DURATION_CLE ), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
			dht.Div( id="card-summaries", className="col-xl-2 col-lg-2 col-md-2 col-sm-2 grid-margin stretch-card", children=cards
			)
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c1', children=[
				get_Bar_Chart('g1r1', db[var_bucket_reasons].value_counts().index,  db[var_bucket_reasons].value_counts(),  horizontal=True, title="All Referrals", marker=bar_color )
			]),
			
			## by facility 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c2', children=[
				get_Bar_Chart('g2r1', db["health_facility"].value_counts().index,  db["health_facility"].value_counts(), horizontal=True )
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c3', children=[
				get_Line_Chart('g3r1', db["Month"].value_counts().index,  db["Month"].value_counts(), horizontal=False)
			]),
			
			## by confirmation of referral 
			dht.Div(className="col-lg-3 stretch-card card", id='r1c4', children=[
				get_Pie_Chart('g4r1', db["health_facility_confirmation"].value_counts().index,  db["health_facility_confirmation"].value_counts())
			]),
		]),
		
		
	])	
	
	
## callbacks for card summaries
@app.callback(Output('card-summaries', 'children'),
[Input('filter-reasons-id', 'value')]) 
def update_cards(ref_reason):
	if ref_reason == var_all_reasons:
		df = db
	else:
		df = db[ db[var_bucket_reasons] == ref_reason]
	
	t = get_pivot_summary_referrals( df)
	
	lt = len( db[var_bucket_reasons].unique())
			
	t2 = t.columns[lt:] if ref_reason == var_all_reasons else t.columns[1:]
	
	cards = []
	for i in t2 :
		cards.append( make_Stats_Card(i, t[i][0] ) ) 
		
	return cards 
		
## Callbacks for the graphs
@app.callback(Output('r1c2', 'children'),
[Input('filter-reasons-id', 'value')]) 
def update_graph1(ref_reason):
	if ref_reason == var_all_reasons:
		df = db
	else:
		df = db[ db[var_bucket_reasons] == ref_reason]
	
	d = df["health_facility"].value_counts()
	
	return get_Bar_Chart('g2r1', 
		d.index,  
		d,
		title= "By Facility - {} ".format( ref_reason) ,
		horizontal=True	
	)

@app.callback(Output('r1c3', 'children'),
[Input('filter-reasons-id', 'value')]) 
def update_graph2(ref_reason):
	if ref_reason == var_all_reasons:
		df = db
	else:
		df = db[ db[var_bucket_reasons] == ref_reason]
	
	#df.sort_values( by='reported_date', inplace=True)
	
	#d = df["Month"].value_counts().sort_index()#.sort_index()
	d = df["Month"].groupby(df["Month"], sort=False).count()
	
	return get_Line_Chart('g3r1', 
		d.index,  
		d,
		title= "Monthly - {} ".format( ref_reason )
	)

@app.callback(Output('r1c4', 'children'),
[Input('filter-reasons-id', 'value')]) 
def update_graph3(ref_reason):
	if ref_reason == var_all_reasons:
		df = db
	else:
		df = db[ db[var_bucket_reasons] == ref_reason]
		
	d = df["health_facility_confirmation"].value_counts() 
	
	return get_Pie_Chart('g4r1', 
		d.index,  
		d,
		title = "Confirmation - {}".format(ref_reason )
	)
		

		

'''
HIVST Tab/View

'''

cards2 = []
t2 = get_pivot_summary_hivst(dbh)
for i in t2.columns[3:] :
	cards2.append( make_Stats_Card(i, t2[i][0] ) ) 


def get_layout_clh():
	return dht.Div(id='clh-body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row", children=[ 
			dht.Div( className="col-sm-2 col-form-label",  
				children=[ dht.P( "Select Health Facility: ", className="card-title")
			]), 
			dht.Div( className="col-sm-6", 
				children=[ 
				get_Filter_Dropdown( options_facility, 'Health Facility', 'filter-unit-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format( STARTED_HIVST, LAST_UPDATED_HIVST, DURATION_CLH ), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
			dht.Div( id="cards-clh", className="col-xl-2 col-lg-2 col-md-2 col-sm-2 grid-margin stretch-card", children=cards2
			)
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by  all HIVST activity 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c1', children=[
				get_Bar_Chart('clh-g1r1', dbh[var_bucket_reasons].value_counts().index,  dbh[var_bucket_reasons].value_counts(),  horizontal=True, title="All HIVST Assessments", marker=bar_color )
				#get_Pie_Chart('clh-g1r1', dbh[var_bucket_reasons].value_counts().index,  dbh[var_bucket_reasons].value_counts(), title="All HIVST Assessments")
			]),
			
			## by referral reason  
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c2', children=[
				get_Bar_Chart('clh-g2r1', dbh["reason_for_referral"].value_counts().index,  dbh["reason_for_referral"].value_counts(), horizontal=True)
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c3', children=[
				get_Line_Chart('clh-g3r1', dbh["Month"].value_counts().index,  dbh["Month"].value_counts(), horizontal=False)
			]),
			
			## by confirmation of referral 
			dht.Div(className="col-lg-3 stretch-card card", id='clh-r1c4', children=[
				get_Pie_Chart('clh-g4r1', dbh["health_facility_confirmation"].value_counts().index,  dbh["health_facility_confirmation"].value_counts())
			]),
		]),
		
		
	])	

## callbacks for card summaries
@app.callback(Output('cards-clh', 'children'),
[Input('filter-unit-id', 'value')]) 
def update_cards_clh(ref_reason):
	if ref_reason == var_all_reasons:
		df = dbh
	else:
		df = dbh[ dbh[var_bucket_unit] == ref_reason]
	
	t = get_pivot_summary_hivst( df )
	
	t2 = t.columns[3:] if ref_reason == var_all_reasons else t.columns[1:] 
	
	cards = []
	for i in t2 :
		cards.append( make_Stats_Card(i, t[i][0] ) ) 
		
	return cards 
	
## Callbacks for the graphs
@app.callback(Output('clh-r1c2', 'children'),
[Input('filter-unit-id', 'value')]) 
def update_graph1_clh(ref_reason):
	if ref_reason == var_all_reasons:
		df = dbh
	else:
		df = dbh[ dbh[var_bucket_unit] == ref_reason]

	d = df[ df["reason_for_referral"] != 'HIVST_Assessed' ]["reason_for_referral"].value_counts()
	
	return get_Bar_Chart('clh-g2r1', 
		d.index,  
		d,
		title= "HIVST Referral Reasons - {} ".format( ref_reason ),
		horizontal=True 
	)

@app.callback(Output('clh-r1c3', 'children'),
[Input('filter-unit-id', 'value')]) 
def update_graph2_clh(ref_reason):
	if ref_reason == var_all_reasons:
		df = dbh
	else:
		df = dbh[ dbh[var_bucket_unit] == ref_reason]
	
	#df.sort_values( by='reported_date', inplace=True)
	
	d = df["Month"].groupby(df["Month"], sort=False).count()
	
	return get_Line_Chart('clh-g3r1', 
		d.index,  
		d, 
		title= "Monthly - {} ".format( ref_reason )
	)

@app.callback(Output('clh-r1c4', 'children'),
[Input('filter-unit-id', 'value')]) 
def update_graph3_clh(ref_reason):
	if ref_reason == var_all_reasons:
		df = dbh
	else:
		df = dbh[ dbh[var_bucket_unit] == ref_reason]
	
	d = df["health_facility_confirmation"].value_counts() 
	
	return get_Pie_Chart('clh-g4r1', 
	d.index,  
	d,
	title = "Confirmation - {}".format(ref_reason )
	)



'''
PA 

''' 

cards3 = []
t2 = get_pivot_summary_PA( dbp )
lt = len( dbp[var_pa_risk_type].unique())
for i in t.columns[lt:] : 
	cards.append( make_Stats_Card(i, t[i][0] ) ) 

def get_layout_pa():
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row form-group", children=[ 
			dht.Div( className="col-sm-2 col-form-label", 
				children=[ dht.P( "Select Risk Type: ", className="card-title")
			]),
			dht.Div( className="col-sm-6", 
				children=[ 
					get_Filter_Dropdown( options_pa, 'Risk Type', 'filter-risk-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format( STARTED_PA, LAST_UPDATED, DURATION_PA ), className="card-title" ) 
				]),
			
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
			dht.Div( id="card-summaries-pa", className="col-xl-2 col-lg-2 col-md-2 col-sm-2 grid-margin stretch-card", children=cards3
			)
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			## by reasons all 
			dht.Div(className="col-lg-3 stretch-card card", id='pr1c1', children=[
				get_Bar_Chart('pg1r1', dbp[var_pa_risk_type].value_counts().index,  dbp[var_pa_risk_type].value_counts(),  horizontal=True, title="All PA Tasks", marker=bar_color )
			]),
			
			## by high risk 
			dht.Div(className="col-lg-3 stretch-card card", id='pr1c2', children=[
				get_Bar_Chart('pg2r1', dbp["high_risk"].value_counts().index,  dbp["high_risk"].value_counts(), horizontal=True,  title="Is High Risk" )
			]),
			
			## by month 
			dht.Div(className="col-lg-3 stretch-card card", id='pr1c3', children=[
				get_Line_Chart('pg3r1', dbp["Month"].value_counts().index,  dbp["Month"].value_counts(), horizontal=False)
			]),
			
			## by task name 
			dht.Div(className="col-lg-3 stretch-card card", id='pr1c4', children=[
				get_Bar_Chart('pg4r1', dbp["task_name"].value_counts().index,  dbp["task_name"].value_counts(), horizontal=True,  title="Task Names" )
			]),
		]),
		
		
	])	
	

## callbacks for card summaries
@app.callback(Output('card-summaries-pa', 'children'),
[Input('filter-risk-id', 'value')]) 
def update_cards_pa(risk):
	if risk == var_all_reasons:
		df = dbp
	else:
		df = dbp[ dbp[var_pa_risk_type] == risk]
	
	t = get_pivot_summary_PA( df )	
	lt = len( dbp[var_pa_risk_type].unique())			
	t2 = t.columns[lt:] if risk == var_all_reasons else t.columns[1:] 
	cards = []
	for i in t2 :
		cards.append( make_Stats_Card(i, t[i][0] ) ) 
		
	return cards 
		
## Callbacks for the graphs
@app.callback(Output('pr1c2', 'children'),
[Input('filter-risk-id', 'value')]) 
def update_graph1p(risk):
	if risk == var_all_reasons:
		df = dbp
	else:
		df = dbp[ dbp[var_pa_risk_type] == risk]
	
	d = df["high_risk"].value_counts()
	
	return get_Bar_Chart('pg2r1', 
		d.index,  
		d,
		title= "Is High Risk - {} ".format( risk) ,
		horizontal=True	
	)

@app.callback(Output('pr1c3', 'children'),
[Input('filter-risk-id', 'value')]) 
def update_graph2p(risk):
	if risk == var_all_reasons:
		df = dbp
	else:
		df = dbp[ dbp[var_pa_risk_type] == risk]
	
	d1 = df["Month"].groupby(df["Month"], sort=False).count()
	d2 = df["Month_end"].groupby(df["Month_end"], sort=False).count()
	
	return get_Line_Chart_2('pg3r1', 
		d1.index, d2.index,   
		d1, d2, 
		"task_start", "task_end", 
		title= "Monthly - {} ".format( risk )
	)

## Callbacks for the graphs
@app.callback(Output('pr1c4', 'children'),
[Input('filter-risk-id', 'value')]) 
def update_graph1p(risk):
	if risk == var_all_reasons:
		df = dbp
	else:
		df = dbp[ dbp[var_pa_risk_type] == risk]
	
	d = df["task_name"].value_counts()
	
	return get_Bar_Chart('pg4r1', 
		d.index,  
		d,
		title= "Task Name - {} ".format( risk) ,
		horizontal=True	
	)
	


'''
Word Clouds
'''

def layout_wordcloud():
	return dht.Div(id='wc-body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row", children=[ 
			dht.Div( className="col-sm-2 col-form-label",  
				children=[ dht.P( "Select Referral Reason: ", className="card-title")
			]), 
			dht.Div( className="col-sm-6", 
				children=[ 
				get_Filter_Dropdown( options_reasons, 'Referral Reason', 'filter-reason-wc-id') 
			]), 
			dht.Div( className="col-sm-4", 
				children=[ dht.P("From: {} To: {} = {} months".format( STARTED_CLE, LAST_UPDATED, DURATION_CLE ), className="card-title" ) 
				]),
			
		]),
		
		## row word clouds 
		dht.Div(className="row", children=[ 
		
			dht.Div(className="col-12 stretch-card card", id='wc1',  children=[
				#dht.Img( src='data:image/png;base64,{}'.format( plot_word_cloud( db[ db[var_bucket_reasons] != var_HIVST ]["reason_for_referral"] ) ), id='wc1img' )
				dht.Img( src=plot_word_cloud( db[ db[var_bucket_reasons] != var_HIVST ]["reason_for_referral"] , var_all_reasons), id='wc1img', width="80%", height="80%" )
			]),
			
		]),
				
	])
@app.callback(Output('wc1img', 'src'),
[Input('filter-reason-wc-id', 'value')]) 
def update_wordcloud(ref_reason):
	df = db if ref_reason == var_HIVST else db[ db[var_bucket_reasons] != var_HIVST ]
	if ref_reason == var_all_reasons:
		df = df
	else:
		df = df[ df[var_bucket_reasons] == ref_reason]
		
	#return 'data:image/png;base64,{}'.format( plot_word_cloud( df["reason_for_referral"] ) )
	return plot_word_cloud( df["reason_for_referral"] , ref_reason) 

STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
@app.server.route('/output/<resource>' )
def serve_image(resource): 
    return flask.send_from_directory(STATIC_PATH, resource) 

if __name__ == '__main__':
	app.run_server( debug=True ) 
	