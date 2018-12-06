import dash
import dash_core_components as dcc
import dash_html_components as dht
from dash.dependencies import Output, Input, State  

from layouts.utils_commons import *

LAST_UPDATED = '123-232'
filter_options = ["The", "Quick", "Brown" , "Fox", "Jumped"]


def get_layout():
	return dht.Div(id='body-container', className="content-wrapper", children=[
		## filter row
		dht.Div(className="row", children=[ 
			dht.P( "Last Updated: {}".format( LAST_UPDATED ) ),
			get_Filter_Dropdown( filter_options, 'Referral Reason', 'filter-reasons') 
		]),
		
		## row card stats
		dht.Div(className="row", children=[ 
			dht.Div( className="col-xl-3 col-lg-3 col-md-3 col-sm-6 grid-margin stretch-card", children=[
				dht.Div( className="card card-statistics", children=[
					dht.Div( className="card-body", children=[
						dht.Div( className="clearfix", children=[
							dht.Div( className="float-right", children=[
								dht.P(id="card-label", className="mb-0 text-rigiht", value= "Avg.Sales" ), 
								dht.Div( className="fluid-container", children=[
									dht.H3(id="card-value", className="font-weight-medium text-right mb-0", value="KES 4, 354" )
								])
							])
						])
					])
				])
			] ) #for i in ["Goergie", "Podgie", "Yes, Papa", "Eating"] )
		
		]),
		
		
		## graphs row 1
		dht.Div(className="row", children=[
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r1c1', children=[
				get_graph_holder('g1r1')
			]),
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r1c2', children=[
				get_graph_holder('g2r1')
			]),
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r1c3', children=[
				get_graph_holder('g3r1')
			])
		]),
		
		## graphs row 2		
		dht.Div(className="row", children=[
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r2c1', children=[
				get_graph_holder('g1r2')
			]),
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r2c2', children=[
				get_graph_holder('g2r2')
			]),
			dht.Div(className="col-lg-4 grid-margin stretch-card", id='r2c3', children=[
				get_graph_holder('g3r2')
			])
		]),
		
		dht.Div(id='test')
		
	])	

		
## Callbacks for the graphs
#@app.callback(Output('test', 'children'),
#[Input('filter-reasons', 'value')])
def update_test(txt):
	return 'Selected: {}'.format( txt )

