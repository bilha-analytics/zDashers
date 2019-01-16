import dash, flask
import dash_core_components as dcc 
import dash_html_components as dht 
from dash.dependencies import Input, Output

from applayouts import digital_referral, hivst, predictives, wordcloud_referral
from applayouts.wordcloud_referral import *
from appmodel import model_commons 
from appmodel.model_commons import * 
from appmodel.utils import lazy_logger 

'''
import os, sys 
from io import StringIO
import boto3
'''

''' 
import time 
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from flask_caching import Cache 
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
		"dataset" : "db_referrals"
	},
	CLH : { 
		"title": "HIVST", 
		"body" : "SOmething something HIVST",
		"dataset" : "db_hivst"
	},
	
	WC : {
		"title" : "Referral Reasons Text",
		"body" : "here",
		"dataset" : 'db_referrals' 	
	}, 
	PA : { 
		"title": "Predictive Algorithms", 
		"body" : "Coming Soon...",
		"dataset" : "db_pa"
	},
	BRC : { 
		"title": "Ogembo Branch", 
		"body" : "Coming Soon...",
		"dataset" : "some-url"
	},
}

#ext_css =['https://codepen.io/chriddyp/pen/bWLwgP.css'] 

lazy_logger("main start", ">>>> LOADING CONFIG") 
model_commons.config_load()

####
##
##
####
app = dash.Dash( __name__) #, external_stylesheets=ext_css) 
server = app.server 

app.title = "IN Initiatives Monitor"
app.config['suppress_callback_exceptions']=True


####
##
##
####
app.layout = dht.Div(className="container-scroller", children=[
	dht.Div( className="navbar default-layout col-lg-12 col-12 p-0 fixed-top d-flex flex-row", children=[
		dht.H1('IN Initiatives Monitor')
	]),

	dht.Div(className="container-fluid page-body-wrapper", style={'width':'100%'},
		children=[
        # logger
        #dht.Div(id='logg', className="", children=[ dht.H1(".|.... Almost there, loading ....|. ")] ), 

		#main content 
		dht.Div( className="main-panel col-12", id="main-bodyz", children = [ 
			dht.Div( className="row align-items-center d-flex flex-row card-info btn-info", 
				children=[dht.H1("..\... Almost there, loading .../.. may take a minute ", id='logg',) ] )
			])
	]),
    dcc.Interval(id='dbloader', interval=0, n_intervals=0, max_intervals=1) ,
    dcc.Interval(id='dbloader1', interval=(3*60*60*1000) ), # n_intervals=0, max_intervals=1) ,
	dcc.Interval(id='logger', interval=3000) #, n_intervals=0 ) #, max_intervals=100) 
	#dcc.Interval(id='fetcher', interval=(3*60*60*1000) ) #, n_intervals=0 ) #, max_intervals=100) 
])



####
##
##
####
@app.callback(
	Output('tab-body', 'children'), 
	[Input('in-tabs', 'value') ]
)
def display_value( tab ):
	if tab == CLE:
		return digital_referral.get_layout()
	elif tab == CLH:
		return hivst.get_layout()
	elif tab == PA:
		return predictives.get_layout()
	elif tab == BRC:
		return dht.H4( the_tabz[tab]['body'] )
	elif tab == WC:
		return wordcloud_referral.get_layout() 
	else:
		return dht.P(className="bg-warning text-white" ,children=[ "Unknown Request or Page Not Found" ])

@app.callback(Output('logg', 'style'), [Input('logger', 'value') ])
def blink_loader(n):
	lazy_logger( "blink_loader", "interval = {}".format( n ) ) 
	return {"color":"yellow"} if (n is not None) and (int(n) % 2 ==0) else {"color":"white"}

#@app.callback(Output('logger', 'value'), [Input('dbloader', 'value') ])
#def blink_loader(n):
	
####
##
##
####
@app.callback(Output('main-bodyz', 'children'), [Input('dbloader', 'value') ])
def loaddbs(n):
	model_commons.load_data_from_files(dird="dat")
	#model_commons.get_cle_data() 
	#model_commons.get_clh_data()
	#model_commons.get_pa_data()
	#model_commons.get_pa_risks_data()
	return [
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
		]


#@app.callback( Output(), [Input('fetcher', 'value')]):

####
##
##
####
digital_referral.register_callback( app ) 
hivst.register_callback( app ) 
predictives.register_callback( app ) 
wordcloud_referral.register_callback( app ) 

#STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
#@app.server.route('/output/<resource>' )
#def serve_image(resource): 
#	return flask.send_from_directory(STATIC_PATH, resource) 
####
##
##
####
#@app.server.route('/dat/<resource>')
#def fetch_csv():
#	S3_BUCKEET = StringIO(str(os.environ.get("S3_BUCKET"))).getvalue()


####
##
##
####
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
#app.css.append_css({"external_url": "https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css"}) 
'''
## p in seconds 
cache = Cache( app.server, config={
	'CACHE_TYPE': 'filesytem',
	'CACHE_DIR':'cache-directory'
})
TIMEOUT = 60
@cache.memoize(timeout=TIMEOUT)
def get_new_data_every( p=5*60*60):
	## update data evey p time period
	#p = 3 in seconds 
	#while True:
	lazy_logger("THREAD::get_new_data_every", "START cycle")
	model_commons.fetch_data_to_ROM()
	lazy_logger("THREAD::get_new_data_every", "FIN cyle")
	#time.sleep( p )

#get_new_data_every(1)

## Thread pull data
#exect = ThreadPoolExecutor(max_workers=1)
#exect.submit( get_new_data_every ) 
''' 
if __name__ == "__main__":
    lazy_logger('main', "starting")
    app.run_server(debug=False) 