import dash
import dash_core_components as dcc 
import dash_html_components as dht 
from dash.dependencies import Input, Output

from applayouts import digital_referral, hivst, predictives, wordcloud_referral
from appmodel import model_commons  
from appmodel.utils import lazy_logger 

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
        dht.Div(id='logg', className="", children=[ dht.P(".|.-... Almost there, loadding ...-.|. ")]), 

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
	]),
    dcc.Interval(id='dbloader', interval=5000, n_intervals=0, max_intervals=1) ,
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
####
##
##
####
@app.callback(
	Output('logg', 'children'), 
	[Input('dbloader', 'value') ]
)
def loaddbs(n): 
    model_commons.get_cle_data() 
    model_commons.get_clh_data()
    model_commons.get_pa_data()
    model_commons.get_pa_risks_data()

    return []
####
##
##
####
digital_referral.register_callback( app ) 
hivst.register_callback( app ) 
predictives.register_callback( app ) 
wordcloud_referral.register_callback( app ) 


####
##
##
####
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
#app.css.append_css({"external_url": "https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css"}) 



####
##
##
####
if __name__ == "__main__":
    lazy_logger('main', "starting")
    app.run_server(debug=True) 