import dash
import dash_core_components as dcc
import dash_html_components as dht

import plotly.plotly as ply
import plotly.graph_objs as go 


from wordcloud import WordCloud
from wordcloud import STOPWORDS  

import os, base64


## TODO: sanitize and checks and controls 

def get_Filter_Dropdown(options_list, label, id, multiselect=False):
	return dcc.Dropdown( className="card-body col-sm-9", 
		id=id,
		options = [{"label" : i , "value": i} for i in options_list ],
		value = options_list[0], 
		#mutli = multiselect
	)
	
def make_Stats_Card(label, value):
	return dht.Div( className="card card-statistics", children=[
				dht.Div( className="card-body", children=[
					dht.Div( className="clearfix", children=[
						dht.Div( className="float-right", children=[
							dht.P(className="mb-0 text-right", children= [ label ] ), 
							dht.Div( className="fluid-container", children=[
								dht.H3(className="font-weight-medium text-right mb-0", children=[ value ] )
							])
						])
					])
				])
			])
	

def get_graph_holder(id):
	return dcc.Graph(id=id, className="card" ) 
	

def get_Pie_Chart(id, labels, values, title=None):
	return dcc.Graph(id=id, className="card",
		figure = {
			'data': [
				{
					'labels': labels,
					'values': values,
					'type' : 'pie',	
					'hoverinfo':'label+percent+value',
					'textinfo':'value' , 
					'hole': 0.4
					
				}
			],
			
			'layout' : {
				'title': title,
				'showlegend': True,
				'font': {"family":'Arial', "size":10}
			}
		}		
	)
	

defaultMarker = {'color': 'rgba(112, 75, 105, 0.9)' }
	
def get_Bar_Chart(id, x, y, title=None, horizontal=False, marker=defaultMarker):

	pct = ( ((y/y.sum()*100).round(0)).astype(int) ).astype(str)+"%"
	
	return dcc.Graph(id=id, className="card",
		figure = {
			'data': [
				{
					'x': y if horizontal else x,
					'y': x if horizontal else y,
					'text': pct, 
					'type' : 'bar',	
					'orientation': 'h' if horizontal else 'v', 
					'textposition': 'auto', 
					'marker': marker
				}
			],
			
			'layout' : {
				'title': title,
				'font': {"family":'Arial', "size":10}
			}
		}		
	)
	

def plot_word_cloud( col, cname ):
	global a
	#fname = os.getcwd() + "/output/ref-reasons-cloud.png"
	fname = "output/ref-reasons-cloud-{}.png".format( cname )
		
	stop_words = STOPWORDS.union( ["CHV", "referral" ] )
    ## Translating Swahili to Eng 	
	
	inputwords = " ".join( col.fillna('No_Input') )
	
	w = WordCloud( width=800, height=600,
        background_color="white", 
        max_font_size=60, 
        max_words=300, 
        stopwords=stop_words,
		).generate( inputwords if inputwords else 'No_Input' ) 
	
	w.to_file( fname )
	
	#return base64.b64encode(  open(fname, 'rb').read() ) #w.to_image() )
	
	return fname
	
	
	
def get_table(df, max_rows=10):
	return dht.Table( className="card",children=
		#header
		[dht.Tr([dht.Th(c) for c in df.columns ] ) ] +  
		
		#body
		[ dht.Tr([
			dht.Td( df.icol[i][c] ) for c in df.columns
		]) for i in range(min(len(df), max_rows))]
	
	)