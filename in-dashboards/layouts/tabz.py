import dash
import dash_core_components as dcc
import dash_html_components as dht
from dash.dependencies import Output, Input, State  

import pandas as pd
import numpy as np

import plotly.graph_objs as go 
import plotly.plotly as ply
from plotly.tools import FigureFactory as FF
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot


## Text mining and visualization
#import pillow  #image reading 
from wordcloud import WordCloud
from wordcloud import STOPWORDS

#from data import dataset
from data.dataset import * 


def plot_counts_bar(df, t, col, order=None):
    ax = sns.countplot( data=df, x=col, order=order ) 
    # set % 
    total = len( df )
    for p in ax.patches:
        height = p.get_height()
        if not( pd.isnull(height) ):        
            ax.text(p.get_x()+p.get_width()/2.,
                height + 0.1,
                '{:1.0f} %'.format(height/total*100),
                ha="center") 

    ax.set_title( t ) ;  
    return ax

def plot_counts_pie(df, t, col, expl=(0.05, 0.05)):
    l = len( df[col].unique() )
    expl = expl if l == 2 else (0.05, 0.05, 0.05) #TODO refactor for i in l 
    ax = df[col].value_counts().plot(
        kind='pie', 
        title="First_Timers: "+t,
        autopct='%1.0f%%',
        startangle=90, 
        shadow=True, 
        explode=expl 
    )
    #draw circle to make donught 
    centre_circle = plt.Circle((-0.05,-0.05),0.60,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    # Equal aspect ratio ensures that pie is drawn as a circle
    ax.axis('equal')
    return ax
    #df["first_time_tester"].value_counts().plot(kind='pie', title="First_Timers: "+title, autopct='%1.1f%%', startangle=90, shadow=True, explode=(0, 0.1) )
    ''' 
    ''' 

def plot_bar_reasons(df, t):
    return plot_counts_bar(df, t, var_bucket_reasons , df[var_bucket_reasons].value_counts().index) 


def plot_bar_timing(df, t):
    return plot_counts_bar(df, t, "Day_of_Week", ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] ) 


def plot_word_cloud( df, t ):
    stop_words = STOPWORDS.union( ["CHV", "referral" ] )
    ## Translating Swahili to Eng 
    plt.imshow( WordCloud( 
        background_color="white", 
        max_font_size=50, 
        max_words=200, 
        stopwords=stop_words,
    ).generate( " ".join( df["reason_for_referral"].fillna('No_Input') ) ) , 
               interpolation='bilinear'  )
    plt.axis( "off" )
    plt.title( t )

def plot_timeline( df, t):
    myFmt = DateFormatter("%d-%b") 
    
    #ax = sns.countplot( data=df, x="Date of referral" ) 
    ax = df['reported_date'].value_counts().sort_index().plot.line()
    ax.set_title( t )  
    ax.xaxis_date( )
    ax.xaxis.set_minor_locator(WeekdayLocator(byweekday=MO)) ## intended 
    ax.xaxis.set_major_locator( WeekdayLocator(byweekday=MO, interval=1) ) ## only label at every 4th week 
    #ax.xaxis.set_major_formatter( DateFormatter('%H:%M:') )
    ax.xaxis.set_major_formatter( myFmt)
    return ax

### TBD refactor all below to

def plot_hivst_reasons(df, t):
    return plot_counts_bar(df[ df["reason_for_referral"] != 'HIVST_Assessed'], t, "reason_for_referral" ) 


def plot_hf_actvity( df, t):
    return plot_counts_bar(df, t, "health_facility")

def plot_hf_confirmation( df, t):
    return plot_counts_bar(df, t, "health_facility_confirmation") 

def plot_monthly_counts(df, t):
    return plot_counts_bar(df, t, "Month") 




## DIGITAL REFERRAL

### Filter
## Options List = referral reasons/types 
options_list = [ var_all_reasons] + db[var_bucket_reasons].unique().tolist()
#print( options_list )



### SUmmary table
t = pd.pivot_table( db, index=var_bucket_reasons, values=["referral_uuid"], aggfunc='count', margins=True ).T
t["Total Per. CHV"] = len( db ) / len( db.chv_name.unique() )
t[ "Total per CHV per Month"] = (len( db ) / len( db.chv_name.unique() ) )/ (db.reported_date.max().to_period('M') - db.reported_date.min().to_period('M'))
t['Number of CHVs'] =  len( db.chv_name.unique() ) 
t['Records per Client'] =  len(db)/len( db.patient_name.unique() )
t = t.round(2)


@interact( referral_reasons=options_list) #, style = {'description_width': 'initial'})
def plot_by( referral_reasons): 
    if( referral_reasons == var_all_reasons):
        df = db
    else:
        df = db[ db[var_bucket_reasons] == referral_reasons ] 
    
    title = '%s Reasons - %s'% (referral_reasons, LAST_UPDATED )
    title_all = '%s Reasons - %s'% (var_all_reasons, LAST_UPDATED )
    title_unsupported = '%s Reasons - %s'% (var_Unsupported, LAST_UPDATED )
    title_HIVST = '%s Reasons - %s'% (var_HIVST, LAST_UPDATED )
    title_timing = 'Timing: %s Referrals - %s' % (referral_reasons, LAST_UPDATED )

    #f, a = plt.subplots( nrows=2, ncols=3, figsize=(20,15) )

    plt.figure(figsize=(25,20))
    grid = plt.GridSpec( 3, 3, wspace=0.4, hspace=0.3)

    plt.subplot( grid[0,0] )
    plot_bar_reasons(db, title_all )

    plt.subplot( grid[0,1] )
    plot_bar_timing( df, title_timing)

    ## HF Activity 
    plt.subplot( grid[0,2] )
    plot_hf_actvity(df, "HF - "+title)

    ## unsupported cases only
    plt.subplot( grid[1,0] )
    plot_word_cloud( db[ (db[var_bucket_reasons]!= var_HIVST) & (db[var_bucket_reasons] == var_Unsupported)] , title_unsupported)

    ## word cloud all cases - no HIVST by default b/c not user entered 
    plt.subplot( grid[1,1] )
    plot_word_cloud( df if referral_reasons == var_HIVST else df[df[var_bucket_reasons]!=var_HIVST], "Referral Reasons - "+referral_reasons)
    #plot_word_cloud(db, 'Referral Reasons')
    
    ## confirmation - unconfirmed 
    plt.subplot( grid[1,2])
    #plot_counts_bar(df, "Unconfirmed - "+title, "unconfirmed")
    plot_counts_pie(df, "Unconfirmed - "+title, "unconfirmed")

    ## monthly counts
    plt.subplot( grid[2,0] )
    plot_monthly_counts( df, title_all)
    
    ## timeline
    plt.subplot( grid[2,1:] )
    plot_timeline( df, title_all)

    plt.tight_layout() 
        

#interact( plot_by, group_by=options_list);


#### HIVST
#display(HTML('<b>Number of records Table - HIVST Assessments</b>'))
t2 = pd.pivot_table( dbh, index="health_facility", values=["hivst_enrollment_uuid"], aggfunc='count', margins=True ).T
t2["Total Per. CHV"] = len( dbh ) / len( dbh.chv_name.unique() )
t2[ "Total per CHV per Month"] = (len( dbh ) / len( dbh.chv_name.unique() ) )/ (dbh.reported_date.max().to_period('M') - dbh.reported_date.min().to_period('M'))
t2['Number of CHVs'] =  len( dbh.chv_name.unique() ) 
t2['Records per Client'] =  len(dbh)/len( dbh.patient_name.unique() )
t2 = t2.round(2)


## options list = health facilities or CU
options_list = [var_all_reasons] + dbh.health_facility.unique().tolist()
var_bucket_unit = "health_facility"


@interact( unit=options_list) #, style = {'description_width': 'initial'})
def plot_by( unit ): 
    if( unit == var_all_reasons):
        df = dbh
    else:
        df = dbh[ dbh[var_bucket_unit] == unit ] 
    
    title = '%s - %s'% (unit, LAST_UPDATED_HIVST ) 
    title_timing = 'Timing: %s Referrals - %s' % (unit, LAST_UPDATED_HIVST )

    
    plt.figure(figsize=(25,15))
    grid = plt.GridSpec( 2, 3, wspace=0.4, hspace=0.3)

    ## HIVST assessment numbers
    plt.subplot( grid[0,0] )
    plot_bar_reasons(df, "Records: "+title )

    ## HIVST referral reasons 
    plt.subplot( grid[0,1] )
    plot_hivst_reasons( df, "Referral Reasons: "+title )

    ## HIVST assessment timing
    plt.subplot( grid[0,2] )    
    plot_bar_timing( df, title_timing)
    

    ## IPV cases 
    plt.subplot( grid[1,0] )
    plot_counts_pie(df, "IPV Cases: "+title, "is_ipv_case")

    ## completed enrollment - catchment issue 
    plt.subplot( grid[1,1] )    
    plot_counts_pie(df, "Catchment??: "+title, "completed_enrollment")
    
    ## require f/u OR eligible retake
    #plt.subplot( grid[1,2])
    #plot_counts_bar(df, "Eligble Retake: "+title, "eligible_for_retake")

    ## first timers
    plt.subplot( grid[1,2])
    plot_counts_pie(df, "First_Timers: "+title, "first_time_tester")
    
    plt.tight_layout() 
        

#interact( plot_by, group_by=options_list);















tab_cle_dash = [
	dht.Div(id='header', children=[
		dht.H6( "Last_Updated: {}".format( LAST_UPDATED ) ),
		
		dcc.Dropdown(
			id='filter',
			options = [{"label": i, "value": i} for i in options_list ],
			value = options_list[0]
		)
	]),
	
	dht.Div(id='body-tbl', children=[
		#print(t)
	]),
	
	dht.Div(id='body-gz', children=[ 		
		## Rows 1
		dht.Div(id='r1', children=[
			#c1
			dcc.Graph(id='g11',
				figure={
					'data':[
						go.Bar(
							x=db[var_bucket_reasons]
							y= db[
						)
					]
				}			
			),
			
			#c2
			dcc.Graph(id='g12'
				figure={
					'data':[
						go.Pie(
							labels=db["unconfirmed"]
							
						)
					]
				}
			),
			
			#c3 
			dcc.Graph(id='g13' )
		
		]),
		
		## Row 2
		dht.Div(id='r2', children=[
			#c1
			dcc.Graph(id='g21'),
			
			#c2
			dcc.Graph(id='g22'),
			
			#c3 
			dcc.Graph(id='g23' )
		
		]),
		
		
		## Row 3 
		dht.Div(id='r3', children=[
			#c1
			dcc.Graph(id='g31'),
			
			#c2:3
			dcc.Graph(id='g32' ),	
		
		])
	
	]),
	
	
	dht.Div(id='footer'),

]

@app.callback(
Output(),
[Input()]
)
def update_g1(filterz):
