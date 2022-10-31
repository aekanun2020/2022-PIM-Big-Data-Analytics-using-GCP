# -*- coding: utf-8 -*-
# import libraries needed for the code to run
import re
import pyspark as ps
from pyspark.ml import PipelineModel
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource


# create a Flask instance
# and create a Flask-RESTful API instance with the created Flask instance
app = Flask(__name__)
api = Api(app)

# create a SparkContext
# load saved pipeline model from the folder 'model'
sc = ps.SparkContext()
sqlContext = ps.sql.SQLContext(sc)
from pyspark.ml.pipeline import PipelineModel as pl
loaded_model = pl.load('./model')

# create a parser
# fill a parser with information about arguments 

parser = reqparse.RequestParser()


parser.add_argument('UniqueCarrier')
parser.add_argument('Origin')
parser.add_argument('Dest')
parser.add_argument('DepTime')
parser.add_argument('TaxiOut')
parser.add_argument('TaxiIn')
parser.add_argument('DepDelay')
parser.add_argument('LateAircraftDelay')
parser.add_argument('DayOfWeek')
parser.add_argument('Distance')

from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import col, udf

min_distance = 10
min_ArrDelay = 10
min_LateAircraftDelay = 10
max_distance = 100
max_ArrDelay = 100
max_LateAircraftDelay = 100

def t_timeperiod(origin):
    if origin is None:
        period = None
    elif origin > 0 and origin < 600:
        period = '00.01-05.59'
    elif origin >= 600 and origin < 1200:
        period = '06.00-11.59'
    elif origin >= 1200 and origin < 1800:
        period = '12.00-17.59'
    elif origin >= 1800 and origin <= 2400:
        period = '18.00-24.00'
    else:
        period = 'NA'
    return period
    
timeperiod = udf(lambda x: t_timeperiod(x),StringType())

def t_normalized_distance(origin):
    if origin is None:
        return None
    else:
        return ((origin-min_distance)/(max_distance-min_distance))

normalized_distance = udf(lambda x: t_normalized_distance(x),DoubleType())

def t_normalized_LateAircraftDelay(origin):
    if origin is None:
        return None
    else:
        return ((origin-min_LateAircraftDelay)/(max_ArrDelay-min_LateAircraftDelay))
        
normalized_LateAircraftDelay = udf(lambda x: t_normalized_LateAircraftDelay(x),DoubleType())
        

class PredictSentiment(Resource):
    def get(self):
    
    	# retrieve query text from API call
    	args = parser.parse_args()
    
    	UniqueCarrier = args['UniqueCarrier']
	Origin = args['Origin']
	Dest = args['Dest']
	DepTime = args['DepTime']
	TaxiOut = args['TaxiOut']
	TaxiIn = args['TaxiIn']
	DepDelay = args['DepDelay']
	LateAircraftDelay = args['LateAircraftDelay']
	DayOfWeek = args['DayOfWeek']
	Distance = args['Distance']

	#UniqueCarrier = 'WN'
	#Origin = 'IND'
	#Dest = 'BWI' 
	#DepTime = 1800
	#TaxiOut = 10
	#TaxiIn = 3
	#DepDelay = '34'
	#DayOfWeek = '4'
	#Distance = '1000'
	#LateAircraftDelay = '10'
	
	min_distance = 10
	min_ArrDelay = 10
	min_LateAircraftDelay = 10
	max_distance = 100
	max_ArrDelay = 100
	max_LateAircraftDelay = 100
	#user_text = args['query']
    	# create a dictionary with the retrieved query text
    	# and make a PySpark dataframe

	raw_dict = {'UniqueCarrier':UniqueCarrier, 'Origin':Origin, \
            'Dest':Dest,'DepTime':DepTime, 'TaxiOut':TaxiOut, 'TaxiIn':TaxiIn, 'DepDelay':DepDelay,
           'DayOfWeek':DayOfWeek, 'Distance':Distance, 'LateAircraftDelay':LateAircraftDelay}
    
    	raw_list = []
	raw_list.append(raw_dict)
	
	airline_row_df = sc.parallelize(raw_list).toDF()
	
	from pyspark.sql.types import StringType, DoubleType
	from pyspark.sql.functions import col, udf

	crunched_df = airline_row_df.withColumn('DepTime',airline_row_df['DepTime'].cast(DoubleType())).\
	withColumn('TaxiOut',airline_row_df['TaxiOut'].cast(DoubleType())).\
	withColumn('TaxiIn',airline_row_df['TaxiIn'].cast(DoubleType())).\
	withColumn('DepDelay',airline_row_df['DepDelay'].cast(DoubleType())).\
	withColumn('DayOfWeek',airline_row_df['DayOfWeek'].cast(DoubleType())).\
	withColumn('Distance',airline_row_df['Distance'].cast(DoubleType())).\
	withColumn('LateAircraftDelay',airline_row_df['LateAircraftDelay'].cast(DoubleType()))
	
	discretized_df = crunched_df.withColumn('DepTime',timeperiod(crunched_df['DepTime']))
	
	normalized_df = discretized_df.withColumn('Distance', normalized_distance(discretized_df['Distance'])).\
	withColumn('LateAircraftDelay', normalized_LateAircraftDelay(discretized_df['LateAircraftDelay']))
	
	final_df = normalized_df.dropna()
	result_df = loaded_model.transform(final_df.dropna())
	result = (result_df.take(1)[0]['prediction']*(max_ArrDelay-min_ArrDelay))+min_ArrDelay
	
	from pyspark.sql.functions import col
	from pyspark.sql.types import StringType, DoubleType
        return result


# Setup the Api resource routing
# Route the URL to the resource
api.add_resource(PredictSentiment, '/')


if __name__ == '__main__':
    # run the Flask RESTful API, make the server publicly available (host='0.0.0.0') on port 8080
    app.run(host='0.0.0.0', port=8080, debug=True)
