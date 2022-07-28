from glob import glob
from itertools import count
import json
from operator import truediv
from re import I
from sqlite3 import Date
from numpy import append
import pandas as pd
import asyncio
import threading
from time import sleep
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from elasticsearch import Elasticsearch
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
print("-------- APPLICATION START PRIMA ATT -----------")


#create elasticsearch index
ADDRESS = "http://elasticsearch:9200"
ELASTICINDEXBESTDAYS = "bestdays"
ELASTICINDEXGOODDAYS = "gooddays"
ELASTICINDEXWORSTDAYS = "worstdays"


temperatureList = []
precipitationList = []

bestList = []
goodList = []
worstList = []

daysList = []

def dayMap(day):
    if day == 0:
        return "Lunedi"
    if day == 1:
        return "Martedi"
    if day == 2:
        return "Mercoledi"
    if day == 3:
        return "Giovedi"
    if day == 4:
        return "Venerdi"
    if day == 5:
        return "Sabato"
    if day == 6:
        return "Domenica"
   
    


def function():
    global temperatureList
    global precipitationList
    global bestList
    global goodList
    global worstList
    global daysList
    #vertex clustering (per document)
    d = {"temperatureList": temperatureList, "precipitationList":precipitationList}
    df = pd.DataFrame(d)
    dataframe = spark.createDataFrame(df)
    vecAssembler = VectorAssembler(inputCols=["temperatureList","precipitationList"], outputCol="features")
    new_df = vecAssembler.transform(dataframe)
    kmeans = KMeans(k=2, seed=1)  
    model = kmeans.fit(new_df.select('features'))
    predictions = model.transform(new_df)
    #predictions = predictions.groupBy("prediction").count().withColumnRenamed("count", "count")
    #print(predictions)
    predictions.show()
    temperatureList.clear()
    precipitationList.clear()
    pandasDF = predictions.toPandas()
    #print(pandasDF)
    for x in range(2, 10):
        daysList.append(pandasDF.iloc[[x]])

    predictionT = pandasDF.iloc[[0]].iloc[0,3]
    for x in range(0, 8):
        #print((datetime.date.today() + datetime.timedelta(days=x)).weekday())
        if x <= 5:
            if daysList[x].iloc[0,3] == predictionT and daysList[x+1].iloc[0,3] == predictionT and daysList[x+2].iloc[0,3] == predictionT:
                bestList.append(str(str(dayMap((datetime.date.today() + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))
            
            elif daysList[x].iloc[0,3] == predictionT and daysList[x+1].iloc[0,3] == predictionT:
                goodList.append(str(str(dayMap((datetime.date.today() + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))
            
            elif daysList[x].iloc[0,3] == predictionT:
                worstList.append(str(str(dayMap((datetime.date.today()  + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))
        
        elif x <= 6:
            if daysList[x].iloc[0,3] == predictionT and daysList[x+1].iloc[0,3] == predictionT:
                goodList.append(str(str(dayMap((datetime.date.today() + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))
            
            elif daysList[x].iloc[0,3] == predictionT:
                worstList.append(str(str(dayMap((datetime.date.today()  + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))
        else:
            if daysList[x].iloc[0,3] == predictionT:
                worstList.append(str(str(dayMap((datetime.date.today()  + datetime.timedelta(days=x)).weekday())) + " - " + str(datetime.date.today() + datetime.timedelta(days=x))))

    if not bestList:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : "Nessun giorno molto consigliato trovato"})
    else:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : bestList})
    #print(data)
    resp = es.index(index=ELASTICINDEXBESTDAYS, document=data)
    print(resp['result'])
    es.indices.refresh(index=ELASTICINDEXBESTDAYS)  

    if not goodList:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : "Nessun giorno consigliato trovato"})
    else:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : goodList})
    #print(data)
    resp = es.index(index=ELASTICINDEXGOODDAYS, document=data)
    print(resp['result'])
    es.indices.refresh(index=ELASTICINDEXGOODDAYS)

    if not worstList:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : "Nessun giorno sconsigliato trovato"})
    else:
        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"data" : worstList})
    #print(data)
    resp = es.index(index=ELASTICINDEXWORSTDAYS, document=data)
    print(resp['result'])
    es.indices.refresh(index=ELASTICINDEXWORSTDAYS)

    bestList.clear()
    goodList.clear()
    worstList.clear()
               
                  

def elaborate(batch_df: DataFrame, batch_id: int):
    print("-----------------ELABORATE FUNCTION START------------------")
    pandasDF = batch_df.select("value").toPandas()
    print("PandasDF created -----------------  ")
    if(pandasDF.empty == False):
        global temperatureList
        global precipitationList
        
        #print("pandasDF: ")
        #print(pandasDF)
        row = pandasDF.iloc[[0]]
        #print("row: ")
        #print(row)
        payload = row.iloc[0,0]
        #print("!!!!! payload: ")
        #print(payload)
        data = json.loads(payload)
        temperature = 60
        precipitations = 20
        temperatureList.append(temperature)
        precipitationList.append(precipitations)

        temperature = 10
        precipitations = 100
        temperatureList.append(temperature)
        precipitationList.append(precipitations)
    
        averageTemp = 0
        averagePrecipitation = 0
        averageHumidity = 0
        averageClouds = 0

        temperature = data["daily"][0]["temp"]["day"]
        precipitations = data["daily"][0]["pop"] * 100
        temperatureList.append(temperature)
        precipitationList.append(precipitations)

        averageTemp += data["daily"][0]["temp"]["day"]
        averagePrecipitation += precipitations
        averageHumidity += data["current"]["humidity"]
        averageClouds += data["current"]["clouds"]
        for x in range(1, 8):
            print("obj n: " + str(x))
            print(data["daily"][x])
            temperature = data["daily"][x]["temp"]["day"]
            precipitations = data["daily"][x]["pop"] * 100
            temperatureList.append(temperature)
            precipitationList.append(precipitations)
            averageTemp += temperature
            averagePrecipitation += precipitations
            averageHumidity += data["daily"][x]["humidity"]
            averageClouds +=  data["daily"][x]["clouds"]
        averageTemp = averageTemp/8
        averageHumidity = averageHumidity/8
        averagePrecipitation = averagePrecipitation/8
        averageClouds = averageClouds/8

        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"averageTemp" : (str(int(averageTemp)) + "C°")})
        #print(data)
        resp = es.index(index="averagetemp", document=data)
        print(resp['result'])
        es.indices.refresh(index="averagetemp")

        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"averageclouds" : (str(int(averageClouds)) + "%")})
        #print(data)
        resp = es.index(index="averageclouds", document=data)
        print(resp['result'])
        es.indices.refresh(index="averageclouds")

        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"averagehumidity" : (str(int(averageHumidity)) + "%")})
        #print(data)
        resp = es.index(index="averagehumidity", document=data)
        print(resp['result'])
        es.indices.refresh(index="averagehumidity")

        data = json.dumps({"timestamp" : datetime.datetime.now().isoformat(),"averageprecipitation" : (str(int(averagePrecipitation)) + "%")})
        #print(data)
        resp = es.index(index="averageprecipitation", document=data)
        print(resp['result'])
        es.indices.refresh(index="averageprecipitation")

        print("tryfunction")
        function()
            


           
               
            
    

#print("-------- Try connection -----------")

kafkaServer="broker:29092"
topic = "mytopic"

#print("-------- connection OK -----------")
#elasticsearch configuration 
#print("try connection to elasticsearch")
es = Elasticsearch(
    ADDRESS,
    verify_certs=False
)
#print(es)
es.indices.create(index=ELASTICINDEXBESTDAYS, ignore=400) 
es.indices.create(index=ELASTICINDEXGOODDAYS, ignore=400) 
es.indices.create(index=ELASTICINDEXWORSTDAYS, ignore=400)
es.indices.create(index="averagetemp", ignore=400) 
es.indices.create(index="averageprecipitation", ignore=400) 
es.indices.create(index="averagehumidity", ignore=400)
es.indices.create(index="averageclouds", ignore=400) 




#print("indice creato")

#spark context creation

#print("-------- SparkContextCreation -----------")

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
sc.setLogLevel("WARN") 

#print("-------- spark read -----------")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()
    
#print("-------- spark select -----------")


df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
    .writeStream \
    .foreachBatch(elaborate) \
    .start() \
    .awaitTermination()
    
    


