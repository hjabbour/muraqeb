from pymongo import MongoClient
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import json


client = MongoClient('mongodb://root:rootpasswordhj123@199.241.137.238:27017')
db = client['raqebloc']
collection = db['raqebdata']

#convert entire collection to Pandas dataframe
#test = pd.DataFrame(list(collection.find({})))
#print(test)


def coltopd():
    agg_result= collection.aggregate( 
        [{ 
        "$group" :  
            {"_id" : "$product",  
             "Total" : {"$sum" : 1},
             "Average" : {"$avg" :"$price"} 
             }} 
        ]) 

    array = []
    for x in agg_result:
        #print(x)
        array.append(x)
    aggdf = pd.DataFrame(array)
    aggdf.rename(columns={'_id':'Product'},inplace=True)
    json_records=aggdf.reset_index().to_json(orient='records')
    arr = json.loads(json_records)
    #return aggdf.to_json()
    #client.close()
    return arr

def colreports():
    all_reports = {}
    pipeline =     [
    {"$group" :  
        {"_id" : "$loc",  
         "Total" : {"$sum" : 1}, 
         "Average" : {"$avg" :"$price"} 
         }} 
    ] 
    todayd = datetime.datetime.today()
    one_day = todayd - relativedelta(days=1)
    past_week = todayd - relativedelta(weeks=1)
    past_month = todayd - relativedelta(weeks=4)
    all_reports['all'] = collection.count_documents({})
    all_reports['day'] = collection.count_documents( {"time": {"$gt":one_day}})
    all_reports['week'] = collection.count_documents( {"time": {"$gt":past_week}})
    all_reports['month'] = collection.count_documents( {"time": {"$gt":past_month}})
    all_reports['locations'] = nmbrofloc()
    return all_reports 

def minprice():
    
    agg_result= collection.aggregate( 
        [{ "$sort": { "price": 1 } },
            { 
        "$group" :  
            {"_id" : {"Product":"$product"},
             "Price" : {"$min" : "$price"},
             "Location" : {"$first":"$rawloc.display_name"}
             
             }} 
        ]) 

    array = []
    for x in agg_result:
        #print(x)
        array.append(x)
    aggdf = pd.DataFrame(array)
    #print(aggdf)
    aggdf.rename(columns={'_id':'Product'},inplace=True)
    json_records=aggdf.reset_index().to_json(orient='records')
    arr = json.loads(json_records)
    #return aggdf.to_json()
    return arr

def maxprice():
    
    agg_result= collection.aggregate( 
        [{ "$sort": { "price": -1 } },
            { 
        "$group" :  
            {"_id" : {"Product":"$product"},
             "Price" : {"$max" : "$price"},
             "Location" : {"$first":"$rawloc.display_name"}
             
             }} 
        ]) 

    array = []
    for x in agg_result:
        #print(x)
        array.append(x)
    aggdf = pd.DataFrame(array)
    #print(aggdf)
    aggdf.rename(columns={'_id':'Product'},inplace=True)
    json_records=aggdf.reset_index().to_json(orient='records')
    arr = json.loads(json_records)
    #return aggdf.to_json()
    return arr

def nmbrofloc():
    
    agg_result= collection.aggregate( 
        [
            { 
        "$group" :  
            {"_id" : {"location":"$rawloc"}
             }} 
        ]) 

    array = []
    for x in agg_result:
        #print(x)
        array.append(x)
    return(len(array))