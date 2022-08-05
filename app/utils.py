from pymongo import MongoClient
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import json
import folium
from folium import plugins
import os
clientstring = os.environ.get('clientstring')
client = MongoClient(clientstring)
print("clientstring=",clientstring)
print("secretkey=",os.environ.get('SECRET_KEY'))
## only in dev environnment in production use the os.environ.get
client = MongoClient('mongodb://root:rootpasswordhj123@199.241.137.238:27017')
db = client['raqebloc']
collection = db['raqebdata']
todayd = datetime.datetime.today()
past_day = todayd - relativedelta(days=1)
past_week = todayd - relativedelta(weeks=1)
past_month = todayd - relativedelta(weeks=4)

#convert entire collection to Pandas dataframe
#test = pd.DataFrame(list(collection.find({})))
#print(test)


def coltopd():
    ## averge prices
    agg_result= collection.aggregate( 
        [{"$match":{"time": {"$gt":past_month}}},
            { 
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
    ## calculated the number of reports day, week , month
    all_reports = {}
    pipeline =     [
    {"$group" :  
        {"_id" : "$loc",  
         "Total" : {"$sum" : 1}, 
         "Average" : {"$avg" :"$price"} 
         }} 
    ] 

    all_reports['all'] = collection.count_documents({})
    all_reports['day'] = collection.count_documents( {"time": {"$gt":past_day}})
    all_reports['week'] = collection.count_documents( {"time": {"$gt":past_week}})
    all_reports['month'] = collection.count_documents( {"time": {"$gt":past_month}})
    all_reports['locations'] = nmbrofloc()
    return all_reports 

def minprice():
    # calculates the min price past month
    agg_result= collection.aggregate( 
        [{"$match":{"time": {"$gt":past_month}}},
            { "$sort": { "price": 1 } },
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
    # calculates max price past month
    
    agg_result= collection.aggregate( 
        [{"$match":{"time": {"$gt":past_month}}},
            { "$sort": { "price": -1 } },
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
    # returns the number of locations 
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

def recentrep():
    ## get daily , monthly weekly reports last 10
    arr ={}
    limitrec = 20
    spandays=['day_rep','week_rep','month_rep']

    day_rep = collection.find( {"time": {"$gt":past_day}},{"time":1,"rawloc.display_name":1, "product":1, "price":1,"_id":0}).limit(limitrec)
    week_rep = collection.find( {"time": {"$gt":past_week}},{"time":1,"rawloc.display_name":1, "product":1, "price":1,"_id":0}).limit(limitrec)
    month_rep = collection.find( {"time": {"$gt":past_month}},{"time":1,"rawloc.display_name":1, "product":1, "price":1,"_id":0}).limit(limitrec)
    
    reports=[day_rep,week_rep,month_rep]
    for (rep,i) in zip(reports, spandays):
        df = pd.DataFrame(rep)
        json_records=df.reset_index().to_json(orient='records')
        arr[i] = json.loads(json_records)
        #print(arr)
    return  arr

def weekmap():
    m=folium.Map(location=[33.87363,35.67592], zoom_start=9)
    arr ={}
    limitrec = 100
    spandays=['day_map','week_map','month_map']
    
    
    week_rep = collection.find( {"time": {"$gt":past_week}},{"lng":1,"lat":1, "product":1, "price":1,"_id":0}).limit(limitrec)
    df = pd.DataFrame(week_rep)
    for i in range(0,len(df)-1):
        folium.Marker(location=[df.iloc[i]['lat'], df.iloc[i]['lng']],popup=df.iloc[i]['product']+'='+ str(df.iloc[i]['price']),icon=folium.Icon(color='green',icon='glyphicon glyphicon-plus-sign')).add_to(m)
        plugins.HeatMap(df[['lat','lng','price']]).add_to(m)
    return  m._repr_html_()


