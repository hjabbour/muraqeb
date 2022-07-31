from pymongo import MongoClient
import pandas as pd
import json


client = MongoClient('mongodb://root:rootpasswordhj123@199.241.137.238:27017')
db = client['raqebloc']
collection = db['raqebdata']
all_reports = collection.count_documents({})
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
