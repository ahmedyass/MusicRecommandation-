
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pymongo import MongoClient
import json
import pandas as pd

cluster = MongoClient("mongodb+srv://ahmedyass:webmining@cluster0-elggs.mongodb.net/test?retryWrites=true&w=majority")
database = cluster["music"]

ratings = database["ratings"]
recommendation = database["recommandations"]

m = ratings.find({}, {"_id": 0})

jsonInput = json.dumps([row for row in m])
with open("C:/Users/ahfai/Recm/app/rating.json", "w") as outfile: 
    outfile.write(jsonInput)

def rate():
    # Initialize spark session
    spark = SparkSession.builder.appName('Recommendation_system').getOrCreate()
    df = spark.read.json('C:/Users/ahfai/Recm/app/rating.json')

    # Load Dataset in Apache Spark
    df.show(100,truncate=True)

    # Select appropriate columns 
    nd=df.select(df['id_song'],df['rating'],df['id_user'])
    nd.show()
    
    # Converting String to index
    indexer = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in list(set(nd.columns)-set(['rating'])) ]
    pipeline = Pipeline(stages=indexer)
    transformed = pipeline.fit(nd).transform(nd)
    transformed.show()

    # Creating training and test data 
    (training,test)=transformed.randomSplit([0.8, 0.2])

    # Creating ALS model and fitting data 
    als=ALS(maxIter=5,regParam=0.09,rank=25,userCol="id_user_index",itemCol="id_song_index",ratingCol="rating",coldStartStrategy="drop",nonnegative=True)
    model=als.fit(training)

    # Generate predictions and evaluate rmse 
    evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
    predictions=model.transform(test)
    rmse=evaluator.evaluate(predictions)
    print("RMSE="+str(rmse))
    predictions.show()

    # Providing Recommendations
    user_recs=model.recommendForAllUsers(20).show(10)
    
    # Converting back to string form 
    recs=model.recommendForAllUsers(10).toPandas()
    nrecs=recs.recommendations.apply(pd.Series) \
                .merge(recs, right_index = True, left_index = True) \
                .drop(["recommendations"], axis = 1) \
                .melt(id_vars = ['id_user_index'], value_name = "recommendation") \
                .drop("variable", axis = 1) \
                .dropna() 
    nrecs=nrecs.sort_values('id_user_index')
    nrecs=pd.concat([nrecs['recommendation'].apply(pd.Series), nrecs['id_user_index']], axis = 1)
    nrecs.columns = [
            'ProductID_index',
            'Rating',
            'UserID_index'
        ]
    md=transformed.select(transformed['id_user'],transformed['id_user_index'],transformed['id_song'],transformed['id_song_index'])
    md=md.toPandas()
    dict1 =dict(zip(md['id_user_index'],md['id_user']))
    dict2=dict(zip(md['id_song_index'],md['id_song']))
    nrecs['id_user']=nrecs['UserID_index'].map(dict1)
    nrecs['id_song']=nrecs['ProductID_index'].map(dict2)
    nrecs=nrecs.sort_values('id_user')
    nrecs.reset_index(drop=True, inplace=True)
    new=nrecs[['id_user','id_song','Rating']]
    new['recommendations'] = list(zip(new.id_song, new.Rating))
    res=new[['id_user','recommendations']]  
    res_new=res['recommendations'].groupby([res.id_user]).apply(list).reset_index()
    print(res_new)
    jsonOutput = json.dumps({"recommendation":\
                            [{"id_user":res_new['id_user'][i],\
                            "recommendations":[{"id_song":res_new['recommendations'][i][j][0],\
                            "rating":res_new['recommendations'][i][j][1]} for j in range(len(res_new['recommendations'][i]))]}\
                             for i in range(len(res_new['id_user'])) ]})
    with open("C:/Users/ahfai/Recm/app/rec.json", "w") as outfile: 
        outfile.write(jsonOutput)
    with open('C:/Users/ahfai/Recm/app/rec.json') as json_file:
        data = json.load(json_file)
        recommendation.delete_many({})
        recommendation.insert_many(data['recommendation'])