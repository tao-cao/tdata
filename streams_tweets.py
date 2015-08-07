############## Parse & save to MongoDB
# where the Model Class is used and how to use it?
# 
#
############## 
from collections import defaultdict
from pymongo import MongoClient

class Model(object):
    timeBucketMins = 30
    def __init__(self):
        self.hashtags = defaultdict(lambda : defaultdict(int))
    def __del__(self):
        collection = MongoClient().twitter_stats.hashtags
        for timeBucket, hashtags in self.hashtags.items():
            item = collection.find_one({'time_bucket': timeBucket})
            if not item:
                item = {'time_bucket': timeBucket, 'tag_counts': {}}
            counts = item['tag_counts'] # counts is a dictionary
            for tag, count in hashtags.items():
                counts[tag] = count + counts.get('tag', 0)# use the dict.get method to make sure to get the right value
            if '_id' in item:
                collection.update({'_id': item['_id']}, {'$set': item})
            else:
                collection.insert(item)
    def predict(self, tweet):
        timeBucket = int(tweet['timestamp_ms']) \
                     / 1000 / 60 / Model.timeBucketMins
        for h in tweet['entities']['hashtags']:
            self.hashtags[timeBucket][h['text']] += 1
        return True

######### Write Streaming RDD into MongoDB
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import os
import importlib
import threading

PREDICTOR_REFRESH_SECS = 20
BATCH_DURATION_SECS = 20

predictors = {}

def makePredictors():
    models = {}
    def callback(arg, dirname, names):
        if "model.py" in names:
            libName = dirname.replace('/', '.') + ".model"
            models[libName] = importlib.import_module(libName).Model()
    os.path.walk("models", callback, None)
    global predictors
    predictors = models
    threading.Timer(PREDICTOR_REFRESH_SECS, makePredictors).start()

def makePredictions(RDD):
    def predictOnTweet(tweet):
        jsonTweet = json.loads(tweet[1])
        prediction = {}
        for n, m in predictors.items():
            try:
                prediction[n] = m.predict(jsonTweet)
            except:
                # Gotta catch 'em all here to prevent failing models
                # taking down all the rest with them
                pass
        #print jsonTweet['user']['name'].encode('ascii', 'ignore'), prediction
    RDD.foreach(predictOnTweet)

if __name__ == '__main__':
    sc = SparkContext('local', 'TwitterStream')
    ssc = StreamingContext(sc, BATCH_DURATION_SECS)
    zkQuorum = ('ip-172-31-33-156.eu-west-1.compute.internal:9092,'
                'ip-172-31-33-155.eu-west-1.compute.internal:9092')
    topics = ['twitter_no']
    kafaConfig = {"metadata.broker.list": zkQuorum}
    kafkaStream = KafkaUtils.createDirectStream(ssc, topics,
                                                kafaConfig)
    makePredictors()
    kafkaStream.foreachRDD(makePredictions)
    ssc.start()
    ssc.awaitTermination()

######### Load from MongoDB & serve

from flask import Flask
from flask import jsonify
from pymongo import MongoClient
from collections import defaultdict
app = Flask(__name__)

@app.route('/popular/<int:hrs>')
def popularHT(hrs):
    tagCounts = defaultdict(int)
    popularHashTags = MongoClient()\
        .twitter_stats.hashtags\
                      .find()\
                      .sort('time_bucket', -1).limit(hrs*2)
    for h in popularHashTags:
        for tag, count in h['tag_counts'].items():
            tagCounts[tag] += count
    return jsonify(**{k: v for (k, v) in sorted(tagCounts.items(),
                                                key=lambda (t, c): -c)[:10]})

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
	
########## 