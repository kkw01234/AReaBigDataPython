from konlpy.tag import Kkma, Okt, Mecab
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from konlpy.utils import pprint
import Restaurant
import TopWordCloud
import re
import os
import threading
import json
import nltk
import traceback
import pymongo
import datetime
import sys
position = ["Noun", "ProperNoun", "Foreign", "Alpha"]
i = 0
def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)


def tokenize(doc):
    # norm은 정규화, stem은 근어로 표시하기를 나타냄
    return ['/'.join(t) for t in Okt().pos(doc, norm=True, stem=True)]


def load_spark(table_list, place_id):
    seq_list = []
    rest_list = []
    for table in table_list:
        spark_session = SparkSession.builder.appName(place_id).master("local[*]") \
            .config("spark.mongodb.output.uri", "mongodb://118.220.3.71/crawling")\
            .config("spark.mongodb.output.collection", table)\
            .config("spark.mongodb.input.uri", "mongodb://118.220.3.71/crawling")\
            .config("spark.mongodb.input.collection", table) \
            .config("spark.local.dir", "C:/tmp/hive/") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
            .getOrCreate()
        df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").load()
        df.printSchema()
        try:
            df.createOrReplaceTempView(table)
            result = spark_session.sql("SELECT * FROM "+table+" Where place_id = '" + place_id + "'").rdd
            for r in result.collect():
                rest = Restaurant.Restaurant(r['_id'], r['name'], r['date'], r['r_addr'], r['r_lat'], r['r_lng'], r['r_name'], strip_e(r['comment']))
                rest_list.append(rest)
                pos = Okt().pos(strip_e(r['comment']).replace("#", " ").replace("\n", " "),norm=True, stem=True)
                seq_list.append(pos)
        except:
            pass

        df.drop()
        spark_session.stop()

    return seq_list, rest_list


def load_mongo(table_list, place_id):
    #global okt
    seq_list = []
    rest_list = []
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['crawling']
    for table in table_list:
        print(table)
        reviews = db[table].find({'place_id': place_id})
        try:
            for r in reviews:
                rest = Restaurant.Restaurant(r['_id'], r['name'], r['date'], r['r_addr'], r['r_lat'], r['r_lng'],
                                         r['r_name'], strip_e(r['comment']))
                rest_list.append(rest)
                pos = Okt().pos(strip_e(r['comment']).replace("#", " ").replace("\n", " "), norm=True, stem=True)
                seq_list.append(pos)
        except:
            continue

    return seq_list, rest_list


def lot_of_keywords(seq_list):
    spark_session = SparkSession.builder.appName("pos").master("local[*]").config('spark.local.dir', 'D:/spark-temp')\
        .getOrCreate()
    count_list = []
    for seq in seq_list:
       for p in seq:
            for pos in position:
                if p[1] == pos:
                    count_list.append({"word": p[0], "pos": p[1]})
    print(count_list)
    df = spark_session.createDataFrame(count_list)

    df.printSchema()
    table_name = "position"
    df.createOrReplaceTempView(table_name)
    result = df.groupBy('word').count().orderBy('count', ascending=False).limit(30)
    print(result)
    result_list = [(row['word'], row['count']) for row in result.collect()]
    print(result_list)
    df.drop()
    spark_session.sparkContext.stop()
    spark_session.stop()
    return result_list


train_data = []
load_doc = False
selected_words = []


def what(doc):
    global load_doc
    global selected_words
    try:
        if not load_doc:
            if os.path.isfile('train_docs.json'):
                print("존재합니다")
                with open('train_docs.json', encoding="utf-8") as f:
                    train_docs = json.load(f)
            tokens = [t for d in train_docs for t in d[0]]
            print('토큰 수 :', len(tokens))
            text = nltk.Text(tokens, name='NMSC')
            selected_words = [f[0] for f in text.vocab().most_common(10000)]
            load_doc = True
    except FileNotFoundError:
        traceback.print_exc()
    return [doc.count(word) for word in selected_words]


model = None


def sentiment_analysis(rest_list):
    global model
    from keras.models import load_model
    import numpy as np
    if model is None:
        model = load_model('./saved_model.h5')
    else:
        pass
    all_score = 0
    positive = 0
    negative = 0
    for rest in rest_list:
        token = tokenize(strip_e(rest.get_comment()).replace("#", " "))
        tf = what(token)
        data = np.expand_dims(np.asarray(tf).astype('float32'), axis=0)
        score = float(model.predict(data))
        all_score += score
        print(rest.get_comment(), ':', score * 100, '점')
    return all_score / len(rest_list)


def load_bigdata(place_id):
    table_list = ['rest_mango_plate', 'rest_instagram', 'rest_dining_code', 'rest_trip_advisor',
                  'dining_code', 'mango_plate', 'instagram', 'trips']
    seq_list, rest_list = load_mongo(table_list, place_id)
    if not rest_list or len(rest_list) < 10:
        return 0 # 너무 적은 데이터이거나 rest_list 가 없을 때
    result = lot_of_keywords(seq_list)
    # result = test(seq_list)
    print(result)
    insert_top30(place_id, result)
    TopWordCloud.make_word_cloud(result, place_id)
    score = sentiment_analysis(rest_list)
    return score


def insert_top30(place_id, result):
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn.crawling
    for i, r in zip(range(0, 30), result):
        db.most_common_place.update({'place_id': place_id, 'top': i+1},
                                    {'$set': {'token': r[0], "count": r[1],
                                              'today': datetime.datetime.now()}}, upsert=True)


# -------------------몽고 디비 관련 함수 ---------------------
def data_format(place_id,token, top, count):
    return {
        'place_id': place_id,
        'token': token,
        'top': int(top),
        'count': int(count),
        'today': datetime.datetime.now()
    }